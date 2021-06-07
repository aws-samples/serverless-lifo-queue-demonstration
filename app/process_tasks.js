// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

'use strict';

const AWSXRay = require('aws-xray-sdk');
const {DynamoDBClient, QueryCommand, UpdateItemCommand} = require('@aws-sdk/client-dynamodb');
const {DateTime} = require('luxon');
const {callFunction} = require('./call_function');
const {taskRunner} = require('./task_runner');
const {serializeError} = require('serialize-error');

const ddbClient = AWSXRay.captureAWSv3Client(new DynamoDBClient({}));

const TRIGGER_TOPIC_ARN = process.env.TRIGGER_TOPIC_ARN;
const QUEUE_TABLE = process.env.QUEUE_TABLE;

const CONFIG = {
    delayBetweenBatch: 500,
    delayBetweenTask: 200,
    maxActiveTime: 59 * 1000,
    pageLimit: 10
};

// Transition a task from one state to another. Only transition if the origin state is as expected.
const transitionTask = async (task, fromTaskStatus, toTaskStatus) => {
    const taskId = task.taskId;
    const now = DateTime.now().toMillis();
    const taskAge = now - task.taskCreated;
    const params = {
        TableName: QUEUE_TABLE,
        Key: {
            taskId: {
                S: taskId
            }
        },
        UpdateExpression: 'set taskStatus = :toTaskStatus, taskUpdated = :taskUpdated',
        ConditionExpression: "taskStatus = :fromTaskStatus",
        ExpressionAttributeValues: {
            ':fromTaskStatus': {'S': fromTaskStatus},
            ':toTaskStatus': {'S': toTaskStatus},
            ':taskUpdated': {'N': `${now}`}
        }
    }
    try {
        await ddbClient.send(new UpdateItemCommand(params));
        console.log({event: 'TRANSITION_TASK', from: fromTaskStatus, to: toTaskStatus, taskId, task, taskAge});
        return true;
    } catch (err) {
        if (err.code !== 'ConditionalCheckFailedException') {
            console.error({event: 'TRANSITION_TASK_ERROR', error: serializeError(err), taskId});
        }
        console.log({event: 'TRANSITION_TASK_FAILED', from: fromTaskStatus, to: toTaskStatus, taskId, task, taskAge});
        return false;
    }
};

// Get a batch of PENDING state tasks. Returns a page (pageLimit) of task items
// in LIFO order, created timestamp descending.
const getPendingTaskBatch = async () => {
    try {
        const params = {
            TableName: QUEUE_TABLE,
            IndexName: 'task-status-created-index',
            ExpressionAttributeValues: {
                ':taskStatus': {S: 'PENDING'}
            },
            KeyConditionExpression: 'taskStatus = :taskStatus',
            Limit: CONFIG.pageLimit,
            ScanIndexForward: false // Descending order, by taskCreated range key
        };
        const results = await ddbClient.send(new QueryCommand(params));
        const tasks = ((results || {}).Items || {}) || [];
        console.log({event: 'GET_PENDING_TASK_BATCH', taskCount: tasks.length});
        return tasks.map(task => ({
            taskId: task.taskId.S,
            taskStatus: task.taskStatus.S,
            taskCreated: Number.parseInt(task.taskCreated.N),
            taskUpdated: Number.parseInt(task.taskUpdated.N)
        }));
    } catch (err) {
        console.error({event: 'GET_PENDING_TASK_BATCH_ERROR', error: serializeError(err)});
        return [];
    }
};

// Process a batch of tasks. Carefully ensure the task state transition is correct.
const processTaskBatch = (tasks) => {
    console.log({event: 'PROCESS_TASK_BATCH', taskCount: tasks.length});
    const processTask = async (task) => {
        const taskId = task.taskId;
        if (await transitionTask(task, 'PENDING', 'TAKEN')) {
            let toState = 'FAILURE';
            try {
                toState = await taskRunner(task);
            } catch (err) {
                console.error({event: 'PROCESS_TASK_ERROR', error: serializeError(err), taskId});
                toState = 'PENDING';
            } finally {
                await transitionTask(task, 'TAKEN', toState);
            }
        }
    };
    const wrapProcessTask = (task, delay) => {
        return new Promise(resolve => {
            setTimeout(async () => {
                resolve(await processTask(task));
            }, delay);
        });
    };
    let delay = 0;
    return tasks.map(task => {
        return wrapProcessTask(task, delay += CONFIG.delayBetweenTask);
    });
};

// Process batches of PENDING tasks until maxActiveTime is exceeded, or there are no tasks to
// process. If maxActiveTime is reached then tail call the trigger function, as there will be
// more PENDING tasks to process.
const processTasks = async (start) => {
    const activeTime = Math.abs(start.diffNow().toMillis());
    const reachedTimeLimit = (activeTime >= CONFIG.maxActiveTime);
    return new Promise(async resolve => {
        const waitAndContinue = async (delay) => {
            setTimeout(async () => {
                await processTasks(start);
            }, delay);
        };
        const getAndProcessTasksOrResolve = async () => {
            const tasks = await getPendingTaskBatch();
            const hasTasks = (tasks.length > 0);
            if (hasTasks && !reachedTimeLimit) {
                await Promise.allSettled(processTaskBatch(tasks));
                await waitAndContinue(CONFIG.delayBetweenBatch);
            } else {
                if (hasTasks) {
                    console.log({event: 'TAIL_CALL_TRIGGER', start, activeTime, hasTasks, reachedTimeLimit});
                    await callFunction({topicArn: TRIGGER_TOPIC_ARN});
                }
                resolve();
            }
        };
        await getAndProcessTasksOrResolve();
    });
};

// The Lambda function handler method.
exports.handler = async () => {
    try {
        const start = DateTime.now();
        console.log({event: 'PROCESS_TASKS_START', start});
        await processTasks(start);
        console.log({event: 'PROCESS_TASKS_END'});
    } catch (err) {
        console.error({event: 'PROCESS_TASKS_ERROR', error: serializeError(err)});
    }
};