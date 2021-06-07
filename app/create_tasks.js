// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

'use strict';

const AWSXRay = require('aws-xray-sdk');
const {DynamoDBClient, PutItemCommand} = require('@aws-sdk/client-dynamodb');
const {v4: uuidv4} = require('uuid');
const {DateTime} = require('luxon');
const {serializeError} = require('serialize-error');

const ddbClient = AWSXRay.captureAWSv3Client(new DynamoDBClient({}));

const QUEUE_TABLE = process.env.QUEUE_TABLE;

const CONFIG = {
    delayBetweenCreate: 1 * 1000,
    maxActiveTime: 15 * 1000,
    ttlAmount: 1,
    ttlUnit: 'hour'
};

// Create a single task in PENDING state, with a TTL value. Old tasks will be removed after the TTL value has
// been reached, this helps especially in the case of insurmountable load on the queue.
const createTask = async (taskId) => {
    const now = DateTime.now();
    const ttl = now.plus({[CONFIG.ttlUnit]: CONFIG.ttlAmount});
    const params = {
        TableName: QUEUE_TABLE,
        Item: {
            taskId: {
                S: taskId
            },
            taskStatus: {
                S: 'PENDING'
            },
            taskCreated: {
                N: `${now.toMillis()}`
            },
            taskUpdated: {
                N: `${now.toMillis()}`
            },
            ttl: {
                N: `${ttl.toMillis()}`
            }
        }
    };
    try {
        await ddbClient.send(new PutItemCommand(params));
        console.log({event: 'CREATE_TASK', taskId});
        return true;
    } catch (err) {
        console.error({event: 'CREATE_TASK_ERROR', error: serializeError(err), taskId});
        return false;
    }
};

// Create a task every delayBetweenCreate milliseconds, until maxActiveTime is exceeded. The Lambda function
// invocation remains active until this function exits.
const createTasks = async (start) => {
    return new Promise(async resolve => {
        const waitAndContinueOrResolve = async () => {
            const activeTime = Math.abs(start.diffNow().toMillis());
            if (activeTime < CONFIG.maxActiveTime) {
                setTimeout(async () => {
                    await createTasks(start);
                }, CONFIG.delayBetweenCreate);
            } else {
                resolve();
            }
        };
        await createTask(uuidv4());
        await waitAndContinueOrResolve();
    });
};

// The Lambda function handler method.
exports.handler = async () => {
    try {
        const start = DateTime.now();
        console.log({event: 'CREATE_TASKS_START'});
        await createTasks(start);
        console.log({event: 'CREATE_TASKS_END'});
    } catch (err) {
        console.error({event: 'CREATE_TASKS_ERROR', error: serializeError(err)});
    }
};