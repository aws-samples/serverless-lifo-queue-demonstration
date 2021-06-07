// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

'use strict';

const {serializeError} = require('serialize-error');
const {callFunction} = require('./call_function');

const PROCESS_TASKS_TOPIC_ARN = process.env.PROCESS_TASKS_TOPIC_ARN;

// Call the process tasks function if:
// - invoked via DynamoDB Streams and INSERT events are present
// - invoked via SNS by the process tasks function (tail call)
exports.handler = async (event) => {
    try {
        const hasRecords = event && Array.isArray(event.Records);
        const hasSnsEvent = hasRecords && event.Records.map(record => record.EventSource).includes('aws:sns');
        const hasInsertEvents = hasRecords && event.Records.map(record => record.eventName).includes('INSERT');
        if (hasSnsEvent || hasInsertEvents) {
            await callFunction({topicArn: PROCESS_TASKS_TOPIC_ARN});
            console.log({event: 'TRIGGER_CALL_PROCESS_TASKS', hasRecords, hasSnsEvent, hasInsertEvents});
        } else {
            console.log({event: 'TRIGGER_SKIP_PROCESS_TASKS', hasRecords, hasSnsEvent, hasInsertEvents});
        }
    } catch (err) {
        console.error({event: 'TRIGGER_ERROR', error: serializeError(err)});
    }
};