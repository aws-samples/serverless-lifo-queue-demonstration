// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

'use strict';

const AWSXRay = require('aws-xray-sdk');
const {SNSClient, PublishCommand} = require("@aws-sdk/client-sns");
const {serializeError} = require('serialize-error');

const snsClient = AWSXRay.captureAWSv3Client(new SNSClient({}));

// Calls a Lambda function via an AWS SNS topic.
const callFunction = async (config = {}) => {
    try {
        const params = {
            TopicArn: config.topicArn,
            Message: 'Start'
        };
        await snsClient.send(new PublishCommand(params));
        console.log({event: 'CALL_FUNCTION', params});
    } catch (err) {
        console.error({event: 'CALL_FUNCTION_ERROR', error: serializeError(err)});
    }
};

module.exports = {callFunction};