/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
 
'use strict';

import { KinesisClient, PutRecordsCommand } from "@aws-sdk/client-kinesis";
import { FirehoseClient, PutRecordBatchCommand as FirehosePutRecordBatchCommand } from "@aws-sdk/client-firehose";
const kinesisClient = new KinesisClient();
const firehoseClient = new FirehoseClient();

let destination = process.env.DESTINATION;
let streamName = process.env.STREAM_NAME;

export const handler = async (event) => {
    console.log('Event headers: ', event.headers)
    console.log('Event body: ', event.body)
    console.log('Received event:', event);
    try {
        // Check if it's a game backend framework request
        if (event.headers['Authorization']) {
            // Validate user token (implement your own validation logic)
            if (!isValidUserToken(event.headers['Authorization'])) {
                return {
                    statusCode: 401,
                    body: JSON.stringify({ message: 'Unauthorized' }),
                };
            }
        }
        // Parse the body
        const body = event.body;
        // Prepare the record
        const records = []

        for (let i = 0; i < body.events.length; i++) {
            const record = {
                Data: Buffer.from(JSON.stringify(body.events[i])),
                PartitionKey: body.events[i].application_id,
            };
            records.push(record)
        }

        let result;
        if (destination === 'FIREHOSE') {
            // Send to Firehose
            const command = new FirehosePutRecordBatchCommand({
                DeliveryStreamName: streamName,
                Records: records,
            });
            result = await firehoseClient.send(command);
        } else {
            // Send to Kinesis Data Stream (default)
            const command = new PutRecordsCommand({
                StreamName: streamName,
                Records: records,
            });
            result = await kinesisClient.send(command);
        }
        return {
            statusCode: 200,
            body: JSON.stringify({ message: 'Event sent successfully', result }),
        };
    } catch (error) {
        console.error('Error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Internal server error', error: error.message }),
        };
    }
};
function isValidUserToken(token) {
    // Implement your token validation logic here
    // This is a placeholder and should be replaced with actual validation
    return token.length > 0;
}