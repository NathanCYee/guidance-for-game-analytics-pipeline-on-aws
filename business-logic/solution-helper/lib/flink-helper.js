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

"use strict";

let AWS = require("aws-sdk");

/**
 * Helper function to interact with Kinesis Analytics SDK (Managed Flink SDK) for cfn custom resource.
 *
 * @class flinkHelper
 */
class flinkHelper {
    /**
     * @class flinkHelper
     * @constructor
     */
    constructor() {
        this.creds = new AWS.EnvironmentCredentials("AWS"); // Lambda provided credentials
        this.config = {
            credentials: this.creds,
            region: process.env.AWS_REGION,
        };
    }

    startKinesisAnalyticsApp(applicationName) {
        return new Promise((resolve, reject) => {
            let params = {
                ApplicationName: applicationName,
            };

            console.log(`Attempting to start Managed Flink App: ${JSON.stringify(params)}`);
            let kda = new AWS.KinesisAnalyticsV2(this.config);

            kda.describeApplication(params, function (err, response) {
                if (err) {
                    console.log(JSON.stringify(err));
                    reject(err);
                } else {
                    if (response == null) {
                        console.log("The Managed Flink application could not be found");
                        reject(err);
                    }
                    if (response.ApplicationDetail.ApplicationStatus === "READY") {
                        console.log("Starting Managed Flink Application");
                        kda.startApplication(
                            {
                                ApplicationName: applicationName,
                                RunConfiguration: {
                                    FlinkRunConfiguration: {
                                        // https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/state/savepoints/#allowing-non-restored-state
                                        AllowNonRestoredState: true,
                                    },
                                },
                            },
                            function (err, response) {
                                if (err) {
                                    console.log(JSON.stringify(err));
                                    reject(err);
                                } else {
                                    console.log("Started Managed Flink Application");
                                    resolve(response);
                                }
                            }
                        );
                    }
                }
            });
        });
    }
}

module.exports = flinkHelper;
