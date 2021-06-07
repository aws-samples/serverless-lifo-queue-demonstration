// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

'use strict';

const CONFIG = {
    fakeMaxActiveTasks: 3,
    fakeTaskDuration: 15000
};

// This method fakes task processing. Only fakeMaxActiveTasks number of tasks can be processed concurrently, and each task
// takes fakeTaskDuration milliseconds to complete. This is intended to simulate integration with a throughput
// constrained external system (e.g. a blocking HTTP GET request). If the active tasks limit is exceeded, then the
// task is rejected and the task is returned to the queue in PENDING state.
// The simulation is rudimentary, a correct implementation would use distributed synchronisation (e.g. rate
// limiting), to ensure the external system is not overloaded.
let activeTaskCount = 0;
const fakeTaskRunner = async (task) => {
    const maxActiveTasks = CONFIG.fakeMaxActiveTasks;
    if (activeTaskCount >= maxActiveTasks) {
        console.log({event: 'TASK_RUN_SKIP', taskId: task.taskId, task, activeTaskCount, maxActiveTasks});
        return 'PENDING';
    } else {
        console.log({event: 'TASK_RUN_START', taskId: task.taskId, task, activeTaskCount, maxActiveTasks});
    }
    activeTaskCount++;
    return new Promise(resolve => {
        setTimeout(async () => {
            console.log({event: 'TASK_RUN_COMPLETE', taskId: task.taskId, task, activeTaskCount, maxActiveTasks});
            activeTaskCount--;
            resolve('SUCCESS');
        }, CONFIG.fakeTaskDuration);
    });
};

module.exports = {taskRunner: fakeTaskRunner};