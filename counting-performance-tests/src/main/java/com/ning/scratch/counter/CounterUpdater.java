/*
 * Copyright 2010-2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ning.scratch.counter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Updates a given counter id in the test database a given number of times
 *
 * @author kguthrie
 */
public class CounterUpdater implements Runnable {

    private final DbHandler db;
    private final BlockingQueue<CountEvent> countQueue;
    private boolean stopRequested;

    public CounterUpdater(String updaterId, BlockingQueue<CountEvent> countQueue,
            DbHandler db) throws Exception {

        this.countQueue = countQueue;
        this.db = db;
        stopRequested = false;
    }

    /**
     * Thread root for the counter updater. This method runs a loop that
     * continuously polls the countQueue, and handles the result
     */
    @Override
    public void run() {

        CountEvent current;

        while (!stopRequested) {
            try {

                //Poll for 1 second max
                if ((current = countQueue.poll(1, TimeUnit.SECONDS)) == null) {
                    continue;
                }

                handleCountEvent(current);
            }
            catch (InterruptedException ie) {
                break;
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Handle the given count event
     *
     * @param count
     */
    private void handleCountEvent(CountEvent count) throws Exception {


        if (count.getCallback() != null) {
            count.getCallback().onCount(false);
        }

    }
}
