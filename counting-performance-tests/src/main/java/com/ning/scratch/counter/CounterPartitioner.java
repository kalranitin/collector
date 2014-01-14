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

/**
 * singleton class that determines into which partition of the counters table a
 * given counter id should go
 *
 * @author kguthrie
 */
public class CounterPartitioner {

    public static final int PARTITION_COUNT = 16;

    private static CounterPartitioner instance;
    private static boolean initialized = false;

    public synchronized static CounterPartitioner get() {

        if (!initialized) {
            instance = new CounterPartitioner();
            initialized = true;
        }

        return instance;
    }

    private CounterPartitioner() {

    }

    /**
     * Gets the partition table for the given counterId.
     *
     * @param counterId
     * @return
     */
    public int getPartition(int counterId) {

        if (counterId < 0) {
            counterId = -counterId;
        }

        return counterId % PARTITION_COUNT;
    }
}
