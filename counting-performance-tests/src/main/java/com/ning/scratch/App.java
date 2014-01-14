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
package com.ning.scratch;

import com.ning.scratch.counter.CountEvent;
import com.ning.scratch.counter.CounterCallback;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class App
{
    private static final long[] twoToThe = new long[32];

    static {
        twoToThe[0] = 1;

        for (int i = 1; i < twoToThe.length; i++) {
            twoToThe[i] = twoToThe[i - 1] * 2L;
        }
    }

    private static final long countersPerSimulation = twoToThe[15];
    private static final long maxCounters = twoToThe[30];
    private static final long hitsPerCounter = 16;
    private static final long numReadQueries = twoToThe[10];

    private static final long numberOfIncrements = twoToThe[18];

    public static void main(String[] args) throws Exception {

        BlockingQueue<CountEvent> counterQueue
                = new LinkedBlockingDeque<>((int) twoToThe[16]);

        final AtomicLong countCompleted = new AtomicLong(0);

        CounterCallback returnQueueCallback = new CounterCallback() {

            @Override
            public void onCount(boolean wasCounterNew) {
                long currCompletedCount = countCompleted.incrementAndGet();

                if (currCompletedCount == numberOfIncrements) {
                    synchronized (countCompleted) {
                        countCompleted.notifyAll();
                    }
                }
            }
        };

        DataGenerator generator = new DataGenerator(numberOfIncrements,
                counterQueue, returnQueueCallback);

        Thread t = new Thread(generator);
        t.start();

        int i = 0;

        while (i++ < numberOfIncrements) {
            counterQueue.poll(10, TimeUnit.SECONDS);
        }

        t.join();

//
//        long generation = 0;
//
//        while (generation * countersPerSimulation < maxCounters) {
//
//            long runTimeMillis = runSimulation(0, countersPerSimulation);
//            long queryTimeMillis = runQueryTest();
//            long updateTimeMillis = runUpdateTest((int) generation);
//
//            long incThroughput = 1000 * countersPerSimulation * hitsPerCounter
//                    / runTimeMillis;
//
//            double msToIncrement = ((double) runTimeMillis)
//                    / ((double) (countersPerSimulation * hitsPerCounter));
//
//            double msToSelectSingleCounter = ((double) queryTimeMillis)
//                    / ((double) numReadQueries);
//
//            double msToUpdateSingleCounter = ((double) updateTimeMillis)
//                    / ((double) countersPerSimulation);
//
//            System.out.println(String.format("%d, %d, %d, %.4f, %.4f, %.4f",
//                    generation, generation * countersPerSimulation,
//                    incThroughput, msToIncrement, msToSelectSingleCounter,
//                    msToUpdateSingleCounter));
//
//            generation++;
//        }
//
//        System.out.println(runTimeMillis + "ms taken for "
//                + (countersPerSimulation * hitsPerCounter)
//                + " increments");
//
//        System.out.println((1000
//                * (countersPerSimulation * hitsPerCounter)
//                / runTimeMillis) + " updates per second");

    }

//    private static long runSimulation(long startingCounterId,
//            long numNewCounters) throws Exception {
//
//        DbHandler.insertCount.set(0);
//        DbHandler.updateCount.set(0);
//
//        CounterModule counterModule = CounterModule.get();
//        DbHandler db = counterModule.getDbHandler();
//
//        int[] data = new DataGenerator((int) (numNewCounters + startingCounterId),
//                (int) hitsPerCounter).generate();
//
//        final AtomicInteger completedCount = new AtomicInteger(0);
//        final int increments = data.length;
//
//        CounterCallback returnQueueCallback = new CounterCallback() {
//
//            @Override
//            public void onCount(boolean wasCounterNew) {
//                int currCompletedCount = completedCount.incrementAndGet();
//
//                if (currCompletedCount == increments) {
//                    synchronized (completedCount) {
//                        completedCount.notifyAll();
//                    }
//                }
//            }
//        };
//
//        long start = System.currentTimeMillis();
//
//        for (int i : data) {
//            counterModule.incrementAsync(i, 1, null, returnQueueCallback);
//        }
//
//        while (completedCount.get() != increments) {
//            synchronized (completedCount) {
//                completedCount.wait(1000);
//            }
//        }
//
//        long stop = System.currentTimeMillis();
//
//        if (DbHandler.insertCount.get() != numNewCounters) {
//            System.err.println("mismatch in insert count, "
//                    + DbHandler.insertCount.get() + " should have been "
//                    + numNewCounters);
//            System.exit(-1);
//        }
//
//        long expectedUpdateCount
//                = (numNewCounters + startingCounterId) * hitsPerCounter
//                - numNewCounters;
//
//        if (DbHandler.updateCount.get() != expectedUpdateCount) {
//            System.err.println("mismatch in update count, "
//                    + DbHandler.updateCount.get() + " should have been "
//                    + expectedUpdateCount);
//            System.exit(-1);
//        }
//
//        int numDbCounters = counterModule.getNumberOfCountersForDate(null);
//
//        if (numDbCounters != countersPerSimulation) {
//            System.err.println(numDbCounters + " counters were found when "
//                    + "should have been " + countersPerSimulation);
//            System.exit(-1);
//        }
//
//
//        int numWrong = counterModule.getNumberOfCountersForDateNotEqualTo(null,
//                (int) hitsPerCounter);
//
//        if (numWrong != 0) {
//            System.err.println(numWrong + " incorrect counters were found");
//            System.exit(-1);
//        }
//
//        return stop - start;
//
//    }
//
//    private static long runQueryTest() throws Exception {
//        int[] queriedIds = new int[(int) numReadQueries];
//        Random rand = new Random(System.currentTimeMillis());
//
//        CounterModule counterModule = CounterModule.get();
//
//        for (int i = 0; i < numReadQueries; i++) {
//                queriedIds[i] = rand.nextInt((int) countersPerSimulation);
//        }
//
//        long start = System.currentTimeMillis();
//
//        for (int id : queriedIds) {
//            counterModule.getCountForCounter(id, null);
//        }
//
//        long stop = System.currentTimeMillis();
//
//        return stop - start;
//    }
//
//    private static long runUpdateTest(int generation) throws Exception {
//
//        CounterModule counterModule = CounterModule.get();
//
//        long start = System.currentTimeMillis();
//        counterModule.incrementCounterDatesBy(generation + 1);
//        return System.currentTimeMillis() - start;
//    }
}
