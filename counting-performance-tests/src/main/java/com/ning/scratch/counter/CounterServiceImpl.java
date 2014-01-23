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

import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Singleton class for interacting with the stored counter system
 *
 * @author kguthrie
 */
public class CounterServiceImpl {

    public static final int DAYS_OF_HISTORY = 30;

    public static final String URL = "jdbc:mysql://localhost:3306/Tests";
    public static final String USERNAME = "";
    public static final String PASSWORD = "";
    public static final int DATABASE_WORKER_THREADS = 16;

    private static boolean initiailized = false;
    private static CounterServiceImpl instance;

    private final BlockingQueue<CountEvent> countQueue;
    private final CounterUpdater[] updaters;
    private final DbHandler[] dbHandlers;

    private final DbHandler db;

    public synchronized static CounterServiceImpl get() throws Exception {
        if (!initiailized) {
            instance = new CounterServiceImpl();
            initiailized = true;
        }
        return instance;
    }

    private CounterServiceImpl() throws Exception {
        countQueue = new LinkedBlockingQueue<>();
        updaters = new CounterUpdater[DATABASE_WORKER_THREADS];
        dbHandlers = new DbHandler[DATABASE_WORKER_THREADS];

        for (int i = 0; i < DATABASE_WORKER_THREADS; i++) {
            dbHandlers[i] = new DbHandler(URL);
            updaters[i] = new CounterUpdater(i + "",
                    countQueue, dbHandlers[i]);
            new Thread(updaters[i]).start();
        }

        db = new DbHandler(URL);
        db.dropAndRecreatePartitions();
    }

    /**
     * Get the connection to the database
     *
     * @return
     */
    public DbHandler getDbHandler() {
        return db;
    }

    /**
     * increment the given counter
     *
     * @param countEvent
     */
    public void count(CountEvent countEvent) {
        countQueue.add(countEvent);
    }

    /**
     * Get the total number of counters. This is equivalent to calling select
     * count(*) on the non-partitioned table.
     *
     * @param countRangeStart
     * @return
     * @throws java.lang.Exception
     */
    public int getNumberOfCountersForDate(Date countRangeStart)
            throws Exception {
        final int[] results = new int[DATABASE_WORKER_THREADS];
        Thread[] threads = new Thread[DATABASE_WORKER_THREADS];

        final java.sql.Date usedDate = countRangeStart == null
                ? db.getToday()
                : new java.sql.Date(countRangeStart.getTime());


        for (int i = 0; i < DATABASE_WORKER_THREADS; i++) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        results[index]
                                = dbHandlers[index].getNumberOfCountersForDate(
                                        usedDate, index,
                                        DATABASE_WORKER_THREADS);
                    }
                    catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });

            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        int result = 0;

        for (int i : results) {
            result += i;
        }

        return result;
    }

    /**
     * get the number of counters in the database with the given date whose
     * count is different from the given count
     *
     * @param countRangeStart
     * @param notEqualToCount
     * @return
     * @throws Exception
     */
    public int getNumberOfCountersForDateNotEqualTo(Date countRangeStart,
            final int notEqualToCount) throws Exception {

        final int[] results = new int[DATABASE_WORKER_THREADS];
        Thread[] threads = new Thread[DATABASE_WORKER_THREADS];

        final java.sql.Date usedDate = countRangeStart == null
                ? db.getToday()
                : new java.sql.Date(countRangeStart.getTime());

        for (int i = 0; i < DATABASE_WORKER_THREADS; i++) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        results[index]
                                = dbHandlers[index]
                                .getNumberOfCountersForDateNotEqualTo(
                                        usedDate, notEqualToCount, index,
                                        DATABASE_WORKER_THREADS);
                    }
                    catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });

            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        int result = 0;

        for (int i : results) {
            result += i;
        }

        return result;
    }

    /**
     * get the count for the counter with the given id and date
     *
     * @param counterId
     * @param date
     * @return
     * @throws java.lang.Exception
     */
    public int getCountForCounter(int counterId, Date date)
            throws Exception {
        return db.getCountForCounter(counterId, date == null
                ? db.getToday() : new java.sql.Date(date.getTime()));
    }

    /**
     * Increment all the counters for "today" by the given number of days
     *
     * @param days
     * @throws Exception
     */
    public void incrementCounterDatesBy(final int days)
            throws Exception {
        Thread[] threads = new Thread[DATABASE_WORKER_THREADS];

        for (int i = 0; i < DATABASE_WORKER_THREADS; i++) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {

                        dbHandlers[index].incrementCounterDatesBy(days, index,
                                DATABASE_WORKER_THREADS);
                    }
                    catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });

            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

    }

}
