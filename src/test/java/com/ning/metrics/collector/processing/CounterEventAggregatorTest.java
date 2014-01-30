/*
 * Copyright 2010-2014 Ning, Inc.
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
package com.ning.metrics.collector.processing;

import com.google.common.collect.Lists;
import com.ning.metrics.collector.processing.db.model.CounterEvent;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author kguthrie
 */
public class CounterEventAggregatorTest {

    CounterEventAggregator aggregator;

    String counterGroup1 = "group";
    String uniqueId1 = "uniqueId";
    int identifierCategory1 = 0;
    DateTime countDate1 = new DateTime(2014, 1, 24, 0, 0);
    String counter11 = "pageView";
    int count11 = 1;
    String counter12 = "tabletPageView";
    int count12 = 1;
    CounterEvent event1;

    public CounterEventAggregatorTest() {

        aggregator = new CounterEventAggregator();
        event1 = new CounterEvent(counterGroup1,
                Lists.asList(new CounterEventData(
                                uniqueId1, identifierCategory1, countDate1,
                                new HashMap<String, Integer>() {
                                    {
                                        put(counter11, count11);
                                        put(counter12, count12);
                                    }
                                }), new CounterEventData[0]));
    }

    @Test
    public void testSingleNonAggregation() {
        aggregator.addEvent(event1);
        Iterable<CounterEvent> aggregatedEvents = aggregator.flush();

        int i = 0;
        for (CounterEvent event : aggregatedEvents) {
            Assert.assertEquals(1, ++i);
            Assert.assertNotSame(event, event1);
            Assert.assertEquals(counterGroup1, event.getAppId());

            int j = 0;
            for (CounterEventData data : event.getCounterEvents()) {
                Assert.assertEquals(1, ++j);
                Assert.assertEquals(uniqueId1, data.getUniqueIdentifier());
                Assert.assertEquals(countDate1.getMillis(),
                        data.getCreatedDate().getMillis());
                Assert.assertEquals(2, data.getCounters().size());
                Assert.assertEquals(count11,
                        (int) data.getCounters().get(counter11));
                Assert.assertEquals(count12,
                        (int) data.getCounters().get(counter12));
            }

        }
    }

    @Test
    public void testSingleAggregation() {
        aggregator.addEvent(event1);
        aggregator.addEvent(event1);
        Iterable<CounterEvent> aggregatedEvents = aggregator.flush();

        int i = 0;
        for (CounterEvent event : aggregatedEvents) {
            Assert.assertEquals(1, ++i);
            Assert.assertNotSame(event, event1);
            Assert.assertEquals(counterGroup1, event.getAppId());

            int j = 0;
            for (CounterEventData data : event.getCounterEvents()) {
                Assert.assertEquals(1, ++j);
                Assert.assertEquals(uniqueId1, data.getUniqueIdentifier());
                Assert.assertEquals(countDate1.getMillis(),
                        data.getCreatedDate().getMillis());
                Assert.assertEquals(2, data.getCounters().size());
                Assert.assertEquals(count11 * 2,
                        (int) data.getCounters().get(counter11));
                Assert.assertEquals(count12 * 2,
                        (int) data.getCounters().get(counter12));
            }

        }
    }

    @Test
    public void testMultithreadOnSingleEvent() throws Exception {

        final int incrementsPerThreads = 1024 * 8;
        final int numberOfThreads = 32;
        Thread[] workers = new Thread[numberOfThreads];

        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Thread(new Runnable() {

                @Override
                public void run() {
                    for (int i = 0; i < incrementsPerThreads; i++) {
                        aggregator.addEvent(event1);
                    }
                }
            });
        }

        long start = System.currentTimeMillis();

        for (Thread t : workers) {
            t.start();
        }

        for (Thread t : workers) {
            t.join();
        }

        long stop = System.currentTimeMillis();

        long ratePerSecond = (1000L * (long) (incrementsPerThreads)
                * ((long) numberOfThreads)) / (stop - start + 1);

        System.out.println(ratePerSecond + " incr/s on the same counter");

        Iterable<CounterEvent> aggregatedEvents = aggregator.flush();

        int i = 0;
        for (CounterEvent event : aggregatedEvents) {
            Assert.assertEquals(1, ++i);
            Assert.assertNotSame(event, event1);
            Assert.assertEquals(counterGroup1, event.getAppId());

            int j = 0;
            for (CounterEventData data : event.getCounterEvents()) {
                Assert.assertEquals(1, ++j);
                Assert.assertEquals(uniqueId1, data.getUniqueIdentifier());
                Assert.assertEquals(countDate1.getMillis(),
                        data.getCreatedDate().getMillis());
                Assert.assertEquals(2, data.getCounters().size());
                Assert.assertEquals(
                        count11 * incrementsPerThreads * numberOfThreads,
                        (int) data.getCounters().get(counter11));
                Assert.assertEquals(
                        count12 * incrementsPerThreads * numberOfThreads,
                        (int) data.getCounters().get(counter12));
            }

        }

    }

    @Test
    public void testMultithreadingOnMultipleEvents() throws Exception {

        final int incrementsPerCounter = 1024 * 4;
        final int numThreads = 32;
        final int numberOfCounters = 1024 ;

        final CounterEvent[] samples
                = genenrateUniformRandomCounts(
                        numberOfCounters, incrementsPerCounter);

        Thread[] workers = new Thread[numThreads];

        for (int i = 0; i < workers.length; i++) {

            final int worker = i;

            workers[i] = new Thread(new Runnable() {

                @Override
                public void run() {
                    for (int i = worker; i < samples.length; i += numThreads) {
                        aggregator.addEvent(samples[i]);
                    }
                }
            });
        }

        long start = System.currentTimeMillis();

        for (Thread t : workers) {
            t.start();
        }

        for (Thread t : workers) {
            t.join();
        }

        long stop = System.currentTimeMillis();

        long ratePerSecond = (1000L * (long) (incrementsPerCounter)
                * ((long) numberOfCounters)) / (stop - start + 1);

        System.out.println(ratePerSecond + " incr/s on the multiple counter");

        start = System.currentTimeMillis();

        Iterable<CounterEvent> aggregatedEvents = aggregator.flush();

        stop = System.currentTimeMillis();

        long flushRatePerSecond = (1000L
                * ((long) numberOfCounters)) / (stop - start + 1);

        System.out.println(flushRatePerSecond
                + " aggregated events flushed per second");

        int i = 0;
        for (CounterEvent event : aggregatedEvents) {
            i++;
            Assert.assertNotSame(event, event1);
            Assert.assertEquals(counterGroup1, event.getAppId());

            int j = 0;
            for (CounterEventData data : event.getCounterEvents()) {
                Assert.assertEquals(1, ++j);
                Assert.assertEquals(countDate1.getMillis(),
                        data.getCreatedDate().getMillis());
                Assert.assertEquals(2, data.getCounters().size());
                Assert.assertEquals(
                        count11 * incrementsPerCounter,
                        (int) data.getCounters().get(counter11));
                Assert.assertEquals(
                        count12 * incrementsPerCounter,
                        (int) data.getCounters().get(counter12));
            }

        }

        Assert.assertEquals(numberOfCounters, i);

    }

    /**
     * Generate an integer array of randomized samples of the given number of
     * counts on the given number of counters
     *
     * @param numberOfCounters
     * @param numberOfIncrements
     * @return
     */
    private CounterEvent[] genenrateUniformRandomCounts(int numberOfCounters,
            int numberOfIncrements) {

        CounterEvent[] result
                = new CounterEvent[numberOfIncrements * numberOfCounters];
        CounterEvent[] eventsById = new CounterEvent[numberOfCounters];
        List<Integer> resultList = Lists.newArrayListWithExpectedSize(
                result.length);

        for (int i = 0; i < numberOfCounters; i++) {
            for (int j = 0; j < numberOfIncrements; j++) {
                resultList.add(i);
            }
        }

        Collections.shuffle(resultList);
        Iterator<Integer> it = resultList.iterator();

        for (int i = 0; i < eventsById.length; i++) {
            eventsById[i] = new CounterEvent(counterGroup1,
                    Lists.asList(new CounterEventData(
                                    uniqueId1 + i,
                                    identifierCategory1, countDate1,
                                    new HashMap<String, Integer>() {
                                        {
                                            put(counter11, count11);
                                            put(counter12, count12);
                                        }
                                    }), new CounterEventData[0]));
        }

        for (int i = 0; i < result.length; i++) {
            int counterId = it.next();
            result[i] = eventsById[counterId];
        }

        return result;
    }

    @Test
    public void testLiveFlush() throws Exception {
        final int incrementsPerCounter = 1024 * 4;
        final int numThreads = 32;
        final int numberOfCounters = 1024;
        final int flushCount = 128;

        final CounterEvent[] samples
                = genenrateUniformRandomCounts(
                        numberOfCounters, incrementsPerCounter);

        Thread[] workers = new Thread[numThreads];

        for (int i = 0; i < workers.length; i++) {

            final int worker = i;

            workers[i] = new Thread(new Runnable() {

                @Override
                public void run() {
                    for (int i = worker; i < samples.length; i += numThreads) {
                        aggregator.addEvent(samples[i]);
                    }
                }
            });
        }

        for (Thread t : workers) {
            t.start();
        }

        List<Iterable<CounterEvent>> flushes
                = Lists.newLinkedList();

        for (int i = 0; i < flushCount; i++) {
            flushes.add(aggregator.flush());
        }

        for (Thread t : workers) {
            t.join();
        }

        for (Iterable<CounterEvent> events : flushes) {
            for (CounterEvent event : events) {
                aggregator.addEvent(event);
            }
        }

        Iterable<CounterEvent> aggregatedEvents = aggregator.flush();

        int i = 0;
        for (CounterEvent event : aggregatedEvents) {
            i++;
            Assert.assertNotSame(event, event1);
            Assert.assertEquals(counterGroup1, event.getAppId());

            int j = 0;
            for (CounterEventData data : event.getCounterEvents()) {
                Assert.assertEquals(1, ++j);
                Assert.assertEquals(countDate1.getMillis(),
                        data.getCreatedDate().getMillis());
                Assert.assertEquals(2, data.getCounters().size());
                Assert.assertEquals(
                        count11 * incrementsPerCounter,
                        (int) data.getCounters().get(counter11));
                Assert.assertEquals(
                        count12 * incrementsPerCounter,
                        (int) data.getCounters().get(counter12));
            }

        }

        Assert.assertEquals(numberOfCounters, i);

    }
}
