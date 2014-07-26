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
package com.ning.metrics.collector.processing.db;

import com.google.common.collect.Multimap;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.Mockito;
import org.skife.config.TimeSpan;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TestMockInMemoryCounterCacheProcessor
{
    private CollectorConfig config;
    private CounterStorage counterStorage;
    private InMemoryCounterCacheProcessor counterCacheProcessor;

    public void setup(TimeUnit timeUnit) throws Exception
    {
        config = Mockito.mock(CollectorConfig.class);
        counterStorage = Mockito.mock(CounterStorage.class);
        Mockito.when(config.getSpoolWriterExecutorShutdownTime()).thenReturn(new TimeSpan(1, TimeUnit.SECONDS));
        Mockito.when(config.getCounterEventMemoryFlushTime()).thenReturn(new TimeSpan(1, timeUnit));
        Mockito.when(config.getMaxCounterEventFlushCacheCount()).thenReturn(2L);

        counterCacheProcessor = new InMemoryCounterCacheProcessor(config, counterStorage);

    }

    @AfterMethod
    public void cleanUp() throws Exception{
        counterCacheProcessor.cleanUp();
    }

    private static CounterEventData prepareCounterEventData(String id,
            List<String> counters, DateTime createdDateTime){
        Map<String,Integer> counterMap = new HashMap<String, Integer>();
        for(String s : counters)
        {
            counterMap.put(s, 1);
        }

        return new CounterEventData(id, createdDateTime, counterMap);
    }

    @Test(groups = "slow")
    public void testAddCounterEventDataWithExpire() throws Exception{
        setup(TimeUnit.MICROSECONDS);

        counterCacheProcessor.addCounterEventData("1", prepareCounterEventData(
                "member123",
                Arrays.asList("pageView","trafficTablet","contribution"),
                new DateTime(DateTimeZone.UTC)));

        counterCacheProcessor.addCounterEventData("1", prepareCounterEventData(
                "member123",
                Arrays.asList("pageView","trafficTablet","contribution"),
                new DateTime(DateTimeZone.UTC)));

        Mockito.verify(counterStorage,
                Mockito.timeout(1000).times(1)).bufferMetrics(
                        Mockito.<Multimap<String, CounterEventData>> any());

        Mockito.verify(counterStorage, Mockito.times(1)).bufferMetrics(
                Mockito.<Multimap<String, CounterEventData>> any());

        Mockito.verify(counterStorage,
                Mockito.timeout(1000).times(1)).bufferMetrics(
                        Mockito.<Multimap<String, CounterEventData>>any());
    }

    @Test(groups = "slow")
    public void testAddCounterEventDataProcessRemaining() throws Exception{
        setup(TimeUnit.MILLISECONDS);

        counterCacheProcessor.addCounterEventData("1", prepareCounterEventData(
                "member123",
                Arrays.asList("pageView","trafficTablet","contribution"),
                new DateTime(DateTimeZone.UTC)));

        counterCacheProcessor.addCounterEventData("2", prepareCounterEventData(
                "member123",
                Arrays.asList("pageView","trafficTablet","contribution"),
                new DateTime(DateTimeZone.UTC)));

        counterCacheProcessor.processRemainingCounters();
    }

    @Test(groups = "slow")
    public void testAddCounterEventDataWithCapacity() throws Exception{
        setup(TimeUnit.SECONDS);
        counterCacheProcessor.addCounterEventData("1", prepareCounterEventData(
                "member123",
                Arrays.asList("pageView","trafficTablet","contribution"),
                new DateTime(DateTimeZone.UTC)));

        counterCacheProcessor.addCounterEventData("1", prepareCounterEventData(
                "member123",
                Arrays.asList("pageView","trafficTablet","contribution"),
                new DateTime(DateTimeZone.UTC)));

        counterCacheProcessor.addCounterEventData("1", prepareCounterEventData(
                "member123",
                Arrays.asList("pageView","trafficTablet","contribution"),
                new DateTime(DateTimeZone.UTC)));

        Mockito.verify(counterStorage,
                Mockito.timeout(2000).atLeastOnce()).bufferMetrics(
                        Mockito.<Multimap<String, CounterEventData>>any());

    }

}
