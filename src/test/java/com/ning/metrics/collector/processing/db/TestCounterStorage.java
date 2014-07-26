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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.ning.metrics.collector.guice.module.CollectorObjectMapperModule;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {"slow", "database"})
public class TestCounterStorage
{
    private CollectorMysqlTestingHelper helper;

    @Inject
    ObjectMapper mapper;

    @Inject
    CounterStorage counterStorage;

    @BeforeClass(groups = {"slow", "database"})
    public void startDB() throws Exception{
        helper = new CollectorMysqlTestingHelper();
        helper.startMysql();
        helper.initDb();

        System.setProperty("collector.spoolWriter.jdbc.url", helper.getJdbcUrl());
        System.setProperty("collector.spoolWriter.jdbc.user", CollectorMysqlTestingHelper.USERNAME);
        System.setProperty("collector.spoolWriter.jdbc.password", CollectorMysqlTestingHelper.PASSWORD);

        Guice.createInjector(new CollectorObjectMapperModule(), new DBConfigModule()).injectMembers(this);

    }

    @BeforeMethod(alwaysRun = true, groups = {"slow", "database"})
    public void clearDB(){
        helper.clear();
    }

    @AfterClass(alwaysRun = true,groups = {"slow", "database"})
    public void stopDB() throws Exception{
        helper.stopMysql();
    }

    @Test(groups = {"slow", "database"})
    public void testInsertAndLoadDailyMetrics() throws Exception
    {

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();
        multimap.put("1", prepareCounterEventData("member123",
                Arrays.asList("pageView","trafficTablet","contribution"),
                new DateTime(DateTimeZone.UTC)));
        multimap.put("1", prepareCounterEventData("member321",
                Arrays.asList("pageView","trafficMobile"),
                new DateTime(DateTimeZone.UTC)));

        counterStorage.bufferMetrics(multimap);

        List<CounterEventData> dailyList =
                counterStorage.loadBufferedMetricsPaged("1", null, 1, 1);

        Assert.assertNotNull(dailyList);
        Assert.assertFalse(dailyList.isEmpty());
        Assert.assertEquals(1, dailyList.size());
        Assert.assertTrue(Objects.equal("member123", dailyList.get(0).getUniqueIdentifier()) || Objects.equal("member321", dailyList.get(0).getUniqueIdentifier()));
    }

    @Test(groups = {"slow", "database"})
    public void testInsertAndLoadGroupedDailyMetrics() throws Exception
    {
        Multimap<String, CounterEventData> multi = ArrayListMultimap.create();

        for(int j=0;j<100;j++)
        {
            multi.put("1", prepareCounterEventData("member123",
                    Arrays.asList("pageView","trafficTablet","contribution"),
                    new DateTime(DateTimeZone.UTC)));
            multi.put("1", prepareCounterEventData("member321",
                    Arrays.asList("pageView","trafficMobile"),
                    new DateTime(DateTimeZone.UTC)));
        }

        counterStorage.bufferMetrics(multi);

        List<CounterEventData> dailyList = counterStorage.loadBufferedMetrics(
                "1", new DateTime(DateTimeZone.UTC));

        Assert.assertNotNull(dailyList);
        Assert.assertFalse(dailyList.isEmpty());
        Assert.assertEquals(2, dailyList.size());
        Assert.assertEquals(new Integer("100"),
                dailyList.get(0).getCounters().get("pageView"));
    }

    /**
     * Convenience method for generating a counter event with the basic info
     * @param id unique identifier of the entity performing the counted action
     * @param counters map of counter name to counts for that counter
     * @param createdDateTime timestamp of the event being counted
     * @return
     */
    private static CounterEventData prepareCounterEventData(String id,
            List<String> counters, DateTime createdDateTime){
        Map<String,Integer> counterMap = new HashMap<String, Integer>();
        for(String s : counters)
        {
            counterMap.put(s, 1);
        }

        return new CounterEventData(id, createdDateTime, counterMap);
    }

    @Test(groups = {"slow", "database"})
    public void testDeleteDailyMetrics() throws Exception{
        Multimap<String, CounterEventData> multimap =
                ArrayListMultimap.create();

        DateTime dateTime = new DateTime(DateTimeZone.UTC);

        multimap.put("1", prepareCounterEventData("member123",
                Arrays.asList("pageView","trafficTablet","contribution"),
                dateTime));

        multimap.put("1", prepareCounterEventData("member321",
                Arrays.asList("pageView","trafficMobile"),
                dateTime));

        multimap.put("1", prepareCounterEventData("member123",
                Arrays.asList("pageView","trafficTablet"),
                dateTime.plusHours(1)));

        counterStorage.bufferMetrics(multimap);

        List<CounterEventData> dailyList =
                counterStorage.loadBufferedMetrics("1", dateTime);

        Assert.assertNotNull(dailyList);
        Assert.assertFalse(dailyList.isEmpty());

        boolean deleted = counterStorage.deleteBufferedMetrics("1", dateTime);

        Assert.assertTrue(deleted);

        dailyList = counterStorage.loadBufferedMetrics("1", dateTime);
        Assert.assertNotNull(dailyList);
        Assert.assertTrue(dailyList.isEmpty());

        dailyList = counterStorage.loadBufferedMetrics("1", dateTime.plusHours(1));
        Assert.assertNotNull(dailyList);
        Assert.assertFalse(dailyList.isEmpty());
        Assert.assertTrue(dailyList.size() == 1);

    }

    @Test(groups = {"slow", "database"})
    public void testGetSubscritionIdsFromDailyMetrics()
    {
        Multimap<String, CounterEventData> events = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(DateTimeZone.UTC);

        events.put("1", prepareCounterEventData("member123",
                Arrays.asList("pageView","trafficTablet","contribution"),
                dateTime));

        events.put("2", prepareCounterEventData("member321",
                Arrays.asList("pageView","trafficMobile"),
                dateTime));

        events.put("3", prepareCounterEventData("member123",
                Arrays.asList("pageView","trafficTablet"),
                dateTime.plusHours(1)));

        counterStorage.bufferMetrics(events);

        List<String> namespaces =
                counterStorage.getNamespacesFromMetricsBuffer();

        Assert.assertNotNull(namespaces);
        Assert.assertTrue(namespaces.containsAll(
                Lists.asList("1", new String[] {"2", "3"})));
    }

}
