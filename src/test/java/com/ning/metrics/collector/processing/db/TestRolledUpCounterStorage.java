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
import com.google.common.base.Optional;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.ning.metrics.collector.guice.module.CollectorObjectMapperModule;
import com.ning.metrics.collector.processing.counter.CounterDistribution;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;
import com.ning.metrics.collector.processing.db.model.RolledUpCounterData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {"slow", "database"})
public class TestRolledUpCounterStorage
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

    private RolledUpCounter prepareRolledUpCounterData(DateTime fromDate, DateTime toDate) throws Exception
    {
        String json = "{"
            + "\"namespace\":\"network_111\","
            + "\"fromDate\":\""+RolledUpCounter.DATE_FORMATTER.print(fromDate)+"\","
            + "\"toDate\":\""+RolledUpCounter.DATE_FORMATTER.print(toDate) + "\","
            + "\"counterSummary\":"
            + "{"
                + "\"contribution\":{\"counterName\":\"contribution\",\"totalCount\":2,\"distribution\":{\"member123\":2}},"
                + "\"pageView\":{\"counterName\":\"pageView\",\"totalCount\":3,\"distribution\":{\"member321\":1,\"member123\":2}},"
                + "\"trafficTablet\":{\"counterName\":\"trafficTablet\",\"totalCount\":1,\"distribution\":{}},"
                + "\"trafficMobile\":{\"counterName\":\"trafficMobile\",\"totalCount\":2,\"distribution\":{}}"
            + "}"
        + "}";

        return mapper.readValue(json, RolledUpCounter.class);
    }

    private static CounterEventData prepareCounterEventData(String id, List<String> counters){
        Map<String,Integer> counterMap = new HashMap<String, Integer>();
        for(String s : counters)
        {
            counterMap.put(s, 1);
        }

        return new CounterEventData(id, new DateTime(DateTimeZone.UTC), counterMap);
    }

    private static List<String> getIdentifierDistribution(int identifierCategory)
    {
        switch (identifierCategory) {
            case 0: return new ArrayList<String>();
            case 1: return Arrays.asList("pageView","contribution");
            case 2: return Arrays.asList("contentViewed","contentLike");
            default: return new ArrayList<String>();
        }
    }


    @Test(groups = {"fast"})
    public void testDistributionSerialization() throws Exception {

        CounterDistribution dist = new CounterDistribution();
        dist.increment("1", 10);
        dist.increment("2", -100343);
        dist.increment("3", 2);
        dist.increment("4", 0); // this one will not be serialized

        RolledUpCounterData tester = new RolledUpCounterData(
                "tester", -100333, 3, dist);

        byte[] serDist = DatabaseCounterStorage.serializeDistribution(tester);
        Map<String, Integer> testDist
                = DatabaseCounterStorage.deserializeDistribution(serDist,
                        Optional.<Set<String>>absent(),
                        Optional.<Integer>absent());

        Assert.assertNotNull(testDist);
        Assert.assertEquals(3, testDist.size());

        String[] keyAnswers = new String[] {
            "1", "3", "2"
        };

        int[] valueAnswers = new int[] {
            10, 2, -100343
        };

        int index = 0;

        for (Map.Entry<String, Integer> e : testDist.entrySet()) {

            Assert.assertEquals(e.getKey(), keyAnswers[index]);
            Assert.assertEquals(e.getValue().intValue(), valueAnswers[index]);

            index++;
        }
    }


    @Test(groups = {"slow", "database"})
    public void testInsertRolledUpCounter() throws Exception{
        DateTime dateTime = new DateTime(
                RolledUpCounter.DATE_FORMATTER.parseMillis("2014-01-24"),
                DateTimeZone.UTC);

        RolledUpCounter rolledUpCounter = prepareRolledUpCounterData(
                dateTime, dateTime);
        String id = counterStorage.insertOrUpdateDailyRolledUpCounter(
                rolledUpCounter);

        Assert.assertNotNull(id);
        Assert.assertEquals(id, "network_111|2014-01-24");
    }

    @Test(groups = {"slow", "database"})
    public void testLoadAndUpdateRolledUpCounter() throws Exception{
        DateTime dateTime = new DateTime(RolledUpCounter
                .DATE_FORMATTER.parseMillis("2014-01-24"),DateTimeZone.UTC);

        RolledUpCounter rolledUpCounter = prepareRolledUpCounterData(
                dateTime, dateTime);
        String id = counterStorage.insertOrUpdateDailyRolledUpCounter(
                rolledUpCounter);

        rolledUpCounter = counterStorage.loadDailyRolledUpCounter(
                rolledUpCounter.getNamespace(),
                rolledUpCounter.getFromDateActual());

        Assert.assertNotNull(rolledUpCounter);
        Assert.assertEquals(rolledUpCounter.getNamespace(), "network_111");

        rolledUpCounter.updateRolledUpCounterData(prepareCounterEventData(
                "member321", Arrays.asList("pageView","trafficMobile")));
        rolledUpCounter.updateRolledUpCounterData(prepareCounterEventData(
                "member111", Arrays.asList("pageView","trafficMobile")));

        Assert.assertEquals(5, rolledUpCounter.getCounterSummary()
                .get("pageView").getTotalCount());

        id = counterStorage.insertOrUpdateDailyRolledUpCounter(rolledUpCounter);

        Assert.assertNotNull(id);
        Assert.assertEquals(id, "network_111|2014-01-24");
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCountersByDateRange() throws Exception{
        DateTime date_22 = new DateTime(
                RolledUpCounter.DATE_FORMATTER.parseMillis("2014-01-22"),
                DateTimeZone.UTC);
        DateTime date_23 = new DateTime(
                RolledUpCounter.DATE_FORMATTER.parseMillis("2014-01-23"),
                DateTimeZone.UTC);
        DateTime date_24 = new DateTime(
                RolledUpCounter.DATE_FORMATTER.parseMillis("2014-01-24"),
                DateTimeZone.UTC);

        RolledUpCounter rolledUpCounter_22
                = prepareRolledUpCounterData(date_22, date_22);
        RolledUpCounter rolledUpCounter_23
                = prepareRolledUpCounterData(date_23, date_23);
        RolledUpCounter rolledUpCounter_24
                = prepareRolledUpCounterData(date_24, date_24);

        counterStorage.insertOrUpdateDailyRolledUpCounter(rolledUpCounter_22);
        counterStorage.insertOrUpdateDailyRolledUpCounter(rolledUpCounter_23);
        counterStorage.insertOrUpdateDailyRolledUpCounter(rolledUpCounter_24);

        List<RolledUpCounter> rolledUpCounters
                = counterStorage.queryDailyRolledUpCounters("network_111",
                        date_22, date_24, null, true, null, null);

        Assert.assertNotNull(rolledUpCounters);
        Assert.assertTrue(rolledUpCounters.size() == 3);
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCountersByStartDate() throws Exception{
        DateTime dateTime = new DateTime(
                RolledUpCounter.DATE_FORMATTER.parseMillis("2014-01-24"),
                DateTimeZone.UTC);

        RolledUpCounter rolledUpCounter
                = prepareRolledUpCounterData(dateTime, dateTime);
        counterStorage.insertOrUpdateDailyRolledUpCounter(rolledUpCounter);

        List<RolledUpCounter> rolledUpCounters
                = counterStorage.queryDailyRolledUpCounters("network_111",
                        dateTime, null, null, false, null, null);

        Assert.assertNotNull(rolledUpCounters);
        Assert.assertFalse(rolledUpCounters.isEmpty());
        Assert.assertEquals(rolledUpCounters.get(0).getNamespace(),
                "network_111");

    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCountersByEndDate() throws Exception{
        DateTime dateTime = new DateTime(
                RolledUpCounter.DATE_FORMATTER.parseMillis("2014-01-24"),
                DateTimeZone.UTC);

        RolledUpCounter rolledUpCounter
                = prepareRolledUpCounterData(dateTime, dateTime);
        counterStorage.insertOrUpdateDailyRolledUpCounter(rolledUpCounter);

        List<RolledUpCounter> rolledUpCounters
                = counterStorage.queryDailyRolledUpCounters(
                        "network_111", null, dateTime, null, false, null, null);

        Assert.assertNotNull(rolledUpCounters);
        Assert.assertFalse(rolledUpCounters.isEmpty());
        Assert.assertEquals(rolledUpCounters.get(0).getNamespace(),"network_111");
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCounterForCounterNames() throws Exception{
        DateTime dateTime = new DateTime(
                RolledUpCounter.DATE_FORMATTER.parseMillis("2014-01-24"),
                DateTimeZone.UTC);

        RolledUpCounter rolledUpCounter
                = prepareRolledUpCounterData(dateTime, dateTime);
        counterStorage.insertOrUpdateDailyRolledUpCounter(rolledUpCounter);
        Set<String> counterNameSet = new HashSet<String>();
        counterNameSet.add("pageView");
        Optional<Set<String>> optional = Optional.of(counterNameSet);

        List<RolledUpCounter> rolledUpCounters
                = counterStorage.queryDailyRolledUpCounters(
                        "network_111", null, null, optional, true, null, null);

        Assert.assertNotNull(rolledUpCounters);
        Assert.assertFalse(rolledUpCounters.isEmpty());
        Assert.assertEquals(rolledUpCounters.get(0).getNamespace(),
                "network_111");

        Assert.assertNotNull(
                rolledUpCounters.get(0).getCounterSummary().get("pageView"));
        Assert.assertNull(rolledUpCounters.get(0).getCounterSummary().get(
                "trafficMobile"));
    }

    @Test(groups = {"slow", "database"})
    public void testCleanUpRolledUpCounters() throws Exception{
        DateTime date_22 = new DateTime(
                RolledUpCounter.DATE_FORMATTER.parseMillis("2014-01-22"),
                DateTimeZone.UTC);
        DateTime date_23 = new DateTime(
                RolledUpCounter.DATE_FORMATTER.parseMillis("2014-01-23"),
                DateTimeZone.UTC);
        DateTime date_24 = new DateTime(
                RolledUpCounter.DATE_FORMATTER.parseMillis("2014-01-24"),
                DateTimeZone.UTC);


        RolledUpCounter rolledUpCounter_22
                = prepareRolledUpCounterData(date_22, date_22);
        RolledUpCounter rolledUpCounter_23
                = prepareRolledUpCounterData(date_23, date_23);
        RolledUpCounter rolledUpCounter_24
                = prepareRolledUpCounterData(date_24, date_24);

        counterStorage.insertOrUpdateDailyRolledUpCounter(rolledUpCounter_22);
        counterStorage.insertOrUpdateDailyRolledUpCounter(rolledUpCounter_23);
        counterStorage.insertOrUpdateDailyRolledUpCounter(rolledUpCounter_24);

        counterStorage.cleanExpiredDailyRolledUpCounters(date_24);

        List<RolledUpCounter> rolledUpCounters
                = counterStorage.queryDailyRolledUpCounters(
                        "network_111", date_22, date_24,
                        null, false, null, null);

        Assert.assertTrue(rolledUpCounters == null || rolledUpCounters.isEmpty());
    }

}
