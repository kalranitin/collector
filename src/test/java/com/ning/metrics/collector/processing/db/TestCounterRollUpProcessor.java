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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.ning.metrics.collector.guice.module.CollectorObjectMapperModule;
import com.ning.metrics.collector.processing.counter.CompositeCounter;
import com.ning.metrics.collector.processing.counter.RollUpCounterProcessor;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;
import com.ning.metrics.collector.processing.db.model.RolledUpCounterData;
import java.util.Arrays;
import java.util.HashMap;
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
public class TestCounterRollUpProcessor
{
    // Each of these tests needs its own namespace so they don't interfere with
    // each other.  This number will be incremented and used for each test
    private static int lastNamespaceNumber = 0;

    private CollectorMysqlTestingHelper helper;

    @Inject
    ObjectMapper mapper;

    @Inject
    CounterStorage counterStorage;

    @Inject
    RollUpCounterProcessor counterProcessor;

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

    /**
     * Explicitely create a counter event data object
     * @param namespace
     * @param category
     * @param counters
     * @param createdDateTime
     * @param count
     * @return
     */
    private static CounterEventData prepareCounterEventData(
            String namespace, List<String> counters,
            DateTime createdDateTime, int count) {

        Map<String,Integer> counterMap = new HashMap<String, Integer>();
        for(String s : counters)
        {
            counterMap.put(s, count);
        }

        return new CounterEventData(namespace, createdDateTime, counterMap);
    }

    /**
     * Explicitely create a counter event data object with a count of 1 for all
     * counters
     *
     * @param id
     * @param category
     * @param counters
     * @param createdDateTime
     * @return
     */
    private static CounterEventData prepareCounterEventData(String id,
            List<String> counters, DateTime createdDateTime){
        return prepareCounterEventData(id, counters, createdDateTime, 1);
    }

    @Test(groups = {"slow", "database"})
    public void testCounterRollUpProcessor() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap
                = ArrayListMultimap.create();

        multimap.put(namespace, prepareCounterEventData("member111",
                Arrays.asList("pageView","trafficTablet","contribution"),
                new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member112",
                Arrays.asList("pageView","trafficMobile"),
                new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","trafficTablet","contribution"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member121", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));

        counterStorage.bufferMetrics(multimap);

        counterProcessor.rollUpDailyCounters(namespace);

        List<RolledUpCounter> rolledUpCounterList
                = counterStorage.queryDailyRolledUpCounters(
                        namespace,
                        new DateTime(DateTimeZone.UTC),
                        new DateTime(DateTimeZone.UTC),
                        null,
                        false,
                        null,
                        null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertTrue(rolledUpCounterList.size() == 1);
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary()
                .get("pageView").getUniqueCount(), 10);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary()
                .get("pageView").getDistribution().containsKey("member111"));


    }

    @Test(groups = {"slow", "database"})
    public void testStreamingCounterRollUpProcessor() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","trafficTablet","contribution"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","trafficTablet","contribution"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(namespace, prepareCounterEventData("member121", Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));

        counterStorage.bufferMetrics(multimap);

        counterProcessor.rollUpStreamingDailyCounters(namespace);

        List<RolledUpCounter> rolledUpCounterList
                = counterStorage.queryDailyRolledUpCounters(
                        namespace,
                        new DateTime(DateTimeZone.UTC),
                        new DateTime(DateTimeZone.UTC),
                        null,
                        false,
                        null,
                        null);

        Assert.assertNotNull(rolledUpCounterList);


    }

    @Test(groups = {"slow", "database"})
    public void testLoadAggregatedRolledUpCounters() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames = Optional.absent();

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, null, false, false, false, (Optional)Optional.absent(), Optional.of(0));

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),4);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member111"));

    }


    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCountersAggregatedOverAll_simple() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames = Optional.absent();

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, null, false, true, false, (Optional)Optional.absent(), null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),1);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member111"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member115"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member117"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member120"));
        Assert.assertEquals(10, rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().size());
        Assert.assertEquals(10, rolledUpCounterList.get(0).getCounterSummary().get("pageView").getUniqueCount());
        Assert.assertEquals(2, rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().get("member112").intValue());

    }


    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCountersAggregatedOverAll_excludeDistribution() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames = Optional.absent();

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, null, false, true, true, (Optional)Optional.absent(), null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),1);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().isEmpty());
        Assert.assertEquals(10, rolledUpCounterList.get(0).getCounterSummary().get("pageView").getUniqueCount());

    }


    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCountersAggregatedOverAll_limitDistribution() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames = Optional.absent();

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, null, false, true, false, (Optional)Optional.absent(), Optional.of(3));

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),1);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member120"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member112"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member113"));

        RolledUpCounterData pageViewCounter =
                rolledUpCounterList.get(0).getCounterSummary().get("pageView");

        Assert.assertNotNull(pageViewCounter);

        // Deserialize this RolledUpCounterData to apply the serialization
        // limit
        pageViewCounter = mapper.readValue(
                mapper.writeValueAsString(pageViewCounter),
                RolledUpCounterData.class);

        Assert.assertEquals(3, pageViewCounter.getDistribution().size());
        Assert.assertEquals(2,
                pageViewCounter.getDistribution().get("member112").intValue());

    }


    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCountersAggregatedOverAll_filterDistribution() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames = Optional.absent();

        List<RolledUpCounter> rolledUpCounterList
                = counterProcessor.loadAggregatedRolledUpCounters(
                        namespace,
                        fromDateOpt,toDateOpt,counterNames,
                        null,
                        false,
                        true,
                        false,
                        Optional.of((Set<String>)Sets.newHashSet(
                                "member112", "member119")),
                        null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),1);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member119"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member112"));
        Assert.assertEquals(2, rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().size());
        Assert.assertEquals(2, rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().get("member112").intValue());

    }


    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCounters_simpleComposite() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames
                = Optional.of((Set<String>)Sets.newHashSet("pageView"));

        Optional<Set<CompositeCounter>> composites = Optional.of(
                (Set<CompositeCounter>)Sets.newHashSet(new CompositeCounter(
                        "doublePageView", new String[] {"pageView"},
                        new int[] {2})));

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, composites, false, false, false, null, null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),4);
        Assert.assertFalse(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member119"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member112"));
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("pageView"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member111"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().get("member111").intValue(), 1);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("doublePageView"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("doublePageView").getDistribution().containsKey("member111"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("doublePageView").getDistribution().get("member111").intValue(), 2);
        Assert.assertNotNull(rolledUpCounterList.get(3).getCounterSummary().get("pageView"));
        Assert.assertTrue(rolledUpCounterList.get(3).getCounterSummary().get("pageView").getDistribution().containsKey("member120"));
        Assert.assertEquals(rolledUpCounterList.get(3).getCounterSummary().get("pageView").getDistribution().get("member120").intValue(), 2);
        Assert.assertNotNull(rolledUpCounterList.get(3).getCounterSummary().get("doublePageView"));
        Assert.assertTrue(rolledUpCounterList.get(3).getCounterSummary().get("doublePageView").getDistribution().containsKey("member120"));
        Assert.assertEquals(rolledUpCounterList.get(3).getCounterSummary().get("doublePageView").getDistribution().get("member120").intValue(), 4);
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCounters_complexComposite() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed","contentLike"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames
                = Optional.of((Set<String>)Sets.newHashSet(
                        "pageView", "memberJoined", "contentViewed",
                        "contentLike"));

        Optional<Set<CompositeCounter>> composites = Optional.of(
                (Set<CompositeCounter>)Sets.newHashSet(
                        new CompositeCounter("memberScore",
                                new String[] {"pageView", "memberJoined"},
                                new int[] {2, 10}),
                        new CompositeCounter("contentScore",
                                new String[] {"contentViewed", "contentLike"},
                                new int[] {1, 5})));

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, composites, false, false, false, null, null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),4);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().containsKey("member111"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().get("member111").intValue(), 10 + 2);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().containsKey("member120"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().get("member120").intValue(), 10 + 4);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("contentScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("contentScore").getDistribution().containsKey("content111"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("contentScore").getDistribution().get("content111").intValue(), 5 + 1);
        Assert.assertNotNull(rolledUpCounterList.get(3).getCounterSummary().get("memberScore"));
        Assert.assertTrue(rolledUpCounterList.get(3).getCounterSummary().get("memberScore").getDistribution().containsKey("member120"));
        Assert.assertEquals(rolledUpCounterList.get(3).getCounterSummary().get("memberScore").getDistribution().get("member120").intValue(), 4);
        Assert.assertNotNull(rolledUpCounterList.get(3).getCounterSummary().get("contentScore"));
        Assert.assertTrue(rolledUpCounterList.get(3).getCounterSummary().get("contentScore").getDistribution().containsKey("content111"));
        Assert.assertEquals(rolledUpCounterList.get(3).getCounterSummary().get("contentScore").getDistribution().get("content111").intValue(), 1);
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCounters_aggregateWithComposites() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed","contentLike"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("content112", Arrays.asList("contentViewed"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames
                = Optional.of((Set<String>)Sets.newHashSet(
                        "pageView", "memberJoined", "contentViewed",
                        "contentLike"));

        Optional<Set<CompositeCounter>> composites = Optional.of(
                (Set<CompositeCounter>)Sets.newHashSet(
                        new CompositeCounter("memberScore",
                                new String[] {"pageView", "memberJoined"},
                                new int[] {2, 10}),
                        new CompositeCounter("contentScore",
                                new String[] {"contentViewed", "contentLike"},
                                new int[] {1, 5})));

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, composites, false, true, false, null, null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),1);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().containsKey("member111"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().get("member111").intValue(), 10 + 2);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().containsKey("member120"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().get("member120").intValue(), 10 + 8);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("contentScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("contentScore").getDistribution().containsKey("content111"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("contentScore").getDistribution().get("content111").intValue(), 5 + 2);
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("contentScore").getTotalCount(), 5 + 3);
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCounters_compositeExcludeDistribution() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed","contentLike"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames
                = Optional.of((Set<String>)Sets.newHashSet(
                        "pageView", "memberJoined", "contentViewed",
                        "contentLike"));

        Optional<Set<CompositeCounter>> composites = Optional.of(
                (Set<CompositeCounter>)Sets.newHashSet(
                        new CompositeCounter("memberScore",
                                new String[] {"pageView", "memberJoined"},
                                new int[] {2, 10}),
                        new CompositeCounter("contentScore",
                                new String[] {"contentViewed", "contentLike"},
                                new int[] {1, 5})));

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, composites, false, false, true, null, null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),4);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().isEmpty());
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("contentScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("contentScore").getDistribution().isEmpty());
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("contentScore").getTotalCount(), 5 + 1);
        Assert.assertNotNull(rolledUpCounterList.get(2).getCounterSummary().get("memberScore"));
        Assert.assertTrue(rolledUpCounterList.get(2).getCounterSummary().get("memberScore").getDistribution().isEmpty());
        Assert.assertEquals(rolledUpCounterList.get(2).getCounterSummary().get("memberScore").getTotalCount(), 20 + 4);
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCounters_aggregateWithCompositesAndExcludeDistribution() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed","contentLike"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("content112", Arrays.asList("contentViewed"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames
                = Optional.of((Set<String>)Sets.newHashSet(
                        "pageView", "memberJoined", "contentViewed",
                        "contentLike"));

        Optional<Set<CompositeCounter>> composites = Optional.of(
                (Set<CompositeCounter>)Sets.newHashSet(
                        new CompositeCounter("memberScore",
                                new String[] {"pageView", "memberJoined"},
                                new int[] {2, 10}),
                        new CompositeCounter("contentScore",
                                new String[] {"contentViewed", "contentLike"},
                                new int[] {1, 5})));

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, composites, false, true, true, null, null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),1);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().isEmpty());
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().isEmpty());
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("contentScore"));
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("contentScore").getDistribution().isEmpty());
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("contentViewed").getUniqueCount(), 2);
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("contentScore").getTotalCount(), 5 + 3);
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCounters_compositeLimitDistribution() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed","contentLike"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames
                = Optional.of((Set<String>)Sets.newHashSet(
                        "pageView", "memberJoined", "contentViewed",
                        "contentLike"));

        Optional<Set<CompositeCounter>> composites = Optional.of(
                (Set<CompositeCounter>)Sets.newHashSet(
                        new CompositeCounter("memberScore",
                                new String[] {"pageView", "memberJoined"},
                                new int[] {2, 10}),
                        new CompositeCounter("contentScore",
                                new String[] {"contentViewed", "contentLike"},
                                new int[] {1, 5})));

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, composites, false, false, false, null, Optional.of(1));

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),4);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));

        RolledUpCounterData memberScoreCounter =
                rolledUpCounterList.get(0).getCounterSummary().get("memberScore");

        // Deserialize this RolledUpCounterData to apply the serialization
        // limit
        memberScoreCounter = mapper.readValue(
                mapper.writeValueAsString(memberScoreCounter),
                RolledUpCounterData.class);

        Assert.assertEquals(memberScoreCounter.getDistribution().size(), 1);
        Assert.assertTrue(memberScoreCounter.getDistribution().containsKey("member113"));
        Assert.assertEquals(memberScoreCounter.getDistribution().get("member113").intValue(), 10 + 6);
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCounters_aggregageCompositeAndLimitDistribution() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed","contentLike"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames
                = Optional.of((Set<String>)Sets.newHashSet(
                        "pageView", "memberJoined", "contentViewed",
                        "contentLike"));

        Optional<Set<CompositeCounter>> composites = Optional.of(
                (Set<CompositeCounter>)Sets.newHashSet(
                        new CompositeCounter("memberScore",
                                new String[] {"pageView", "memberJoined"},
                                new int[] {2, 10}),
                        new CompositeCounter("contentScore",
                                new String[] {"contentViewed", "contentLike"},
                                new int[] {1, 5})));

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, composites, false, true, false, null, Optional.of(1));

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),1);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));

        RolledUpCounterData memberScoreCounter =
                rolledUpCounterList.get(0).getCounterSummary().get("memberScore");

        Assert.assertNotNull(memberScoreCounter);

        // Deserialize this RolledUpCounterData to apply the serialization
        // limit
        memberScoreCounter = mapper.readValue(
                mapper.writeValueAsString(memberScoreCounter),
                RolledUpCounterData.class);


        Assert.assertEquals(memberScoreCounter.getDistribution().size(), 1);
        Assert.assertTrue(memberScoreCounter.getDistribution().containsKey("member120"));
        Assert.assertEquals(memberScoreCounter.getDistribution().get("member120").intValue(), 10 + 8);
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCounters_compositeAndFilterDistribution() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed","contentLike"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames
                = Optional.of((Set<String>)Sets.newHashSet(
                        "pageView", "memberJoined", "contentViewed",
                        "contentLike"));

        Optional<Set<CompositeCounter>> composites = Optional.of(
                (Set<CompositeCounter>)Sets.newHashSet(
                        new CompositeCounter("memberScore",
                                new String[] {"pageView", "memberJoined"},
                                new int[] {2, 10}),
                        new CompositeCounter("contentScore",
                                new String[] {"contentViewed", "contentLike"},
                                new int[] {1, 5})));

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, composites, false, false, false, Optional.of((Set<String>)Sets.newHashSet("member113")), null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),4);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().size(), 1);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().containsKey("member113"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().get("member113").intValue(), 10 + 6);
    }

    @Test(groups = {"slow", "database"})
    public void testLoadRolledUpCounters_aggregageCompositeAndFilterDistribution() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData("member111", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","memberJoined"),dateTime));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed","contentLike"),dateTime));
        multimap.put(namespace, prepareCounterEventData("member112", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member113", Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member114", Arrays.asList("pageView","memberJoined"),dateTime.plusHours(1)));
        multimap.put(namespace, prepareCounterEventData("member115", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member116", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(1)));
        multimap.put(namespace, prepareCounterEventData("member117", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member118", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(2)));
        multimap.put(namespace, prepareCounterEventData("member119", Arrays.asList("pageView","memberJoined"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("content111", Arrays.asList("contentViewed"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(namespace, prepareCounterEventData("member120", Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames
                = Optional.of((Set<String>)Sets.newHashSet(
                        "pageView", "memberJoined", "contentViewed",
                        "contentLike"));

        Optional<Set<CompositeCounter>> composites = Optional.of(
                (Set<CompositeCounter>)Sets.newHashSet(
                        new CompositeCounter("memberScore",
                                new String[] {"pageView", "memberJoined"},
                                new int[] {2, 10}),
                        new CompositeCounter("contentScore",
                                new String[] {"contentViewed", "contentLike"},
                                new int[] {1, 5})));

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, composites, false, true, false, Optional.of((Set<String>)Sets.newHashSet("member113")), null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),1);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary().get("memberScore"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().size(), 1);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().containsKey("member113"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("memberScore").getDistribution().get("member113").intValue(), 10 + 6);
    }

    @Test(groups = {"slow", "database"})
    public void testNegativeCounts() throws Exception
    {
        String namespace = "namespace_" + (++lastNamespaceNumber);

        Multimap<String, CounterEventData> multimap = ArrayListMultimap.create();

        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);

        multimap.put(namespace, prepareCounterEventData(
                "member111", Arrays.asList("pageView","memberJoined"),
                dateTime, -1));

        counterStorage.bufferMetrics(multimap);
        counterProcessor.rollUpDailyCounters(namespace);

        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames
                = Optional.of((Set<String>)Sets.newHashSet(
                        "pageView", "memberJoined", "contentViewed",
                        "contentLike"));

        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters(namespace, fromDateOpt,toDateOpt,counterNames, null, false, true, false, Optional.of((Set<String>)Sets.newHashSet("member111")), null);

        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),1);
        Assert.assertNotNull(rolledUpCounterList.get(0).getCounterSummary()
                .get("pageView"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().size(), 1);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().containsKey("member111"));
        Assert.assertEquals(rolledUpCounterList.get(0).getCounterSummary().get("pageView").getDistribution().get("member111").intValue(), -1);

    }


}
