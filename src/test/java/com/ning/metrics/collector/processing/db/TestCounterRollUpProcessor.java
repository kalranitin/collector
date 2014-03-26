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
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.ning.metrics.collector.guice.module.CollectorObjectMapperModule;
import com.ning.metrics.collector.processing.counter.RollUpCounterProcessor;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Test(groups = {"slow", "database"})
public class TestCounterRollUpProcessor
{
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
    
    private static CounterEventData prepareCounterEventData(String id, Integer category, List<String> counters, DateTime createdDateTime){
        Map<String,Integer> counterMap = new HashMap<String, Integer>();
        for(String s : counters)
        {
            counterMap.put(s, 1);
        }
        
        return new CounterEventData(id, category, createdDateTime, counterMap);
    }
    
    @Test(groups = {"slow", "database"})
    public void testCounterRollUpProcessor() throws Exception
    {
        String jsonData = "{\"appId\":\"network_111\","
                + "\"identifierDistribution\":"
                + "{\"1\":[\"pageView\",\"memberJoined\"],\"2\":[\"contentViewed\",\"contentLike\"]}"
                + "}";
        
        CounterSubscription counterSubscription = mapper.readValue(jsonData, CounterSubscription.class);
        
        Long id = counterStorage.createCounterSubscription(counterSubscription);
        Multimap<Long, CounterEventData> multimap = ArrayListMultimap.create();
        
        multimap.put(id, prepareCounterEventData("member111", 1, Arrays.asList("pageView","trafficTablet","contribution"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member112", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member113", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member114", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member115", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member116", 1, Arrays.asList("pageView","trafficTablet","contribution"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member117", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member118", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member119", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member121", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        
        counterStorage.insertDailyMetrics(multimap);
        
        counterProcessor.rollUpDailyCounters(counterStorage.loadCounterSubscription("network_111"));
        
        List<RolledUpCounter> rolledUpCounterList = counterStorage.loadRolledUpCounters(id, new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC), null, false, null);
        
        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertTrue(rolledUpCounterList.size() == 1);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "uniques").getTotalCount() == 10);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "pageView").getDistribution().containsKey("member111"));
        
        
    }
    
    @Test(groups = {"slow", "database"})
    public void testStreamingCounterRollUpProcessor() throws Exception
    {
        String jsonData = "{\"appId\":\"network_111\","
                + "\"identifierDistribution\":"
                + "{\"1\":[\"pageView\",\"memberJoined\"],\"2\":[\"contentViewed\",\"contentLike\"]}"
                + "}";
        
        CounterSubscription counterSubscription = mapper.readValue(jsonData, CounterSubscription.class);
        
        Long id = counterStorage.createCounterSubscription(counterSubscription);
        Multimap<Long, CounterEventData> multimap = ArrayListMultimap.create();
        
        multimap.put(id, prepareCounterEventData("member111", 1, Arrays.asList("pageView","trafficTablet","contribution"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member112", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member113", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member114", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member115", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member116", 1, Arrays.asList("pageView","trafficTablet","contribution"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member117", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member118", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member119", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        multimap.put(id, prepareCounterEventData("member121", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));
        
        counterStorage.insertDailyMetrics(multimap);
        
        counterProcessor.rollUpStreamingDailyCounters(counterStorage.loadCounterSubscription("network_111"));
        
        List<RolledUpCounter> rolledUpCounterList = counterStorage.loadRolledUpCounters(id, new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC), null, false, null);
        
        Assert.assertNotNull(rolledUpCounterList);
        
        
    }
    
    @Test(groups = {"slow", "database"})
    public void testLoadAggregatedRolledUpCounters() throws Exception
    {
        String jsonData = "{\"appId\":\"network_112\","
                + "\"identifierDistribution\":"
                + "{\"1\":[\"pageView\",\"memberJoined\"],\"2\":[\"contentViewed\",\"contentLike\"]}"
                + "}";
        
        CounterSubscription counterSubscription = mapper.readValue(jsonData, CounterSubscription.class);
        
        Long id = counterStorage.createCounterSubscription(counterSubscription);
        Multimap<Long, CounterEventData> multimap = ArrayListMultimap.create();
        DateTime dateTime = new DateTime(2014,2,2,1,0,DateTimeZone.UTC);
        
        multimap.put(id, prepareCounterEventData("member111", 1, Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(id, prepareCounterEventData("member112", 1, Arrays.asList("pageView","trafficTablet"),dateTime));
        multimap.put(id, prepareCounterEventData("member113", 1, Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(id, prepareCounterEventData("member114", 1, Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        multimap.put(id, prepareCounterEventData("member115", 1, Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(id, prepareCounterEventData("member116", 1, Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(1)));
        multimap.put(id, prepareCounterEventData("member117", 1, Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(id, prepareCounterEventData("member118", 1, Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(2)));
        multimap.put(id, prepareCounterEventData("member119", 1, Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        multimap.put(id, prepareCounterEventData("member120", 1, Arrays.asList("pageView","trafficTablet"),dateTime.plusDays(3)));
        
        counterStorage.insertDailyMetrics(multimap);
        counterProcessor.rollUpDailyCounters(counterStorage.loadCounterSubscription("network_112"));
        
        Optional<String> fromDateOpt = Optional.of("2014-02-02");
        Optional<String> toDateOpt = Optional.absent();
        Optional<Set<String>> counterNames = Optional.absent();
        
        List<RolledUpCounter> rolledUpCounterList = counterProcessor.loadAggregatedRolledUpCounters("network_112", fromDateOpt,toDateOpt,counterNames, false, false, Optional.of(0));
        
        Assert.assertNotNull(rolledUpCounterList);
        Assert.assertEquals(rolledUpCounterList.size(),4);
        Assert.assertTrue(rolledUpCounterList.get(0).getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "pageView").getDistribution().containsKey("member111"));
        
    }
    
    

}
