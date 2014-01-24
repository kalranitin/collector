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
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Objects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;

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

@Test(groups = {"slow", "database"})
public class TestCounterStorage
{
    private CollectorMysqlTestingHelper helper;
    private static final ObjectMapper mapper = new ObjectMapper();
    
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
        
        Guice.createInjector(new DBConfigModule()).injectMembers(this);
        
        mapper.registerModule(new JodaModule());
        mapper.registerModule(new GuavaModule());
                
    }
    
    @BeforeMethod(alwaysRun = true, groups = {"slow", "database"})
    public void clearDB(){
        helper.clear();
    }
    
    @AfterClass(alwaysRun = true,groups = {"slow", "database"})
    public void stopDB() throws Exception{
        helper.stopMysql();
    }
    
    @Test
    public void testCreateCounterSubscription() throws Exception
    {
        String jsonData = "{\"appId\":\"network_111\","
                + "\"identifierDistribution\":"
                + "{\"1\":[\"pageView\",\"memberJoined\"],\"2\":[\"contentViewed\",\"contentLike\"]}"
                + "}";
        
        CounterSubscription counterSubscription = mapper.readValue(jsonData, CounterSubscription.class);
        
        Long id = counterStorage.createCounterSubscription(counterSubscription);
        
        Assert.assertNotNull(id);
        
        counterSubscription = counterStorage.loadCounterSubscription("network_111");
        
        Assert.assertNotNull(counterSubscription);
        Assert.assertEquals(counterSubscription.getId(), id);
        
    }
    
    @Test
    public void testInsertAndLoadDailyMetrics() throws Exception
    {
        
        Multimap<Long, CounterEventData> multimap = ArrayListMultimap.create();
        multimap.put(1L, prepareCounterEventData("member123", 1, Arrays.asList("pageView","trafficTablet","contribution"),new DateTime(DateTimeZone.UTC)));
        multimap.put(1L, prepareCounterEventData("member321", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC))); 
        
        counterStorage.insertDailyMetrics(multimap);
        
        List<CounterEventData> dailyList = counterStorage.loadDailyMetrics(1L, null);
        
        Assert.assertNotNull(dailyList);
        Assert.assertFalse(dailyList.isEmpty());
        Assert.assertEquals(2, dailyList.size());
        Assert.assertTrue(Objects.equal("member123", dailyList.get(0).getUniqueIdentifier()) || Objects.equal("member321", dailyList.get(0).getUniqueIdentifier()));
    }
    
    @Test
    public void testInsertAndLoadGroupedDailyMetrics() throws Exception
    {
        Multimap<Long, CounterEventData> multimap = ArrayListMultimap.create();
        
        for(int j=0;j<100;j++)
        {
            multimap.put(1L, prepareCounterEventData("member123", 1, Arrays.asList("pageView","trafficTablet","contribution"),new DateTime(DateTimeZone.UTC)));
            multimap.put(1L, prepareCounterEventData("member321", 1, Arrays.asList("pageView","trafficMobile"),new DateTime(DateTimeZone.UTC)));                
        }            
        
        counterStorage.insertDailyMetrics(multimap);
        
        List<CounterEventData> dailyList = counterStorage.loadGroupedDailyMetrics(1L, new DateTime(DateTimeZone.UTC));
        
        Assert.assertNotNull(dailyList);
        Assert.assertFalse(dailyList.isEmpty());
        Assert.assertEquals(2, dailyList.size());
        Assert.assertTrue(dailyList.get(0).getCounters().get("pageView") == 100);
    }
    
    private static CounterEventData prepareCounterEventData(String id, int category, List<String> counters, DateTime createdDateTime){
        Map<String,Integer> counterMap = new HashMap<String, Integer>();
        for(String s : counters)
        {
            counterMap.put(s, 1);
        }
        
        return new CounterEventData(id, category, createdDateTime, counterMap);
    }
    
    @Test
    public void testDeleteDailyMetrics() throws Exception{
        Multimap<Long, CounterEventData> multimap = ArrayListMultimap.create();
        
        DateTime dateTime = new DateTime(DateTimeZone.UTC);
        
        multimap.put(1L, prepareCounterEventData("member123", 1, Arrays.asList("pageView","trafficTablet","contribution"),dateTime));
        multimap.put(1L, prepareCounterEventData("member321", 1, Arrays.asList("pageView","trafficMobile"),dateTime));       
        multimap.put(1L, prepareCounterEventData("member123", 1, Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        
        counterStorage.insertDailyMetrics(multimap);
        
        List<CounterEventData> dailyList = counterStorage.loadGroupedDailyMetrics(1L, dateTime);
        
        Assert.assertNotNull(dailyList);
        Assert.assertFalse(dailyList.isEmpty());
        
        boolean deleted = counterStorage.deleteDailyMetrics(1L, dateTime);
        
        Assert.assertTrue(deleted);
        
        dailyList = counterStorage.loadGroupedDailyMetrics(1L, dateTime);
        Assert.assertNotNull(dailyList);
        Assert.assertTrue(dailyList.isEmpty());
        
        dailyList = counterStorage.loadGroupedDailyMetrics(1L, dateTime.plusHours(1));
        Assert.assertNotNull(dailyList);
        Assert.assertFalse(dailyList.isEmpty());
        Assert.assertTrue(dailyList.size() == 1);
        
    }
    
    @Test
    public void testGetSubscritionIdsFromDailyMetrics()
    {
        Multimap<Long, CounterEventData> multimap = ArrayListMultimap.create();
        
        DateTime dateTime = new DateTime(DateTimeZone.UTC);
        multimap.put(1L, prepareCounterEventData("member123", 1, Arrays.asList("pageView","trafficTablet","contribution"),dateTime));
        multimap.put(2L, prepareCounterEventData("member321", 1, Arrays.asList("pageView","trafficMobile"),dateTime));       
        multimap.put(3L, prepareCounterEventData("member123", 1, Arrays.asList("pageView","trafficTablet"),dateTime.plusHours(1)));
        
        counterStorage.insertDailyMetrics(multimap);
        
        List<Long> subscriptionIds = counterStorage.getSubscritionIdsFromDailyMetrics();
        
        Assert.assertNotNull(subscriptionIds);
        Assert.assertFalse(subscriptionIds.isEmpty());
        Assert.assertTrue(subscriptionIds.size() == 3);
    }

}
