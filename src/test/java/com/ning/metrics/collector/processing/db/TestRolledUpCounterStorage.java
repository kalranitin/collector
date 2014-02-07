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
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
                        + "\"appId\":\"network_111\","
                        + "\"fromDate\":\""+RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.print(fromDate)+"\","
                        + "\"toDate\":\""+RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.print(toDate)+"\","
                        + "\"counterSummary_1\":"
                        + "{"
                            + "\"contribution\":{\"counterName\":\"contribution\",\"totalCount\":2,\"distribution\":{\"member123\":2}},"
                            + "\"pageView\":{\"counterName\":\"pageView\",\"totalCount\":3,\"distribution\":{\"member321\":1,\"member123\":2}},"
                            + "\"trafficTablet\":{\"counterName\":\"trafficTablet\",\"totalCount\":1,\"distribution\":{}},"
                            + "\"trafficMobile\":{\"counterName\":\"trafficMobile\",\"totalCount\":2,\"distribution\":{}},"
                            + "\"uniques\":{\"counterName\":\"uniques\",\"totalCount\":2}"
                        + "}"
                    + "}";
        
        return mapper.readValue(json, RolledUpCounter.class);
    }
    
    private static CounterEventData prepareCounterEventData(String id, int category, List<String> counters){
        Map<String,Integer> counterMap = new HashMap<String, Integer>();
        for(String s : counters)
        {
            counterMap.put(s, 1);
        }
        
        return new CounterEventData(id, category, new DateTime(DateTimeZone.UTC), counterMap);
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
    
    @Test
    public void testInsertRolledUpCounter() throws Exception{
        DateTime dateTime = new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis("2014-01-24"),DateTimeZone.UTC);
        
        RolledUpCounter rolledUpCounter = prepareRolledUpCounterData(dateTime, dateTime);
        String id = counterStorage.insertOrUpdateRolledUpCounter(1L, rolledUpCounter);
        
        Assert.assertNotNull(id);
        Assert.assertEquals(id, "network_1112014-01-24");
    }
    
    @Test
    public void testLoadAndUpdateRolledUpCounter() throws Exception{
        DateTime dateTime = new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis("2014-01-24"),DateTimeZone.UTC);
        
        RolledUpCounter rolledUpCounter = prepareRolledUpCounterData(dateTime, dateTime);
        String id = counterStorage.insertOrUpdateRolledUpCounter(1L, rolledUpCounter);
        
        rolledUpCounter = counterStorage.loadRolledUpCounterById(id, false);
        
        Assert.assertNotNull(rolledUpCounter);
        Assert.assertEquals(rolledUpCounter.getAppId(), "network_111");
        
        List<String> identifierDistribution = getIdentifierDistribution(1);
        
        rolledUpCounter.updateRolledUpCounterData(prepareCounterEventData("member321", 1, Arrays.asList("pageView","trafficMobile")),identifierDistribution);
        rolledUpCounter.updateRolledUpCounterData(prepareCounterEventData("member111", 1, Arrays.asList("pageView","trafficMobile")),identifierDistribution);
        rolledUpCounter.evaluateUniques();
        
        Assert.assertEquals(new Integer(5), rolledUpCounter.getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "pageView").getTotalCount());
        Assert.assertEquals(new Integer(3), rolledUpCounter.getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", RolledUpCounter.UNIQUES_KEY).getTotalCount());
        
        id = counterStorage.insertOrUpdateRolledUpCounter(1L, rolledUpCounter);
        
        Assert.assertNotNull(id);
        Assert.assertEquals(id, "network_1112014-01-24");
    }
    
    @Test
    public void testLoadRolledUpCountersByDateRange() throws Exception{
        DateTime date_22 = new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis("2014-01-22"),DateTimeZone.UTC);
        DateTime date_23 = new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis("2014-01-23"),DateTimeZone.UTC);
        DateTime date_24 = new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis("2014-01-24"),DateTimeZone.UTC);
        
        RolledUpCounter rolledUpCounter_22 = prepareRolledUpCounterData(date_22, date_22);
        RolledUpCounter rolledUpCounter_23 = prepareRolledUpCounterData(date_23, date_23);
        RolledUpCounter rolledUpCounter_24 = prepareRolledUpCounterData(date_24, date_24);
        
        counterStorage.insertOrUpdateRolledUpCounter(1L, rolledUpCounter_22);
        counterStorage.insertOrUpdateRolledUpCounter(1L, rolledUpCounter_23);
        counterStorage.insertOrUpdateRolledUpCounter(1L, rolledUpCounter_24);
        
        List<RolledUpCounter> rolledUpCounters = counterStorage.loadRolledUpCounters(1L, date_22, date_24, null, false);
        
        Assert.assertNotNull(rolledUpCounters);
        Assert.assertTrue(rolledUpCounters.size() == 3);  
    }
    
    @Test
    public void testLoadRolledUpCountersByStartDate() throws Exception{
        DateTime dateTime = new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis("2014-01-24"),DateTimeZone.UTC);
        
        RolledUpCounter rolledUpCounter = prepareRolledUpCounterData(dateTime, dateTime);
        counterStorage.insertOrUpdateRolledUpCounter(1L, rolledUpCounter);
        
        List<RolledUpCounter> rolledUpCounters = counterStorage.loadRolledUpCounters(1L, dateTime, null, null, false);
        
        Assert.assertNotNull(rolledUpCounters);
        Assert.assertFalse(rolledUpCounters.size() == 0);
        Assert.assertEquals(rolledUpCounters.get(0).getAppId(),"network_111");
        
    }
    
    @Test
    public void testLoadRolledUpCountersByEndDate() throws Exception{
        DateTime dateTime = new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis("2014-01-24"),DateTimeZone.UTC);
        
        RolledUpCounter rolledUpCounter = prepareRolledUpCounterData(dateTime, dateTime);
        counterStorage.insertOrUpdateRolledUpCounter(1L, rolledUpCounter);
        
        List<RolledUpCounter> rolledUpCounters = counterStorage.loadRolledUpCounters(1L, null, dateTime, null, false);
        
        Assert.assertNotNull(rolledUpCounters);
        Assert.assertFalse(rolledUpCounters.size() == 0);
        Assert.assertEquals(rolledUpCounters.get(0).getAppId(),"network_111");
    }
    
    @Test
    public void testLoadRolledUpCounterForCounterNames() throws Exception{
        DateTime dateTime = new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis("2014-01-24"),DateTimeZone.UTC);
        
        RolledUpCounter rolledUpCounter = prepareRolledUpCounterData(dateTime, dateTime);
        counterStorage.insertOrUpdateRolledUpCounter(1L, rolledUpCounter);
        Set<String> counterNameSet = new HashSet<String>();
        counterNameSet.add("pageView");
        Optional<Set<String>> optional = Optional.of(counterNameSet);
        
        List<RolledUpCounter> rolledUpCounters = counterStorage.loadRolledUpCounters(1L, null, null, optional, true);
        
        Assert.assertNotNull(rolledUpCounters);
        Assert.assertFalse(rolledUpCounters.size() == 0);
        Assert.assertEquals(rolledUpCounters.get(0).getAppId(),"network_111");
        
        Assert.assertNotNull(rolledUpCounters.get(0).getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "pageView"));
        Assert.assertNotNull(rolledUpCounters.get(0).getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1",RolledUpCounter.UNIQUES_KEY));
        Assert.assertNull(rolledUpCounters.get(0).getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "trafficMobile"));
    }

}
