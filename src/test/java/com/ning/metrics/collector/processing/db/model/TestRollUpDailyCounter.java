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
package com.ning.metrics.collector.processing.db.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.inject.Inject;
import com.ning.metrics.collector.guice.module.CollectorObjectMapperModule;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Test(groups = "fast")
@Guice(modules = CollectorObjectMapperModule.class)
public class TestRollUpDailyCounter
{
    @Inject
    private ObjectMapper mapper;
    
    
    @Test
    public void testRollUp() throws Exception{
        List<CounterEventData> dailyCounterList = new ArrayList<CounterEventData>();
        dailyCounterList.add(prepareCounterEventData("member123", 1, Arrays.asList("pageView","trafficMobile","contribution")));
        
        dailyCounterList.add(prepareCounterEventData("member123", 1, Arrays.asList("pageView","trafficTablet","contribution")));
        dailyCounterList.add(prepareCounterEventData("member321", 1, Arrays.asList("pageView","trafficMobile")));
        
        RolledUpCounter rolledUpCounter = new RolledUpCounter("app123", new DateTime(), new DateTime());
        
        Assert.assertTrue(rolledUpCounter.getCounterSummary().isEmpty());
        
        for(CounterEventData counterEventData : dailyCounterList)
        {
            List<String> identifierDistribution = getIdentifierDistribution(counterEventData.getIdentifierCategory());
            
            rolledUpCounter.updateRolledUpCounterData(counterEventData, identifierDistribution);
        }
        
        rolledUpCounter.evaluateUniques();
        
        Assert.assertTrue(rolledUpCounter.getCounterSummary().containsRow(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1"));
        Assert.assertEquals(new Integer(3), rolledUpCounter.getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "pageView").getTotalCount());
        Assert.assertFalse(rolledUpCounter.getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "pageView").getDistribution().isEmpty());
        Assert.assertTrue(rolledUpCounter.getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "trafficMobile").getDistribution().isEmpty());
        Assert.assertEquals(new Integer(2), rolledUpCounter.getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", RolledUpCounter.UNIQUES_KEY).getTotalCount());
        
        Set<String> aggregatedCounterNames = new HashSet<String>(Arrays.asList("pageView"));
        
        rolledUpCounter.aggregateCounterDataFor(aggregatedCounterNames, true);
        
        Assert.assertNotNull(rolledUpCounter.getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "pageView"));
        Assert.assertNotNull(rolledUpCounter.getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1",RolledUpCounter.UNIQUES_KEY));
        Assert.assertNull(rolledUpCounter.getCounterSummary().get(RolledUpCounter.COUNTER_SUMMARY_PREFIX+"1", "trafficMobile"));
        
        
    }
    
    @Test
    public void testGroupDailyCounterList() throws Exception{
        List<CounterEventData> dailyCounterList = new ArrayList<CounterEventData>();
        
        dailyCounterList.add(prepareCounterEventData("member123", 1, Arrays.asList("pageView","trafficMobile")));
        dailyCounterList.add(prepareCounterEventData("member123", 1, Arrays.asList("pageView","trafficTablet","contribution")));
            
        dailyCounterList.add(prepareCounterEventData("member321", 1, Arrays.asList("pageView","trafficMobile")));
        dailyCounterList.add(prepareCounterEventData("member321", 1, Arrays.asList("pageView","trafficMobile")));     
        
        Map<String,CounterEventData> groupMap = new ConcurrentHashMap<String, CounterEventData>();
        
        for(CounterEventData counterEventData : dailyCounterList){
            CounterEventData groupedData = groupMap.get(counterEventData.getUniqueIdentifier()+counterEventData.getFormattedDate());
            if(Objects.equal(null, groupedData))
            {
                groupMap.put(counterEventData.getUniqueIdentifier()+counterEventData.getFormattedDate(), counterEventData);
                continue;
            }
            
            groupedData.mergeCounters(counterEventData.getCounters());
            groupMap.put(counterEventData.getUniqueIdentifier()+counterEventData.getFormattedDate(), groupedData);            
        }
        
        Assert.assertEquals(groupMap.values().size(), 2);
        Assert.assertEquals(groupMap.values().iterator().next().getCounters().get("pageView"), new Integer(2));
        
    }
    
    
    private static CounterEventData prepareCounterEventData(String id, int category, List<String> counters){
        Map<String,Integer> counterMap = new HashMap<String, Integer>();
        for(String s : counters)
        {
            counterMap.put(s, 1);
        }
        
        return new CounterEventData(id, category, new DateTime(), counterMap);
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

}
