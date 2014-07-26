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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = "fast")
@Guice(modules = CollectorObjectMapperModule.class)
public class TestRollUpDailyCounter
{
    @Inject
    private ObjectMapper mapper;


    @Test
    public void testRollUp() throws Exception{
        List<CounterEventData> dailyCounterList = new ArrayList<CounterEventData>();
        dailyCounterList.add(prepareCounterEventData("member123",
                Arrays.asList("pageView","trafficMobile","contribution")));

        dailyCounterList.add(prepareCounterEventData("member123",
                Arrays.asList("pageView","trafficTablet","contribution")));
        dailyCounterList.add(prepareCounterEventData("member321",
                Arrays.asList("pageView","trafficMobile")));

        RolledUpCounter rolledUpCounter = new RolledUpCounter("app123",
                new DateTime(), new DateTime());

        Assert.assertTrue(rolledUpCounter.getCounterSummary().isEmpty());

        for(CounterEventData counterEventData : dailyCounterList) {
            rolledUpCounter.updateRolledUpCounterData(counterEventData);
        }

        Assert.assertEquals(3, rolledUpCounter.getCounterSummary()
                .get("pageView").getTotalCount());
        Assert.assertFalse(rolledUpCounter.getCounterSummary()
                .get("pageView").getDistribution().isEmpty());
        Assert.assertEquals(2, rolledUpCounter.getCounterSummary()
                .get("trafficMobile").getDistribution().size());
    }

    @Test
    public void testGroupDailyCounterList() throws Exception{
        List<CounterEventData> dailyCounterList
                = new ArrayList<CounterEventData>();

        dailyCounterList.add(prepareCounterEventData(
                "member123", Arrays.asList("pageView","trafficMobile")));
        dailyCounterList.add(prepareCounterEventData("member123",
                Arrays.asList("pageView","trafficTablet","contribution")));

        dailyCounterList.add(prepareCounterEventData(
                "member321", Arrays.asList("pageView","trafficMobile")));
        dailyCounterList.add(prepareCounterEventData(
                "member321", Arrays.asList("pageView","trafficMobile")));

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


    @Test
    public void testRollUpWithFilteredDistribution() throws Exception{
        List<CounterEventData> dailyCounterList = new ArrayList<CounterEventData>();
        dailyCounterList.add(prepareCounterEventData("member111", Arrays.asList("pageView","trafficMobile","contribution")));
        dailyCounterList.add(prepareCounterEventData("member111", Arrays.asList("pageView","trafficMobile","contribution")));
        dailyCounterList.add(prepareCounterEventData("member111", Arrays.asList("pageView","trafficMobile","contribution")));
        dailyCounterList.add(prepareCounterEventData("member111", Arrays.asList("pageView","trafficMobile","contribution")));
        dailyCounterList.add(prepareCounterEventData("member111", Arrays.asList("pageView","trafficMobile","contribution")));

        dailyCounterList.add(prepareCounterEventData("member112", Arrays.asList("pageView","trafficMobile","contribution")));
        dailyCounterList.add(prepareCounterEventData("member112", Arrays.asList("pageView","trafficMobile","contribution")));

        dailyCounterList.add(prepareCounterEventData("member113", Arrays.asList("pageView","trafficMobile","contribution")));
        dailyCounterList.add(prepareCounterEventData("member113", Arrays.asList("pageView","trafficMobile","contribution")));
        dailyCounterList.add(prepareCounterEventData("member113", Arrays.asList("pageView","trafficMobile","contribution")));
        dailyCounterList.add(prepareCounterEventData("member113", Arrays.asList("pageView","trafficMobile","contribution")));

        RolledUpCounter rolledUpCounter = new RolledUpCounter("app123", new DateTime(), new DateTime());

        Assert.assertTrue(rolledUpCounter.getCounterSummary().isEmpty());

        for(CounterEventData counterEventData : dailyCounterList) {
            rolledUpCounter.updateRolledUpCounterData(counterEventData);
        }

        Assert.assertFalse(rolledUpCounter.getCounterSummary().get("pageView")
                .getDistribution().isEmpty());
        Assert.assertEquals(3, rolledUpCounter.getCounterSummary().get(
                "pageView").getUniqueCount());

    }
    private static CounterEventData prepareCounterEventData(String id,
            List<String> counters){
        Map<String,Integer> counterMap = new HashMap<String, Integer>();
        for(String s : counters)
        {
            counterMap.put(s, 1);
        }

        return new CounterEventData(id, new DateTime(), counterMap);
    }


}
