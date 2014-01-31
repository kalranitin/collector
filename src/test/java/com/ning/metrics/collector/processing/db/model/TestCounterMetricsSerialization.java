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
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Test(groups = "fast")
public class TestCounterMetricsSerialization
{
    private static final ObjectMapper mapper = new ObjectMapper();
    
    @BeforeClass
    public void setUpObjectMapper() throws Exception{
        mapper.registerModule(new JodaModule());
        mapper.registerModule(new GuavaModule());
    }
    
    @Test
    public void testCounterSubscriptionDeserialization() throws Exception{
        String jsonData = "{\"appId\":\"network_111\","
                + "\"identifierDistribution\":"
                + "{\"1\":[\"pageView\",\"memberJoined\"],\"2\":[\"contentViewed\",\"contentLike\"]}"
                + "}";
        
        CounterSubscription counterSubscription = mapper.readValue(jsonData, CounterSubscription.class);
        
        Assert.assertNotNull(counterSubscription);
        Assert.assertEquals("network_111", counterSubscription.getAppId());
        Assert.assertTrue(counterSubscription.getIdentifierDistribution().keySet().size() == 2);
        Assert.assertTrue(counterSubscription.getIdentifierDistribution().get(1).contains("pageView"));
    }
    
    @Test
    public void testCounterEventDeserialization() throws Exception{
        String jsonData = "{\"appId\": \"network_id:111\","
                + "\"buckets\":["
                + "{\"uniqueIdentifier\": \"member:123\","
                + "\"identifierCategory\": \"1\","
                + "\"createdDate\":\"2013-01-10\","
                + "\"counters\":"
                + "{\"pageView\":1,\"trafficDesktop\":0,\"trafficMobile\":0,\"trafficTablet\":1,\"trafficSearchEngine\":0,\"memberJoined\":1,\"memberLeft\":0,\"contribution\":1,\"contentViewed\":0,\"contentLike\":0,\"contentComment\":0}},"
                + "{\"uniqueIdentifier\": \"content:222\","
                + "\"identifierCategory\": \"2\","
                + "\"createdDate\":\"2013-01-10\","
                + "\"counters\":{\"pageView\":0,\"trafficDesktop\":0,\"trafficMobile\":0,\"trafficTablet\":0,\"trafficSearchEngine\":0,\"memberJoined\":0,\"memberLeft\":0,\"contribution\":0,\"contentViewed\":1,\"contentLike\":5,\"contentComment\":10}}]}";
            
        CounterEvent counterEvent = mapper.readValue(jsonData,CounterEvent.class);
        
        Assert.assertNotNull(counterEvent);
        Assert.assertTrue(counterEvent.getCounterEvents().size() == 2);
        Assert.assertEquals("network_id:111", counterEvent.getAppId());
        Assert.assertEquals("member:123", counterEvent.getCounterEvents().get(0).getUniqueIdentifier());
        Assert.assertTrue(counterEvent.getCounterEvents().get(0).getCounters().containsKey("pageView"));
    }
    
    @Test
    public void testRolledUpCounterSerialization() throws Exception
    {
        Table<String, String, RolledUpCounterData> counterSummary = HashBasedTable.create();
        Map<String,Integer> distributionMap = new HashMap<String,Integer>();
        distributionMap.put("member1", 2);
        distributionMap.put("member2", 3);
        distributionMap.put("member3", 1);
        
        RolledUpCounterData counterData1 = new RolledUpCounterData("pageView", 6, distributionMap);
        counterSummary.put("counterSummary_0", "pageView", counterData1);
        
        Assert.assertEquals(new Integer(3), counterData1.getUniqueCount());
        
        RolledUpCounter rolledUpCounter = new RolledUpCounter("app123", new DateTime(), new DateTime(), counterSummary);
        
        Assert.assertNotNull(mapper.writeValueAsString(rolledUpCounter));
    }
    
    @Test
    public void testRolledUpCounterDeserialization() throws Exception{
        String rollUpJson = "{\"appId\":\"app123\",\"fromDate\":1389829178063,\"toDate\":1389829178070,\"counterSummary_1\":{\"pageView\":{\"counterName\":\"pageView\",\"totalCount\":6,\"distribution\":{\"member2\":3,\"member3\":1,\"member1\":2}}}}";
        
        RolledUpCounter rolledUpCounter = mapper.readValue(rollUpJson, RolledUpCounter.class);
        
        Assert.assertNotNull(rolledUpCounter);
        Assert.assertEquals("app123", rolledUpCounter.getAppId());
        Assert.assertTrue(rolledUpCounter.getCounterSummary().containsRow("counterSummary_1"));
        Assert.assertTrue(rolledUpCounter.getCounterSummary().contains("counterSummary_1", "pageView"));
        Assert.assertEquals(new Integer(6), rolledUpCounter.getCounterSummary().get("counterSummary_1", "pageView").getTotalCount());
        Assert.assertFalse(rolledUpCounter.getCounterSummary().get("counterSummary_1", "pageView").getDistribution().isEmpty());
        Assert.assertEquals(new Integer(3), rolledUpCounter.getCounterSummary().get("counterSummary_1", "pageView").getUniqueCount());
        
    }

}
