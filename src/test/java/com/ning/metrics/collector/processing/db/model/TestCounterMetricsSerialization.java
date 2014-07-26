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
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.ning.metrics.collector.guice.module.CollectorObjectMapperModule;
import com.ning.metrics.collector.processing.counter.CounterDistribution;
import java.util.Map;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = "fast")
@Guice(modules = CollectorObjectMapperModule.class)
public class TestCounterMetricsSerialization
{
    @Inject
    private ObjectMapper mapper;


    @Test
    public void testCounterEventDeserialization() throws Exception{
        String jsonData = "{\"namespace\": \"network_id:111\","
                + "\"buckets\":["
                + "{\"uniqueIdentifier\": \"member:123\","
                + "\"createdDate\":\"2013-01-10\","
                + "\"counters\":"
                + "{\"pageView\":1,\"trafficDesktop\":0,\"trafficMobile\":0,\"trafficTablet\":1,\"trafficSearchEngine\":0,\"memberJoined\":1,\"memberLeft\":0,\"contribution\":1,\"contentViewed\":0,\"contentLike\":0,\"contentComment\":0}},"
                + "{\"uniqueIdentifier\": \"content:222\","
                + "\"createdDate\":\"2013-01-10\","
                + "\"counters\":{\"pageView\":0,\"trafficDesktop\":0,\"trafficMobile\":0,\"trafficTablet\":0,\"trafficSearchEngine\":0,\"memberJoined\":0,\"memberLeft\":0,\"contribution\":0,\"contentViewed\":1,\"contentLike\":5,\"contentComment\":10}}]}";

        CounterEvent counterEvent = mapper.readValue(jsonData,CounterEvent.class);

        Assert.assertNotNull(counterEvent);
        Assert.assertTrue(counterEvent.getCounterEvents().size() == 2);
        Assert.assertEquals("network_id:111", counterEvent.getNamespace());
        Assert.assertEquals("member:123", counterEvent.getCounterEvents().get(0).getUniqueIdentifier());
        Assert.assertTrue(counterEvent.getCounterEvents().get(0).getCounters().containsKey("pageView"));

//        System.out.println(mapper.writeValueAsString(counterEvent));
    }

    @Test
    public void testRolledUpCounterSerialization() throws Exception
    {
        Map<String, RolledUpCounterData> counterSummary = Maps.newHashMap();
        CounterDistribution distributionMap = new CounterDistribution();
        distributionMap.increment("member1", 2);
        distributionMap.increment("member2", 3);
        distributionMap.increment("member3", 1);

        RolledUpCounterData counterData1 = new RolledUpCounterData(
                "pageView", 6, distributionMap);
        counterSummary.put("pageView", counterData1);

        RolledUpCounter rolledUpCounter = new RolledUpCounter("app123", new DateTime(), new DateTime(), counterSummary);

        Assert.assertNotNull(mapper.writeValueAsString(rolledUpCounter));
    }

    @Test
    public void testRolledUpCounterDeserialization() throws Exception{
        String rollUpJson =
                "{"
                + "\"namespace\":\"app123\","
                + "\"fromDate\":\"2014-02-01\","
                + "\"toDate\":\"2014-02-01\","
                + "\"counterSummary\":{"
                    + "\"pageView\":{"
                        + "\"counterName\":\"pageView\","
                        + "\"totalCount\":6,"
                        + "\"distribution\":{"
                            + "\"member2\":3,"
                            + "\"member3\":1,"
                            + "\"member1\":2"
                + "}}}}";

        RolledUpCounter rolledUpCounter = mapper.readValue(
                rollUpJson, RolledUpCounter.class);

        Assert.assertNotNull(rolledUpCounter);
        Assert.assertEquals("app123", rolledUpCounter.getNamespace());
        Assert.assertTrue(rolledUpCounter.getCounterSummary().containsKey("pageView"));
        Assert.assertEquals(6, rolledUpCounter.getCounterSummary()
                .get("pageView").getTotalCount());
        Assert.assertFalse(rolledUpCounter.getCounterSummary()
                .get("pageView").getDistribution().isEmpty());
        Assert.assertEquals(3, rolledUpCounter.getCounterSummary()
                .get("pageView").getUniqueCount());

    }

}
