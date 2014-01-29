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
import com.mchange.v2.io.FileUtils;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.SerializationType;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;
import com.ning.metrics.serialization.event.Event;
import com.ning.metrics.serialization.event.EventDeserializer;

import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.InputStream;

public class TestMockCounterEventProcessor
{
    private Event event;
    private EventDeserializer eventDeserializer;
    private SerializationType serializationType;
    private CounterStorage counterStorage;
    private File file;
    private CounterEventCacheProcessor counterEventCacheProcessor;
    private CounterEventSpoolProcessor counterEventSpoolProcessor;
    
    @BeforeMethod
    public void setup() throws Exception
    {
        event = Mockito.mock(Event.class);
        eventDeserializer = Mockito.mock(EventDeserializer.class);
        serializationType = Mockito.mock(SerializationType.class);
        counterStorage = Mockito.mock(CounterStorage.class);
        counterEventCacheProcessor = Mockito.mock(CounterEventCacheProcessor.class);
        
        file = new File(System.getProperty("java.io.tmpdir")+"/feedEventTest.json");
        FileUtils.touch(file);
        
        counterEventSpoolProcessor = new CounterEventSpoolProcessor(null, counterStorage, null, counterEventCacheProcessor);
        
        Mockito.when(serializationType.getDeSerializer(Mockito.<InputStream>any())).thenReturn(eventDeserializer);
        Mockito.when(eventDeserializer.hasNextEvent()).thenReturn(true,false);
        Mockito.when(eventDeserializer.getNextEvent()).thenReturn(event);
        Mockito.when(event.getName()).thenReturn("CounterEvent");
        
    }
    
    @AfterMethod
    public void cleanUp() throws Exception{
        org.apache.commons.io.FileUtils.deleteQuietly(file);
    }
    
    @Test
    public void testExcludeEmptyCounterEvents() throws Exception{
        
        
        String jsonData = "{\"appId\": \"network_id:111\","
                + "\"buckets\":[]}";
        
        
        Mockito.when(event.getData()).thenReturn(jsonData);
        
        counterEventSpoolProcessor.processEventFile(null, serializationType, file, null);
        
        Mockito.verify(serializationType,Mockito.times(1)).getDeSerializer(Mockito.<InputStream>any());
        Mockito.verify(eventDeserializer, Mockito.times(2)).hasNextEvent();
        Mockito.verify(eventDeserializer, Mockito.times(1)).getNextEvent();
        Mockito.verify(event, Mockito.times(2)).getName();
    }
    
    @Test
    public void testExceludeInvalidAppId() throws Exception{
        
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
        
        Mockito.when(event.getData()).thenReturn(jsonData);
        
        Mockito.when(counterStorage.loadCounterSubscription(Mockito.anyString())).thenReturn(null);
        
        counterEventSpoolProcessor.processEventFile(null, serializationType, file, null);
        
        Mockito.verify(counterEventCacheProcessor,Mockito.times(0)).addCounterEventData(Mockito.anyLong(), Mockito.<CounterEventData>any());
        
    }
    
    @Test
    public void testAddCounterEvents() throws Exception{
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.registerModule(new GuavaModule());
        
        String subscriptionJsonData = "{\"appId\":\"network_111\","
                + "\"identifierDistribution\":"
                + "{\"1\":[\"pageView\",\"memberJoined\"],\"2\":[\"contentViewed\",\"contentLike\"]}"
                + "}";
        
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
        
        Mockito.when(event.getData()).thenReturn(jsonData);
        
        Mockito.when(counterStorage.loadCounterSubscription(Mockito.anyString())).thenReturn(mapper.readValue(subscriptionJsonData, CounterSubscription.class));
        
        counterEventSpoolProcessor.processEventFile(null, serializationType, file, null);
        
        Mockito.verify(counterEventCacheProcessor,Mockito.times(2)).addCounterEventData(Mockito.anyLong(), Mockito.<CounterEventData>any());
    }
    
    
    
    
    
    

}
