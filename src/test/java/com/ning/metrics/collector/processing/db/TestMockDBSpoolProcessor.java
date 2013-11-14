/*
 * Copyright 2010-2013 Ning, Inc.
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

import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.SerializationType;
import com.ning.metrics.collector.processing.db.model.FeedEvent;
import com.ning.metrics.collector.processing.db.model.FeedEventData;
import com.ning.metrics.collector.processing.db.model.FeedEventMetaData;
import com.ning.metrics.collector.processing.db.model.Subscription;
import com.ning.metrics.serialization.event.Event;
import com.ning.metrics.serialization.event.EventDeserializer;

import com.mchange.v2.io.FileUtils;

import org.mockito.Mockito;
import org.quartz.Scheduler;
import org.skife.config.TimeSpan;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestMockDBSpoolProcessor
{
    private DBSpoolProcessor dbSpoolProcessor;
    private Event event;
    private EventDeserializer eventDeserializer;
    private SerializationType serializationType;
    private SubscriptionStorage subscriptionStorage;
    private CollectorConfig config;
    private FeedEventStorage feedEventStorage;
    private File file;
    private Scheduler quartzScheduler;
    
    
    @BeforeMethod
    public void setup() throws Exception{
        event = Mockito.mock(Event.class);
        eventDeserializer = Mockito.mock(EventDeserializer.class);
        serializationType = Mockito.mock(SerializationType.class);
        config = Mockito.mock(CollectorConfig.class);
        subscriptionStorage = Mockito.mock(SubscriptionStorage.class);
        feedEventStorage = Mockito.mock(FeedEventStorage.class);
        quartzScheduler = Mockito.mock(Scheduler.class);
        file = new File(System.getProperty("java.io.tmpdir")+"/dbtest.json");
        FileUtils.touch(file);
        //Mockito.when(file.getPath()).thenReturn(System.getProperty("java.io.tmpdir"));
        
        Mockito.when(config.getSpoolWriterExecutorShutdownTime()).thenReturn(new TimeSpan("1s"));
        Mockito.when(quartzScheduler.isStarted()).thenReturn(true);
        
        dbSpoolProcessor = new DBSpoolProcessor(null, config, subscriptionStorage, feedEventStorage,quartzScheduler);
    }
    
    @AfterMethod
    public void cleanUp() throws Exception{
        org.apache.commons.io.FileUtils.deleteQuietly(file);
    }
    
    @Test
    public void testFilterOutNonFeedEvents() throws Exception{
        
        Mockito.when(serializationType.getDeSerializer(Mockito.<InputStream>any())).thenReturn(eventDeserializer);
        Mockito.when(eventDeserializer.hasNextEvent()).thenReturn(true,false);
        Mockito.when(eventDeserializer.getNextEvent()).thenReturn(event);
        Mockito.when(event.getName()).thenReturn("nonFeedEvent");
        dbSpoolProcessor.processEventFile(null, serializationType, file, null);
        
        Mockito.verify(serializationType,Mockito.times(1)).getDeSerializer(Mockito.<InputStream>any());
        Mockito.verify(eventDeserializer, Mockito.times(2)).hasNextEvent();
        Mockito.verify(eventDeserializer, Mockito.times(1)).getNextEvent();
        Mockito.verify(event, Mockito.times(2)).getName();
        Mockito.verifyNoMoreInteractions(eventDeserializer,serializationType);
        Mockito.verifyZeroInteractions(subscriptionStorage,feedEventStorage);
    }
    
    
    private void processFeedEvents(boolean testThread, boolean isSuppressType) throws Exception{
        final String topic = "topic";
        final String channel = "channel";
        final String feed = "feed";
        
        String eventData = "{"
                + "\""+FeedEventData.FEED_EVENT_ID_KEY+"\": \"123:Meal:456\","
                + "\"content-type\": \"Meal\","
                + (isSuppressType?"\""+FeedEventData.EVENT_TYPE_KEY+"\": \""+FeedEventData.EVENT_TYPE_SUPPRESS+"\",":"")
                + "\""+FeedEventData.TOPICS_KEY+"\": [\""+topic+"\"]"                
         + "}";
        
        Mockito.when(serializationType.getDeSerializer(Mockito.<InputStream>any())).thenReturn(eventDeserializer);
        Mockito.when(eventDeserializer.hasNextEvent()).thenReturn(true,false);
        Mockito.when(eventDeserializer.getNextEvent()).thenReturn(event);
        Mockito.when(event.getName()).thenReturn("FeedEvent");
        
        Mockito.when(event.getData()).thenReturn(eventData);
        
        Set<Subscription> subscriptionSet = new HashSet<Subscription>(Arrays.asList(getSubscription(1L,topic, channel, feed)));
        if(isSuppressType)
        {
            Mockito.when(subscriptionStorage.loadByStartsWithTopic(Mockito.anyString())).thenReturn(subscriptionSet);
        }
        else
        {
            Mockito.when(subscriptionStorage.loadByTopic(Mockito.anyString())).thenReturn(subscriptionSet);
        }
        
        
        dbSpoolProcessor.processEventFile(null, serializationType, file, null);
        // Sleeping so that the insertion call is done successfully.
        if(testThread){
            Thread.sleep(5000);
        }
        
        dbSpoolProcessor.close();
        
        
        Mockito.verify(serializationType,Mockito.times(1)).getDeSerializer(Mockito.<InputStream>any());
        Mockito.verify(eventDeserializer, Mockito.times(2)).hasNextEvent();
        Mockito.verify(eventDeserializer, Mockito.times(1)).getNextEvent();
        Mockito.verify(event, Mockito.times(3)).getName();
        if(isSuppressType)
        {
            Mockito.verify(subscriptionStorage,Mockito.times(1)).loadByStartsWithTopic(Mockito.anyString());
            Mockito.verify(subscriptionStorage,Mockito.times(0)).loadByTopic(Mockito.anyString());
        }
        else
        {
            Mockito.verify(subscriptionStorage,Mockito.times(1)).loadByTopic(Mockito.anyString());
            Mockito.verify(subscriptionStorage,Mockito.times(0)).loadByStartsWithTopic(Mockito.anyString());
        }
        
        Mockito.verify(feedEventStorage,Mockito.times(1)).insert(Mockito.<FeedEvent>anyCollectionOf(FeedEvent.class));
        Mockito.verifyNoMoreInteractions(eventDeserializer,serializationType);
    }
    
    @Test
    public void testProcessFeedEvents() throws Exception{
        processFeedEvents(false,false);
    }
    
    @Test
    public void testProcessSuppressTypeFeedEvents() throws Exception{
        processFeedEvents(false,true);
    }
    
    @Test
    public void testProcessFeedEventsByThread() throws Exception{
        processFeedEvents(true,false);
    }
    
    private Subscription getSubscription(Long id, String topic, String channel, String feed){
        FeedEventMetaData metadata = new FeedEventMetaData(feed);
        Subscription subscription = new Subscription(id,topic, metadata, channel);
        return subscription;
    }
    
    
}
