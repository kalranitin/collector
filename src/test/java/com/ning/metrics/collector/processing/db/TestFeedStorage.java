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

import com.ning.metrics.collector.processing.db.model.Feed;
import com.ning.metrics.collector.processing.db.model.FeedEvent;
import com.ning.metrics.collector.processing.db.model.FeedEventData;
import com.ning.metrics.collector.processing.db.model.FeedEventMetaData;
import com.ning.metrics.collector.processing.db.model.Subscription;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

@Test(groups = {"slow", "database"})
public class TestFeedStorage
{
    private CollectorMysqlTestingHelper helper;
    private static final ObjectMapper mapper = new ObjectMapper();
    
    @Inject
    FeedStorage feedStorage;
    
    final String topic = "topic";
    final String channel = "channel";
    final String feed = "feed";
    final String eventData = "{"
            + "\"content-id\": \"123:Meal:456\","
            + "\"content-type\": \"Meal\","
            + "\"topics\": [\""+topic+"\"]"                
     + "}";
    
    @BeforeClass(groups = {"slow", "database"})
    public void startDB() throws Exception{
        helper = new CollectorMysqlTestingHelper();
        helper.startMysql();
        helper.initDb();
        
        System.setProperty("collector.spoolWriter.jdbc.url", helper.getJdbcUrl());
        System.setProperty("collector.spoolWriter.jdbc.user", CollectorMysqlTestingHelper.USERNAME);
        System.setProperty("collector.spoolWriter.jdbc.password", CollectorMysqlTestingHelper.PASSWORD);
        
        Guice.createInjector(new DBConfigModule()).injectMembers(this);
                
    }
    
    @BeforeMethod(groups = {"slow", "database"})
    public void clearDB(){
        helper.clear();
    }
    
    @AfterClass(groups = {"slow", "database"})
    public void stopDB() throws Exception{
        helper.stopMysql();
    }
    
    @Test
    public void testFeedDBOperations() throws Exception{
        Feed feeds = new Feed(Arrays.asList(getFeedEvent(getSubscription(1L, topic, channel, feed), eventData)));
        feedStorage.addOrUpdateFeed(feed, feeds);
        
        feeds = feedStorage.loadFeedByKey(feed);
        
        Assert.assertNotNull(feeds);
        Assert.assertEquals(feeds.getFeedEvents().size(), 1);
        Assert.assertEquals(feeds.getFeedEvents().iterator().next().getChannel(), channel);
        
        feedStorage.deleteFeed(feed);
        feeds = feedStorage.loadFeedByKey(feed);
        Assert.assertNull(feeds);
    }
    
    @Test
    public void testMaxFeedSize() throws Exception{
        Feed feeds = new Feed(Arrays.asList(getFeedEvent(getSubscription(1L, topic, channel, feed), eventData)));
        feeds.deleteFeedEvent("123:Meal:456");
        
        Assert.assertEquals(feeds.getFeedEvents().size(), 0);
        
        for(int i=1;i<=5;i++){
            feeds.addFeedEvents(Arrays.asList(getFeedEvent(getSubscription((long)i, topic, channel, feed), eventData)), 3);            
        }
        
        Assert.assertEquals(feeds.getFeedEvents().size(), 3);
        Assert.assertEquals((long)feeds.getFeedEvents().iterator().next().getSubscriptionId(),3);
        
    }
    
    private Subscription getSubscription(Long id, String topic, String channel, String feed){
        FeedEventMetaData metadata = new FeedEventMetaData(feed);
        Subscription subscription = new Subscription(id,topic, metadata, channel);
        return subscription;
    }
    
    private FeedEvent getFeedEvent(Subscription subscription, String eventData) throws JsonParseException, JsonMappingException, IOException{        
        return new FeedEvent(mapper.readValue(eventData, FeedEventData.class), 
            subscription.getChannel(), 
            subscription.getId(), 
            subscription.getMetadata());
    }

}
