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

import com.ning.metrics.collector.processing.db.model.FeedEventMetaData;
import com.ning.metrics.collector.processing.db.model.Subscription;

import com.google.inject.Guice;
import com.google.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;

@Test(groups = {"slow", "database"})
public class TestSubscriptionStorageImpl
{
    private CollectorMysqlTestingHelper helper;
    
    @Inject
    SubscriptionStorage subscriptionStorage;
    
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
    
    @BeforeMethod(alwaysRun = true, groups = {"slow", "database"})
    public void clearDB(){
        helper.clear();
    }
    
    @Test
    public void testCreateSubscription() throws Exception{
        Subscription subscription = getSubscription("topic","channel","feed");
        Long id = subscriptionStorage.insert(subscription);
        Assert.assertNotNull(id);
        
        Subscription loadSubscription = subscriptionStorage.loadSubscriptionById(id);
        Assert.assertNotNull(loadSubscription);
        
        Assert.assertEquals(loadSubscription.getChannel(), subscription.getChannel());
        Assert.assertEquals(loadSubscription.getTopic(), subscription.getTopic());
        Assert.assertEquals(loadSubscription.getMetadata(), subscription.getMetadata());
        
    }
    
    @Test
    public void testLoadSubscriptionByTopic() throws Exception{
        subscriptionStorage.insert(getSubscription("topic","channel","feed"));
        subscriptionStorage.insert(getSubscription("topic","channel","feed1"));
        
        Set<Subscription> subscriptionSet = subscriptionStorage.loadByTopic("topic");
        
        Assert.assertNotNull(subscriptionSet);
        Assert.assertFalse(subscriptionSet.isEmpty());
        Assert.assertTrue(subscriptionSet.size() == 2);
        
    }
    
    @Test
    public void testLoadSubscriptionByTopicStartsWith() throws Exception{
        subscriptionStorage.insert(getSubscription("topic","channel","feed"));
        subscriptionStorage.insert(getSubscription("topic1","channel","feed1"));
        
        Set<Subscription> subscriptionSet = subscriptionStorage.loadByStartsWithTopic("topic");
        
        Assert.assertNotNull(subscriptionSet);
        Assert.assertFalse(subscriptionSet.isEmpty());
        Assert.assertTrue(subscriptionSet.size() == 2);
        
    }
    
    @Test
    public void testLoadSubscriptionByFeed() throws Exception{
        subscriptionStorage.insert(getSubscription("topic","channel","feed"));
        subscriptionStorage.insert(getSubscription("topic1","channel","feed"));
        
        Set<Subscription> subscriptionSet = subscriptionStorage.loadByFeed("feed");
        
        Assert.assertNotNull(subscriptionSet);
        Assert.assertFalse(subscriptionSet.isEmpty());
        Assert.assertTrue(subscriptionSet.size() == 2);
        
    }
    
    @Test
    public void testLoadSubscriptionForMultipleTopics() throws Exception{
        subscriptionStorage.insert(getSubscription("content-created network:bedazzlenw","channel-activity","feed"));
        subscriptionStorage.insert(getSubscription("content-created network:bedazzlenw tag:breakfast","channel-activity","feed1"));
        
        Set<Subscription> subscriptionSet = subscriptionStorage.loadByTopic("content-created network:bedazzlenw tag:breakfast");
        
        Assert.assertNotNull(subscriptionSet);
        Assert.assertFalse(subscriptionSet.isEmpty());
        Assert.assertTrue(subscriptionSet.size() == 2);
    }
    
    @Test
    public void testDeleteSubscription(){
        Long id = subscriptionStorage.insert(getSubscription("topic-1","channel-activity","feed-1"));
        Assert.assertNotNull(id);
        boolean deleted = subscriptionStorage.deleteSubscriptionById(id);
        Assert.assertTrue(deleted);
        Assert.assertNull(subscriptionStorage.loadSubscriptionById(id));
    }
    
    @Test
    public void testDeleteSubscriptionAfterLoadingToFeedCache()
    {
        final String feed = "feed-1";
        Long id = subscriptionStorage.insert(getSubscription("topic-1","channel-activity",feed));
        Assert.assertNotNull(id);
        Set<Subscription> subscriptionSet = subscriptionStorage.loadByFeed(feed);
        
        Assert.assertNotNull(subscriptionSet);
        Assert.assertNotEquals(subscriptionSet.size(), 0);
        
        boolean deleted = subscriptionStorage.deleteSubscriptionById(id);
        Assert.assertTrue(deleted);
        Assert.assertNull(subscriptionStorage.loadSubscriptionById(id));
        Assert.assertTrue(subscriptionStorage.loadByFeed(feed).isEmpty());
    }
    
    @Test
    public void testDeleteSubscriptionAfterLoadingToTopicCache()
    {
        final String topic = "feed-1";
        Long id = subscriptionStorage.insert(getSubscription(topic,"channel-activity","feed-1"));
        Assert.assertNotNull(id);
        Set<Subscription> subscriptionSet = subscriptionStorage.loadByTopic(topic);
        
        Assert.assertNotNull(subscriptionSet);
        Assert.assertNotEquals(subscriptionSet.size(), 0);
        
        boolean deleted = subscriptionStorage.deleteSubscriptionById(id);
        Assert.assertTrue(deleted);
        Assert.assertNull(subscriptionStorage.loadSubscriptionById(id));
        Assert.assertTrue(subscriptionStorage.loadByTopic(topic).isEmpty());
    }
    
    @Test(enabled = false)
    public void testDeleteSubscriptionAfterLoadingMultiplesToTopicCache()
    {
        final String topic = "topic-1";
        final String queryTopics = topic + " topic-2";
        Long id = subscriptionStorage.insert(getSubscription(topic,
                "channel-activity","feed-1"));
        Assert.assertNotNull(id);
        Set<Subscription> subscriptionSet = 
                subscriptionStorage.loadByTopic(queryTopics);
        
        Assert.assertNotNull(subscriptionSet);
        Assert.assertNotEquals(subscriptionSet.size(), 0);
        
        boolean deleted = subscriptionStorage.deleteSubscriptionById(id);
        Assert.assertTrue(deleted);
        Assert.assertNull(subscriptionStorage.loadSubscriptionById(id));
        
        Assert.assertTrue(
                subscriptionStorage.loadByTopic(queryTopics).isEmpty());
    }
    
    private Subscription getSubscription(String topic, String channel, String feed){
        FeedEventMetaData metadata = new FeedEventMetaData(feed);
        Subscription subscription = new Subscription(topic, metadata, channel);
        return subscription;
    }
    
    @AfterClass(alwaysRun = true,groups = {"slow", "database"})
    public void stopDB() throws Exception{
        helper.stopMysql();
    }
}
