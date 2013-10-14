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

import com.ning.metrics.collector.processing.db.model.EventMetaData;
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
        Subscription subscription = getSubscription("target","channel","feed");
        Long id = subscriptionStorage.insert(subscription);
        Assert.assertNotNull(id);
        
        Subscription loadSubscription = subscriptionStorage.loadSubscriptionById(id);
        Assert.assertNotNull(loadSubscription);
        
        Assert.assertEquals(loadSubscription.getChannel(), subscription.getChannel());
        Assert.assertEquals(loadSubscription.getTarget(), subscription.getTarget());
        Assert.assertEquals(loadSubscription.getMetadata(), subscription.getMetadata());
        
    }
    
    @Test
    public void testLoadSubscriptionByTarget() throws Exception{
        subscriptionStorage.insert(getSubscription("target","channel","feed"));
        subscriptionStorage.insert(getSubscription("target","channel","feed1"));
        
        Set<Subscription> subscriptionSet = subscriptionStorage.load("target");
        
        Assert.assertNotNull(subscriptionSet);
        Assert.assertFalse(subscriptionSet.isEmpty());
        Assert.assertTrue(subscriptionSet.size() == 2);
        
    }
    
    @Test
    public void testLoadSubscriptionForMultipleTargets() throws Exception{
        subscriptionStorage.insert(getSubscription("content-created network:bedazzlenw","channel-activity","feed"));
        subscriptionStorage.insert(getSubscription("content-created network:bedazzlenw tag:breakfast","channel-activity","feed1"));
        
        Set<Subscription> subscriptionSet = subscriptionStorage.load("content-created network:bedazzlenw tag:breakfast");
        
        Assert.assertNotNull(subscriptionSet);
        Assert.assertFalse(subscriptionSet.isEmpty());
        Assert.assertTrue(subscriptionSet.size() == 2);
    }
    
    private Subscription getSubscription(String target, String channel, String feed){
        EventMetaData metadata = new EventMetaData(feed);
        Subscription subscription = new Subscription(target, metadata, channel);
        return subscription;
    }
    
    @AfterClass(alwaysRun = true,groups = {"slow", "database"})
    public void stopDB() throws Exception{
        helper.stopMysql();
    }
}
