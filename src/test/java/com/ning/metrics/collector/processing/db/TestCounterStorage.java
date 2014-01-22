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
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {"slow", "database"})
public class TestCounterStorage
{
    private CollectorMysqlTestingHelper helper;
    private static final ObjectMapper mapper = new ObjectMapper();
    
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
        
        Guice.createInjector(new DBConfigModule()).injectMembers(this);
        
        mapper.registerModule(new JodaModule());
        mapper.registerModule(new GuavaModule());
                
    }
    
    @BeforeMethod(alwaysRun = true, groups = {"slow", "database"})
    public void clearDB(){
        helper.clear();
    }
    
    @AfterClass(alwaysRun = true,groups = {"slow", "database"})
    public void stopDB() throws Exception{
        helper.stopMysql();
    }
    
    @Test
    public void testCreateCounterSubscription() throws Exception
    {
        String jsonData = "{\"appId\":\"network_111\","
                + "\"identifierDistribution\":"
                + "{\"1\":[\"pageView\",\"memberJoined\"],\"2\":[\"contentViewed\",\"contentLike\"]}"
                + "}";
        
        CounterSubscription counterSubscription = mapper.readValue(jsonData, CounterSubscription.class);
        
        Long id = counterStorage.createCounterSubscription(counterSubscription);
        
        Assert.assertNotNull(id);
        
        counterSubscription = counterStorage.loadCounterSubscription("network_111");
        
        Assert.assertNotNull(counterSubscription);
        Assert.assertEquals(counterSubscription.getId(), id);
        
    }

}
