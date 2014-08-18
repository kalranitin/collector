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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.ning.metrics.collector.processing.db.model.FeedEvent;
import com.ning.metrics.collector.processing.db.model.FeedEventData;
import com.ning.metrics.collector.processing.db.model.FeedEventMetaData;
import com.ning.metrics.collector.processing.db.model.Subscription;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {"slow", "database"})
public class TestFeedEventStorage
{
    private CollectorMysqlTestingHelper helper;

    @Inject
    SubscriptionStorage subscriptionStorage;

    @Inject
    FeedEventStorage feedEventStorage;

    private static final ObjectMapper mapper = new ObjectMapper();

    private Subscription subscription;
    final String topic = "topic";
    final String channel = "channel";
    final String feed = "feed";

    final String eventData = "{"
            + "\""+FeedEventData.FEED_EVENT_ID_KEY+"\": \"123:Meal:456\","
            + "\"content-type\": \"Meal\","
            + "\""+FeedEventData.TOPICS_KEY+"\": [\""+topic+"\"],"
            + "\"" + FeedEventData.ROLLUP_KEEP_LAST_KEY + "\":10"
     + "}";

    @BeforeClass(groups = {"slow", "database"})
    public void startDB() throws Exception{
        helper = new CollectorMysqlTestingHelper();
        helper.startMysql();
        helper.initDb();

        System.setProperty("collector.spoolWriter.jdbc.url", helper.getJdbcUrl());
        System.setProperty("collector.spoolWriter.jdbc.user", CollectorMysqlTestingHelper.USERNAME);
        System.setProperty("collector.spoolWriter.jdbc.password", CollectorMysqlTestingHelper.PASSWORD);
        System.setProperty("collector.spoolWriter.feedEvent.retention.period", "1s");

        Guice.createInjector(new DBConfigModule()).injectMembers(this);

    }

    @BeforeMethod(groups = {"slow", "database"})
    public void clearDB(){
        helper.clear();
        subscriptionStorage.insert(getSubscription(topic,channel,feed));

        Set<Subscription> subscriptions = subscriptionStorage.loadByTopic(topic);
        Assert.assertNotEquals(subscriptions.size(), 0);
        Assert.assertEquals(subscriptions.size(), 1);

        subscription = subscriptions.iterator().next();
        Assert.assertEquals(subscription.getTopic(), topic);

    }

    @AfterClass(groups = {"slow", "database"})
    public void stopDB() throws Exception{
        helper.stopMysql();
    }

    @Test
    public void testInsertFeedEvents() throws Exception{

        List<FeedEvent> feedEvents = Lists.newArrayList();

        for(int i=0;i<10;i++){
            feedEvents.add(getFeedEvent(subscription, eventData));
        }

        List<String> idList = feedEventStorage.insert(feedEvents);

        feedEvents.clear();
        Assert.assertTrue(feedEvents.isEmpty());

        feedEvents = feedEventStorage.load(channel, idList, 10);

        Assert.assertTrue(feedEvents.size() == 10);
        Assert.assertEquals(feedEvents.get(0).getChannel(), channel);
        Assert.assertEquals(feedEvents.get(0).getMetadata().getFeed(), feed);
        Assert.assertEquals(feedEvents.get(0).getSubscriptionId(),
                subscription.getId());
        Assert.assertEquals(feedEvents.get(0).getEvent().getRollupKeepLast()
                , 10);

    }

    @Test
    public void testFeedEventCleanup() throws Exception{

        List<String> idList = feedEventStorage.insert(Arrays.asList(getFeedEvent(subscription, eventData)));
        Thread.sleep(2000);
        feedEventStorage.cleanOldFeedEvents();
        List<FeedEvent> feedEvents = feedEventStorage.load(channel, idList, 10);

        Assert.assertEquals(feedEvents.size(), 0);
    }

    private Subscription getSubscription(String topic, String channel, String feed){
        FeedEventMetaData metadata = new FeedEventMetaData(feed);
        Subscription subscription = new Subscription(topic, metadata, channel);
        return subscription;
    }

    private FeedEvent getFeedEvent(Subscription subscription, String eventData) throws JsonParseException, JsonMappingException, IOException{
        return new FeedEvent(mapper.readValue(eventData, FeedEventData.class),
            subscription.getChannel(),
            subscription.getId(),
            subscription.getMetadata());
    }

}
