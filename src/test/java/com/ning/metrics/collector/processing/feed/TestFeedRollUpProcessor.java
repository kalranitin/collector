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
package com.ning.metrics.collector.processing.feed;

import com.ning.metrics.collector.processing.db.model.Feed;
import com.ning.metrics.collector.processing.db.model.FeedEvent;
import com.ning.metrics.collector.processing.db.model.FeedEventData;
import com.ning.metrics.collector.processing.db.model.FeedEventMetaData;
import com.ning.metrics.collector.processing.db.model.RolledUpFeedEvent;
import com.ning.metrics.collector.processing.db.model.Subscription;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class TestFeedRollUpProcessor
{
    private static final ObjectMapper mapper = new ObjectMapper();
    
    @Test
    public void testFeedApplyRollUp() throws Exception{
        
        DateTime dt = new DateTime(DateTimeZone.UTC);

        Subscription subscription = getSubscription(1L, "topic", "channel", "feed");
        Feed feed = new Feed(Arrays.asList(getFeedEvent(subscription, "1",dt,"member",RolledUpEventTypes.JOIN_GROUP.itemFieldName,"\"event1\"",RolledUpEventTypes.JOIN_GROUP.itemFieldName)));
        
        feed.addFeedEvents(Arrays.asList(
                                            getFeedEvent(subscription, "2",dt.plusHours(1), "member1",RolledUpEventTypes.JOIN_GROUP.itemFieldName,"\"event2\"",RolledUpEventTypes.JOIN_GROUP.itemFieldName),
                                            getFeedEvent(subscription, "3",dt.plusHours(2), "member",RolledUpEventTypes.JOIN_GROUP.itemFieldName,"\"event3\"",RolledUpEventTypes.JOIN_GROUP.itemFieldName),
                                            getFeedEvent(subscription, "4",dt.plusHours(25), "member",RolledUpEventTypes.JOIN_GROUP.itemFieldName,"\"event4\"",RolledUpEventTypes.JOIN_GROUP.itemFieldName)
                                        ) , 
                                        100);
        
        FeedRollUpProcessor feedRollUpProcessor = new FeedRollUpProcessor();
        
        Feed newFeed = feedRollUpProcessor.applyRollUp(feed, new HashMap<String, Object>(){{put("visibility","member");}});
        
        Assert.assertNotNull(newFeed);
        Assert.assertEquals(newFeed.getFeedEvents().size(), 2);
        Assert.assertEquals(newFeed.getFeedEvents().iterator().next().getClass(), RolledUpFeedEvent.class);
        Assert.assertEquals(((RolledUpFeedEvent)newFeed.getFeedEvents().iterator().next()).getCount(), 2);
    }
    
    
    @Test
    public void testFeedApplyRollUpWithSuppress() throws Exception{
        
        DateTime dt = new DateTime(DateTimeZone.UTC);

        Subscription subscription = getSubscription(1L, "topic", "channel", "feed");
        Feed feed = new Feed(Arrays.asList(getFeedEvent(subscription, "1",dt,"member",RolledUpEventTypes.JOIN_GROUP.itemFieldName,"\"mainEvent\"",RolledUpEventTypes.JOIN_GROUP.itemFieldName)));
        
        feed.addFeedEvents(Arrays.asList(
                                            getFeedEvent(subscription, "2",dt.plusHours(1), "member1","","\"event2\"",""),
                                            getFeedEvent(subscription, "3",dt.plusHours(2), "member",RolledUpEventTypes.JOIN_GROUP.itemFieldName,"\"mainEvent\",\"event3\"",RolledUpEventTypes.JOIN_GROUP.itemFieldName),
                                            getFeedEvent(subscription, "4",dt.plusHours(3), "member",RolledUpEventTypes.JOIN_GROUP.itemFieldName,"\"mainEvent\",\"event4\"",RolledUpEventTypes.JOIN_GROUP.itemFieldName),
                                            getFeedEvent(subscription, "5",dt.plusHours(4), "",FeedEventData.EVENT_TYPE_SUPPRESS,"\"mainEvent\"","")
                                        ) , 
                                        100);
        
        FeedRollUpProcessor feedRollUpProcessor = new FeedRollUpProcessor();
        
        Feed newFeed = feedRollUpProcessor.applyRollUp(feed, null);
        
        Assert.assertNotNull(newFeed);
        Assert.assertEquals(newFeed.getFeedEvents().size(), 1);
        Assert.assertEquals(newFeed.getFeedEvents().iterator().next().getEvent().getFeedEventId(), "2");
    }
    
    private Subscription getSubscription(Long id, String topic, String channel, String feed){
        FeedEventMetaData metadata = new FeedEventMetaData(feed);
        Subscription subscription = new Subscription(id,topic, metadata, channel);
        return subscription;
    }
    
    private FeedEvent getFeedEvent(Subscription subscription, String contentId, DateTime date, String visibility, String eventType, String removalTarget, String rollupKey) throws JsonParseException, JsonMappingException, IOException{  
        String eventData = "{"
                + "\""+FeedEventData.FEED_EVENT_ID_KEY+"\": \""+contentId+"\","
                + "\"content-type\": \"Meal\","
                + "\"visibility\": \""+visibility+"\","
                + "\""+FeedEventData.CREATED_DATE_KEY+"\": \""+date+"\","
                + "\""+FeedEventData.EVENT_TYPE_KEY+"\": \""+eventType+"\","
                + "\""+FeedEventData.REMOVAL_TARGETS+"\": ["+removalTarget+"],"
                + "\""+FeedEventData.TOPICS_KEY+"\": [\"topic\"],"
                + "\""+FeedEventData.ROLLUP_KEY+"\": \""+rollupKey+"\""
         + "}";
        
        return new FeedEvent(mapper.readValue(eventData, FeedEventData.class), 
            subscription.getChannel(), 
            subscription.getId(), 
            subscription.getMetadata());
    }

}
