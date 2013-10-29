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
import java.util.Map;

public class TestFeedRollUpProcessor
{
    private static final ObjectMapper mapper = new ObjectMapper();
    
    @Test
    public void testFeedApplyRollUp() throws Exception{
        
        DateTime dt = new DateTime(DateTimeZone.UTC);

        Subscription subscription = getSubscription(1L, "topic", "channel", "feed");
        Feed feed = new Feed(Arrays.asList(getFeedEvent(subscription, "1",dt,"member","join-group")));
        
        feed.addFeedEvents(Arrays.asList(
                                            getFeedEvent(subscription, "2",dt.plusHours(1), "member1","join-group"),
                                            getFeedEvent(subscription, "3",dt.plusHours(2), "member","join-group"),
                                            getFeedEvent(subscription, "4",dt.plusHours(25), "member","join-group")
                                        ) , 
                                        100);
        
        FeedRollUpProcessor feedRollUpProcessor = new FeedRollUpProcessor();
        
        Feed newFeed = feedRollUpProcessor.applyRollUp(feed, new HashMap<String, Object>(){{put("visibility","member");}});
        
        Assert.assertNotNull(newFeed);
        Assert.assertEquals(newFeed.getFeedEvents().size(), 2);
        Assert.assertEquals(newFeed.getFeedEvents().iterator().next().getClass(), RolledUpFeedEvent.class);
        Assert.assertEquals(((RolledUpFeedEvent)newFeed.getFeedEvents().iterator().next()).getCount(), 2);
    }
    
    private Subscription getSubscription(Long id, String topic, String channel, String feed){
        FeedEventMetaData metadata = new FeedEventMetaData(feed);
        Subscription subscription = new Subscription(id,topic, metadata, channel);
        return subscription;
    }
    
    private FeedEvent getFeedEvent(Subscription subscription, String contentId, DateTime date, String visibility, String eventType) throws JsonParseException, JsonMappingException, IOException{  
        String eventData = "{"
                + "\"content-id\": \""+contentId+"\","
                + "\"content-type\": \"Meal\","
                + "\"visibility\": \""+visibility+"\","
                + "\"created-date\": \""+date+"\","
                + "\"event-type\": \""+eventType+"\","
                + "\"topics\": [\"topic\"]"                
         + "}";
        
        return new FeedEvent(mapper.readValue(eventData, FeedEventData.class), 
            subscription.getChannel(), 
            subscription.getId(), 
            subscription.getMetadata());
    }

}
