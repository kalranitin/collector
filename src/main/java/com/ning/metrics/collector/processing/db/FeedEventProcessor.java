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
import com.ning.metrics.collector.processing.db.model.FeedEvent;
import com.ning.metrics.collector.processing.db.model.Feed;

import com.google.common.base.Objects;
import com.google.common.collect.ArrayListMultimap;
import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FeedEventProcessor
{
    private static final Logger log = LoggerFactory.getLogger(FeedEventProcessor.class);
    private final DatabaseFeedStorage databaseFeedStorage;
    private final DatabaseFeedEventStorage databaseFeedEventStorage;
    private final CollectorConfig config;
    
    @Inject
    public FeedEventProcessor(final DatabaseFeedStorage databaseFeedStorage, final DatabaseFeedEventStorage databaseFeedEventStorage, final CollectorConfig config){
        this.databaseFeedEventStorage = databaseFeedEventStorage;
        this.databaseFeedStorage = databaseFeedStorage;
        this.config = config;
    }
    
    public void process(List<FeedEvent> feedEvents){
        
        ArrayListMultimap<String, FeedEvent> multimap = sortEventsByFeedKey(feedEvents);            
        for(String feedKey : multimap.keySet())
        {
            Feed feed = databaseFeedStorage.loadFeedByKey(feedKey);
            if(feed == null)
            {
                feed = new Feed(multimap.get(feedKey));
            }
            else
            {
                // TODO make the max feed count configurable
                feed.addFeedEvents(multimap.get(feedKey), 1000);
            }
            
            databaseFeedStorage.addOrUpdateFeed(feedKey, feed);
        }
    }
    
    private ArrayListMultimap<String, FeedEvent> sortEventsByFeedKey(List<FeedEvent> feedEvents)
    {
        ArrayListMultimap<String, FeedEvent> multimap = ArrayListMultimap.create();
        for(FeedEvent feedEvent : feedEvents)
        {
            final String feedKey = feedEvent.getMetadata().getFeed();
            if(Objects.equal(null, feedKey))
            {
                continue;
            }
            
            multimap.put(feedKey, feedEvent);
            
        }
        
        return multimap;
    }
}
