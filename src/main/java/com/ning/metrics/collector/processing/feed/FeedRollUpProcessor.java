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
import com.ning.metrics.collector.processing.db.model.RolledUpFeedEvent;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class FeedRollUpProcessor
{
    private static final Logger log = LoggerFactory.getLogger(FeedRollUpProcessor.class);
    
    
    public Feed applyRollUp(final Feed feed, final Map<String,Object> filterMap){
        final List<FeedEvent> compiledFeedEventList = Lists.newArrayList();
        
        // filter out all events which do not match "any" of the provided key value pair
        List<FeedEvent> feedEventList = Lists.newArrayList(Iterables.filter(feed.getFeedEvents(), FeedEvent.isAnyKeyValuMatching(filterMap)));
        
        FeedEventComparator feedEventComparator = new FeedEventComparator();
        Collections.sort(feedEventList,feedEventComparator);
        
        ArrayListMultimap<String, FeedEvent> arrayListMultimap = ArrayListMultimap.create();
        ListIterator<FeedEvent> iterator = feedEventList.listIterator();
        
        while(iterator.hasNext()){
            FeedEvent feedEvent = iterator.next();
            
            if(feedEvent.getEvent() != null && RolledUpEventTypes.asSet.contains(feedEvent.getEvent().getEventType())){
                
                List<FeedEvent> feedEventListByType = arrayListMultimap.get(feedEvent.getEvent().getEventType());
                
                if(feedEventListByType == null){
                    feedEventListByType = Lists.newArrayList();
                }
                
                if(!feedEventListByType.isEmpty()){
                    
                    FeedEvent compareFeedEvent = feedEventListByType.get(0);
                    
                    if(feedEvent.getEvent().getCreatedDate().plusHours(24).isAfter(compareFeedEvent.getEvent().getCreatedDate()))
                    {
                        // event been iterated upon is a candidate for roll up
                        arrayListMultimap.put(feedEvent.getEvent().getEventType(), feedEvent);
                        
                     // Remove the event from the list as it has to be grouped based on the type as it is grouped and we do now want duplicates
                        iterator.remove();
                        
                    }
                }
                else
                {
                    arrayListMultimap.put(feedEvent.getEvent().getEventType(), feedEvent);
                    
                 // Remove the event from the list as it has to be grouped based on the type as it is grouped and we do now want duplicates
                    iterator.remove();
                }
            }
        }
        
        for(String eventType : arrayListMultimap.keySet())
        {
            compiledFeedEventList.add(new RolledUpFeedEvent(eventType, arrayListMultimap.get(eventType)));
        }
        
        // add rest of the events
        compiledFeedEventList.addAll(feedEventList);
        
        return new Feed(compiledFeedEventList);
    }
    
    private static final class FeedEventComparator implements Comparator<FeedEvent>{
        
        @Override
        public int compare(FeedEvent feedEvent1, FeedEvent feedEvent2)
        {
            // Sort feed events by descending date
            return feedEvent2.getEvent().getCreatedDate().compareTo(feedEvent1.getEvent().getCreatedDate());
        }
        
    }

}
