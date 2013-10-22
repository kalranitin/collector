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
package com.ning.metrics.collector.processing.db.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Feeds
{
    private List<FeedEvent> feedEvents;

    @JsonCreator
    public Feeds(@JsonProperty("feedEvents") List<FeedEvent> feedEvents)
    {
        this.feedEvents = ImmutableList.copyOf(feedEvents);
    }
    
    public Collection<FeedEvent> getFeedEvents(){
        return feedEvents;
    }
    
    @JsonIgnore
    public void addFeedEvents(final Collection<FeedEvent> feedEvents, final int maxFeedSize){
        if(this.feedEvents.isEmpty()){
            this.feedEvents = ImmutableList.<FeedEvent>copyOf(feedEvents);
        }
        else
        {
            List<FeedEvent> tmpList = Lists.newArrayList(this.feedEvents);
            tmpList.addAll(feedEvents);
            
            // Max Feed event which needs to be in place are based on maxFeedSize
            this.feedEvents = ImmutableList.copyOf(tmpList.subList(Math.max(0, tmpList.size() - maxFeedSize), tmpList.size()));
        }
    }
    
    @JsonIgnore
    public boolean deleteFeedEvent(final String feedEventId) {
        boolean deleted = false;
        List<FeedEvent> tmpList = Lists.newArrayList(this.feedEvents);
        deleted = Iterables.removeIf(tmpList, FeedEvent.findFeedEventById(feedEventId));  
        
        if(deleted)
        {
            this.feedEvents = ImmutableList.copyOf(tmpList);
        }
        
        return deleted;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((feedEvents == null) ? 0 : feedEvents.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Feeds other = (Feeds) obj;
        if (feedEvents == null) {
            if (other.feedEvents != null)
                return false;
        }
        else if (!feedEvents.equals(other.feedEvents))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "Feeds [feedEvents=" + feedEvents + "]";
    }
}
