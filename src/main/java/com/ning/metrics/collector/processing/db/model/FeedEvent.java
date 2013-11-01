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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FeedEvent
{
    private final String channel;
    private final FeedEventMetaData metadata;
    private final FeedEventData event;
    private final Long subscriptionId;
    private final String id;
    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonCreator
    public FeedEvent(@JsonProperty("event") FeedEventData event,
                        @JsonProperty("channel") String channel,
                        @JsonProperty("subscriptionId") Long subscriptionId,
                        @JsonProperty("metadata") FeedEventMetaData metadata)
    {
        this.channel = channel;
        this.event = event;
        this.subscriptionId = subscriptionId;
        this.metadata = metadata;
        this.id = "";
    }
    
    public FeedEvent(String id, String channel, String metadata, String event, long subscriptionId) throws IOException{
        this.subscriptionId = subscriptionId;
        this.event = mapper.readValue(event, FeedEventData.class);
        this.metadata = mapper.readValue(metadata, FeedEventMetaData.class);
        this.channel = channel;
        this.id = id;
    }
    
    public String getChannel()
    {
        return channel;
    }

    public FeedEventData getEvent()
    {
        return event;
    }

    public Long getSubscriptionId()
    {
        return subscriptionId;
    }

    public FeedEventMetaData getMetadata() {
        return metadata;
    }
    
    @JsonIgnore
    public String getId()
    {
        return id;
    }
    
    @JsonIgnore
    public static Predicate<FeedEvent> isAnyKeyValuMatching(final Map<String,Object> filterMap){
        Predicate<FeedEvent> feedEventPredicate = new Predicate<FeedEvent>() {

            @Override
            public boolean apply(FeedEvent input)
            {
                if(filterMap == null || filterMap.isEmpty())
                {
                    return true;
                }
                
                return !Maps.difference(filterMap, input.getEvent().getData()).entriesInCommon().isEmpty();
            }};
            
            return feedEventPredicate;
    }
    
    @JsonIgnore
    public static Predicate<FeedEvent> findFeedEventsByType(final String eventType){
        Predicate<FeedEvent> feedEventPredicate = new Predicate<FeedEvent>() {

            @Override
            public boolean apply(FeedEvent input)
            {
               return Objects.equal(eventType, input.getEvent().getEventType());
            }
            
        };
        
        return feedEventPredicate;
    }
    
    @JsonIgnore
    public static Predicate<FeedEvent> findByFeedEventId(final String feedEventId){
        Predicate<FeedEvent> feedEventPredicate = new Predicate<FeedEvent>() {

            @Override
            public boolean apply(FeedEvent input)
            {
                return Objects.equal(feedEventId, input.getEvent().getFeedEventId());
            }};
            
            return feedEventPredicate;
        
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        result = prime * result + ((event == null) ? 0 : event.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((metadata == null) ? 0 : metadata.hashCode());
        result = prime * result + ((subscriptionId == null) ? 0 : subscriptionId.hashCode());
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
        FeedEvent other = (FeedEvent) obj;
        if (channel == null) {
            if (other.channel != null)
                return false;
        }
        else if (!channel.equals(other.channel))
            return false;
        if (event == null) {
            if (other.event != null)
                return false;
        }
        else if (!event.equals(other.event))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        }
        else if (!id.equals(other.id))
            return false;
        if (metadata == null) {
            if (other.metadata != null)
                return false;
        }
        else if (!metadata.equals(other.metadata))
            return false;
        if (subscriptionId == null) {
            if (other.subscriptionId != null)
                return false;
        }
        else if (!subscriptionId.equals(other.subscriptionId))
            return false;
        return true;
    }

    
}
