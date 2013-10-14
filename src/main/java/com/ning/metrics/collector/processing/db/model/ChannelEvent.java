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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ChannelEvent
{
    private final String channel;
    private final EventMetaData metadata;
    private final ChannelEventData event;
    private final Long subscriptionId;
    private final int offset;
    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonCreator
    public ChannelEvent(@JsonProperty("event") ChannelEventData event,
                        @JsonProperty("channel") String channel,
                        @JsonProperty("subscriptionId") Long subscriptionId,
                        @JsonProperty("metadata") EventMetaData metadata)
    {
        this.channel = channel;
        this.event = event;
        this.subscriptionId = subscriptionId;
        this.metadata = metadata;
        this.offset = -1;
    }
    
    public ChannelEvent(int offset, String channel, String metadata, String event, long subscriptionId) throws IOException{
        this.subscriptionId = subscriptionId;
        this.event = mapper.readValue(event, ChannelEventData.class);
        this.metadata = mapper.readValue(metadata, EventMetaData.class);
        this.channel = channel;
        this.offset = offset;
    }
    
    public String getChannel()
    {
        return channel;
    }

    public ChannelEventData getEvent()
    {
        return event;
    }

    public Long getSubscriptionId()
    {
        return subscriptionId;
    }

    public EventMetaData getMetadata() {
        return metadata;
    }
    
    @JsonIgnore
    public int getOffset()
    {
        return offset;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
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
        ChannelEvent other = (ChannelEvent) obj;
        if (channel == null) {
            if (other.channel != null)
                return false;
        }
        else if (!channel.equals(other.channel))
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
