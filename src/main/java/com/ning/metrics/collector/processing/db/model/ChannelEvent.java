package com.ning.metrics.collector.processing.db.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class ChannelEvent
{
    private final String channel;
    private final Map<String, String> metadata;
    private final Event event;
    private final Long subscriptionId;

    @JsonCreator
    public ChannelEvent(@JsonProperty("metadata") Map<String, String> metadata,
                        @JsonProperty("event") Event event,
                        @JsonProperty("channel") String channel,
                        @JsonProperty("subscriptionId") Long subscriptionId)
    {
        this.channel = channel;
        this.metadata = metadata;
        this.event = event;
        this.subscriptionId = subscriptionId;
    }
    
    public String getChannel()
    {
        return channel;
    }

    public Event getEvent()
    {
        return event;
    }

    public Long getSubscriptionId()
    {
        return subscriptionId;
    }

    public Map<String, String> getMetadata()
    {
        return metadata;
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
