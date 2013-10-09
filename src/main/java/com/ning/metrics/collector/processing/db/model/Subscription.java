package com.ning.metrics.collector.processing.db.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Subscription
{
    private final String target;
    private final String channel;
    private final EventMetaData metadata;
    private final Long id;

    public Subscription(String target, EventMetaData metadata, String channel)
    {
        this(null, target, metadata, channel);
    }

    @JsonCreator
    public Subscription(@JsonProperty("id") @Nullable Long id,
                        @JsonProperty("target") String target,
                        @JsonProperty("metadata") EventMetaData metadata,
                        @JsonProperty("channel") String channel)
    {
        this.id = id;
        this.target = target.replaceAll("\\s+", " ");
        this.channel = channel;
        this.metadata = metadata;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Long getId()
    {
        return id;
    }

    public String getChannel()
    {
        return channel;
    }

    public EventMetaData getMetadata() {
        return metadata;
    }

    public String getTarget()
    {
        return target;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((metadata == null) ? 0 : metadata.hashCode());
        result = prime * result + ((target == null) ? 0 : target.hashCode());
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
        Subscription other = (Subscription) obj;
        if (channel == null) {
            if (other.channel != null)
                return false;
        }
        else if (!channel.equals(other.channel))
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
        if (target == null) {
            if (other.target != null)
                return false;
        }
        else if (!target.equals(other.target))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "Subscription [target=" + target + ", channel=" + channel + ", metadata=" + metadata + ", id=" + id + "]";
    }    
}
