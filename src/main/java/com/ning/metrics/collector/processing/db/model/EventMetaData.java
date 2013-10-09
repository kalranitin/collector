package com.ning.metrics.collector.processing.db.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EventMetaData {
	private final String feed;

    @JsonCreator
    public EventMetaData(@JsonProperty("feed") String feed) {
    	this.feed = feed;
    }

    public String getFeed() {
        return feed;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((feed == null) ? 0 : feed.hashCode());
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
        EventMetaData other = (EventMetaData) obj;
        if (feed == null) {
            if (other.feed != null)
                return false;
        }
        else if (!feed.equals(other.feed))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "MetaData [feed=" + feed + "]";
    }
    
}
