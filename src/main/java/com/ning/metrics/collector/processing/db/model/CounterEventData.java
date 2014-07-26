/*
 * Copyright 2010-2014 Ning, Inc.
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
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Map.Entry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Storage and transport class for counter events
 * @author kguthrie
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CounterEventData
{
    private final String uniqueIdentifier;
    private final DateTime createdTime;
    private final Map<String, Integer> counters;
    public static final DateTimeFormatter DAILY_COUNTER_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);

    @JsonCreator
    public CounterEventData(@JsonProperty("uniqueIdentifier") String uniqueIdentifier,
        @JsonProperty("createdDate") DateTime createdTime,
        @JsonProperty("counters") Map<String, Integer> counters)
    {
        this.uniqueIdentifier = uniqueIdentifier;

        this.counters = Maps.newHashMap(counters);

        if(createdTime != null)
        {
            this.createdTime = createdTime;
        }
        else
        {
            this.createdTime = new DateTime(DateTimeZone.UTC);
        }
    }

    public DateTime getCreatedTime(){
        return createdTime;
    }

    @JsonIgnore
    public String getFormattedDate() {
        return DAILY_COUNTER_DATE_FORMATTER.print(getCreatedTime());
    }

    /**
     * Iterate through the given map of counters to counts and increment this
     * object's local counters by each one
     * @param mergeFrom
     */
    @JsonIgnore
    public void mergeCounters(Map<String, Integer> mergeFrom)
    {
        for(Entry<String, Integer> mapEntry : mergeFrom.entrySet())
        {
            incrementCounter(mapEntry.getKey(), mapEntry.getValue());
        }
    }

    public String getUniqueIdentifier() {
        return uniqueIdentifier;
    }

    public Map<String, Integer> getCounters()
    {
        return counters;
    }

    /**
     * This method increases the count for the given counter by the given
     * increment.  If the given counter is not present, it will be added with
     * its count set to the given increment
     * @param counterName
     * @param increment
     */
    @JsonIgnore
    public void incrementCounter(String counterName, Integer increment)
    {
        Integer counterValue = getCounters().get(counterName);
        if(Objects.equal(null, counterValue)) {
            counterValue = 0;
        }

        counterValue += increment;
        counters.put(counterName, counterValue);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((createdTime == null) ? 0 : createdTime.hashCode());
        result = prime * result + ((uniqueIdentifier == null) ? 0 : uniqueIdentifier.hashCode());
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
        CounterEventData other = (CounterEventData) obj;
        if (createdTime == null) {
            if (other.createdTime != null)
                return false;
        }
        else if (!createdTime.equals(other.createdTime))
            return false;
        if (uniqueIdentifier == null) {
            if (other.uniqueIdentifier != null)
                return false;
        }
        else if (!uniqueIdentifier.equals(other.uniqueIdentifier))
            return false;
        return true;
    }
}
