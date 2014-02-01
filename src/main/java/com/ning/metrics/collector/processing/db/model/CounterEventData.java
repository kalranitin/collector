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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CounterEventData
{
    private final String uniqueIdentifier;
    private final Integer identifierCategory;
    private DateTime createdDate;
    private final Map<String, Integer> counters = new ConcurrentHashMap<String, Integer>();
    
    @JsonCreator
    public CounterEventData(@JsonProperty("uniqueIdentifier") String uniqueIdentifier, 
        @JsonProperty("identifierCategory") Integer identifierCategory, 
        @JsonProperty("createdDate") DateTime createdDate,
        @JsonProperty("counters") Map<String, Integer> counters)
    {
        this.uniqueIdentifier = uniqueIdentifier;
        this.identifierCategory = identifierCategory;
        this.counters.putAll(counters);
        if(createdDate != null)
        {
            this.createdDate = createdDate;
        }
        else
        {
            this.createdDate = new DateTime(this.createdDate,DateTimeZone.UTC);
        }
    }
   
    public DateTime getCreatedDate(){
        if(this.createdDate != null)
        {
            try {
                return new DateTime(this.createdDate,DateTimeZone.UTC);
            }
            catch (Exception e) {
                return new DateTime(DateTimeZone.UTC);
            }
        }
        
        return new DateTime(DateTimeZone.UTC);
    }
    
    @JsonIgnore
    public String getFormattedDate()
    {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        return formatter.print(getCreatedDate());
    }
    
    @JsonIgnore
    public void mergeCounters(Map<String, Integer> mergeFrom)
    {
        for(Entry<String, Integer> mapEntry : mergeFrom.entrySet())
        {
            incrementCounter(mapEntry.getKey(), mapEntry.getValue());
        }
    }

    public String getUniqueIdentifier()
    {
        return uniqueIdentifier;
    }

    public Integer getIdentifierCategory()
    {
        return identifierCategory;
    }

    public Map<String, Integer> getCounters()
    {
        return counters;
    }
    
    @JsonIgnore
    public void incrementCounter(String counterName, Integer count)
    {
        Integer counterValue = getCounters().get(counterName);
        if(Objects.equal(null, counterValue))
        {
            counterValue = new Integer(0);
        }
        
        counterValue += count;
        counters.put(counterName, counterValue);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((createdDate == null) ? 0 : createdDate.hashCode());
        result = prime * result + identifierCategory;
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
        if (createdDate == null) {
            if (other.createdDate != null)
                return false;
        }
        else if (!createdDate.equals(other.createdDate))
            return false;
        if (identifierCategory != other.identifierCategory)
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
