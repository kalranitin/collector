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
import com.google.common.base.Objects;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CounterEventData
{
    private final String uniqueIdentifier;
    private final int identifierCategory;
    private DateTime createdDate;
    private final Map<String, Integer> counters = new ConcurrentHashMap<String, Integer>();
    
    @JsonCreator
    public CounterEventData(@JsonProperty("uniqueIdentifier") String uniqueIdentifier, 
        @JsonProperty("identifierCategory") int identifierCategory, 
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

    public String getUniqueIdentifier()
    {
        return uniqueIdentifier;
    }

    public int getIdentifierCategory()
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
    
    
    
    

}
