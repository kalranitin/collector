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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RolledUpCounterData
{
    private final String counterName;
    private Integer totalCount;
    private Integer uniqueCount = 0;
    private final Map<String, Integer> distribution;
    
    @JsonCreator
    public RolledUpCounterData(@JsonProperty("counterName") final String counterName, 
        @JsonProperty("totalCount") final Integer totalCount, 
        @JsonProperty("distribution") final Map<String,Integer> distribution){
        this.counterName = counterName;
        this.totalCount = totalCount;
        if(distribution == null)
        {
            this.distribution = new ConcurrentHashMap<String, Integer>();
        }
        else
        {
            this.distribution = distribution;
        }
        
        this.uniqueCount = this.distribution.keySet().size();
    }

    public String getCounterName()
    {
        return counterName;
    }

    public Integer getTotalCount()
    {
        return totalCount;
    }

    public Map<String, Integer> getDistribution()
    {
        return distribution;
    }
    
    public Integer getUniqueCount()
    {
        return this.uniqueCount;
    }
    
    @JsonIgnore
    public void incrementCounter(Integer incrementValue)
    {
        totalCount += incrementValue;
    }
    
    @JsonIgnore
    public void incrementDistributionCounter(final String uniqueIdentifier, final Integer incrementValue)
    {
        Integer distributionCount = distribution.get(uniqueIdentifier);
        if(Objects.equal(null, distributionCount))
        {
            distributionCount = new Integer(0);
        }
        
        distributionCount += incrementValue;
            
        distribution.put(uniqueIdentifier, distributionCount);
        this.uniqueCount = distribution.keySet().size();
    }
    
    @JsonIgnore
    public void truncateDistribution()
    {
        if(!Objects.equal(null, distribution))
        {
            distribution.clear();
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((counterName == null) ? 0 : counterName.hashCode());
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
        RolledUpCounterData other = (RolledUpCounterData) obj;
        if (counterName == null) {
            if (other.counterName != null)
                return false;
        }
        else if (!counterName.equals(other.counterName))
            return false;
        return true;
    }
    
}
