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
import com.ning.metrics.collector.processing.counter.CounterDistribution;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RolledUpCounterData
{
    private final String counterName;
    private int totalCount = 0;
    private int uniqueCount = 0;
    private final CounterDistribution distribution;

    @JsonCreator
    public RolledUpCounterData(
            @JsonProperty("counterName") String counterName,
            @JsonProperty("totalCount") int totalCount,
            @JsonProperty("distribution") Map<String, Integer> distMap) {

        if (distMap != null) {
            if (distMap instanceof CounterDistribution) {
                this.distribution = (CounterDistribution)distMap;
            }
            else {
                this.distribution = new CounterDistribution();
                for (Map.Entry<String, Integer> e : distMap.entrySet()) {
                    this.distribution.putPresortedEntry(
                            e.getKey(), e.getValue());
                }
            }
        }
        else {
            distribution = new CounterDistribution();
        }

        this.counterName = counterName;
        this.totalCount = totalCount;
        this.uniqueCount = distribution.size();
    }


    public String getCounterName()
    {
        return counterName;
    }

    public int getTotalCount()
    {
        return totalCount;
    }

    /**
     * Get the distribution as an immutable map of uniqueId to count.
     *
     * @return
     */
    public CounterDistribution getDistribution() {
        return distribution;
    }

    public int getUniqueCount()
    {
        return this.uniqueCount;
    }

    /**
     * remove all but the top N distribution entries from this representation
     * of the counter
     * @param distributionLimit
     */
    @JsonIgnore
    public void setDistributionSerializationLimit(int distributionLimit) {
        distribution.setSerializationLimit(distributionLimit);
    }

    /**
     * increment the total count for this counter
     * @param incrementValue
     */
    @JsonIgnore
    public void incrementCounter(Integer incrementValue) {
        totalCount += incrementValue;
    }

    /**
     * increment the counter for the given unique id in the distribution by the
     * given amount
     * @param uniqueIdentifier
     * @param increment
     */
    @JsonIgnore
    public void incrementDistributionCounter(String uniqueIdentifier,
            int increment) {
        Integer prevCount = distribution.increment(uniqueIdentifier, increment);

        if (prevCount == null) {
            uniqueCount++;
        }
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
