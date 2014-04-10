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
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RolledUpCounterData
{
    private final String counterName;
    private Integer totalCount;
    private Integer uniqueCount = 0;
    private final Map<String, Integer> distribution;
    private boolean applySortOnDistribution = false;
    private int distributionLimit = 0;

    private boolean applyDistrubtionFilterOnId = false;
    private Set<String> uniqueIdsForFilter;

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

    /**
     * Get the distribution as an immutable map of uniqueId to count.  This
     * method will perform some basic computations on the distribution based on
     * local configurations for sorting, limiting, and filtering.  The rules for
     * these alterations is if a filter is configured it takes priority and the
     * count filter and sorting are not performed.
     *
     * @return
     */
    public Map<String, Integer> getDistribution()
    {
        // initialize the result to the normal distribution.  If there are no
        // modifications to be made later, this will be the result anyway
        Map<String, Integer> result = distribution;

        // If the distribution is empty, there's nothing to do
        if (result.isEmpty()) {
            return result;
        }

        // Build a result that filters out all those uniqueIds that do not
        // appear in the filter set
        if (applyDistrubtionFilterOnId) {

            final ImmutableMap.Builder<String, Integer> builder
                    = ImmutableMap.builder();

            for (Entry<String, Integer> entry : distribution.entrySet()) {

                if (uniqueIdsForFilter.contains(entry.getKey())) {
                    builder.put(entry.getKey(), entry.getValue());
                }
            }

            result = builder.build();
        }
        // sort and only return the top distributionLimit of the distribution,
        // but only if there is no explicit set of uniqueIds to filter on
        else if(applySortOnDistribution)
        {
            final Ordering<Map.Entry<String, Integer>> entryOrdering = Ordering.natural().reverse().nullsLast()
                    .onResultOf(new Function<Entry<String, Integer>, Integer>() {
                        public Integer apply(Entry<String, Integer> entry) {
                          return entry.getValue();
                        }
                      });

         // Desired entries in desired order.  Put them in an ImmutableMap in this order.
            final ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
            int entryCounter = 0;
            for (Entry<String, Integer> entry : entryOrdering.sortedCopy(distribution.entrySet()))
            {
              builder.put(entry.getKey(), entry.getValue());
              entryCounter++;

              if(distributionLimit != 0 && entryCounter >= distributionLimit)
              {
                  break;
              }

            }

            result = builder.build();
        }

        return result;
    }

    public Integer getUniqueCount()
    {
        return this.uniqueCount;
    }

    @JsonIgnore
    public void applyDistributionLimit(final Integer distributionLimit)
    {
        if(distributionLimit > 0)
        {
            this.applySortOnDistribution = true;
            this.distributionLimit = distributionLimit;
        }
    }

    /**
     * Indicate that the distribution should only include the uniqueIds in the
     * given set.  This method will only do anything if the uniqieIds set is
     * not null and not empty.  If you wish to include no uniqueIds ever, the
     * exclude distribution is the appropriate option
     * @param uniqueIds
     */
    @JsonIgnore
    public void applyDistributionFilterById(final Set<String> uniqueIds)
    {
        if(uniqueIds != null && !uniqueIds.isEmpty())
        {
            this.applyDistrubtionFilterOnId = true;
            this.uniqueIdsForFilter = uniqueIds;
        }
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

            //If this is a new counter, we know the unique count has increased
            this.uniqueCount++;
        }

        distributionCount += incrementValue;

        distribution.put(uniqueIdentifier, distributionCount);
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
