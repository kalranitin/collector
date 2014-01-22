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
import com.google.common.collect.ArrayListMultimap;

import javax.annotation.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CounterSubscription
{
    private final Long id;
    private final String appId;
    private final ArrayListMultimap<Integer,String> identifierDistribution;
    
    @JsonCreator
    public CounterSubscription(@JsonProperty("id") @Nullable Long id, 
        @JsonProperty("appId") final String appId, 
        @JsonProperty("identifierDistribution") final ArrayListMultimap<Integer, String> identifierDistribution)
    {
        this.id = id;
        this.appId = appId;
        this.identifierDistribution = identifierDistribution;
    }
    
    @JsonIgnore
    public Long getId()
    {
        return id;
    }

    public String getAppId()
    {
        return appId;
    }

    public ArrayListMultimap<Integer, String> getIdentifierDistribution()
    {
        return identifierDistribution;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((appId == null) ? 0 : appId.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
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
        CounterSubscription other = (CounterSubscription) obj;
        if (appId == null) {
            if (other.appId != null)
                return false;
        }
        else if (!appId.equals(other.appId))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        }
        else if (!id.equals(other.id))
            return false;
        return true;
    }
}
