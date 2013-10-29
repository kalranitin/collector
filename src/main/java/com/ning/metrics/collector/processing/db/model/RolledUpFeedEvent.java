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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RolledUpFeedEvent extends FeedEvent
{
    private final String rollUpType;
    private final List<FeedEvent> feedEvents;
    
    
    @JsonCreator
    public RolledUpFeedEvent(@JsonProperty("rollUpType") String rollUpType, @JsonProperty("feedEvents") List<FeedEvent> feedEvents){
        
        super(null,null,null,null);
        
        this.feedEvents = feedEvents;
        this.rollUpType = rollUpType;
        
    }
    
    
    public String getRollUpType()
    {
        return rollUpType;
    }

    public int getCount()
    {
        return this.feedEvents.size();
    }


    public List<FeedEvent> getFeedEvents()
    {
        return feedEvents;
    }

}
