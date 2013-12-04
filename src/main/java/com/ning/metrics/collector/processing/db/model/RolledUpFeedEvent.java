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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;

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

    /**
     * Implementation of the RolledUpFeedEvent class that can be used to
     * construct an instance of the rolled up feed event an event at a time.
     */
    public static class Builder extends FeedEvent {

        private final String rollUpType;
        private final ArrayList<FeedEvent> feedEvents;

        public Builder(String rollUpType) {

            super(null,null,null,null);

            this.rollUpType = rollUpType;
            this.feedEvents = new ArrayList<FeedEvent>();
        }

        /**
         * Build a feed event based on this rolled up feed event builder.  If
         * there is only one event in the list of be built, then just return the
         * original event.  If there is more than one, return a legit, immutable
         * RolledUpFeedEvent
         * @return
         */
        public FeedEvent build() {
            if (feedEvents.size() == 1) {
                return feedEvents.get(0);
            }

            return new RolledUpFeedEvent(rollUpType,
                    ImmutableList.copyOf(feedEvents));
        }


        /**
         * Add an event to the roll up
         * @param event
         */
        public void add(FeedEvent event) {
            feedEvents.add(event);
        }

        /**
         * return the first feed event in the list to be rolled
         * @return
         */
        public FeedEvent getFirst() {
            if (feedEvents.isEmpty()) {
                return null;
            }

            return feedEvents.get(0);
        }
    }

}
