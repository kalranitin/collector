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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is used both to store the raw events of a feed and to transport
 * the feed after rollup processing to the client.
 * @author kguthrie
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Feed
{
    private List<FeedEvent> feedEvents;
    private transient final Set<FeedEvent> uniqueSet;
    private transient final Map<String, int[]> minRollupKeepLasts;

    /**
     * Convenience constructor for json that enables event processing on this
     * given list of events
     * @param feedEvents
     */
    @JsonCreator
    public Feed(@JsonProperty("feedEvents") List<FeedEvent> feedEvents) {
        this(feedEvents, false);
    }

    /**
     * Create a new feed with the given set of events.  the given boolean for
     * disableProcessing allows a set of feed of events to be used directly with
     * no overhead for sorting or filtering.  Disabling of event processing is
     * only meant to be used when building a final result feed for transport
     * @param feedEvents
     * @param disableProcessing
     */
    public Feed(List<FeedEvent> feedEvents, boolean disableProcessing) {
        if (disableProcessing) {
            this.feedEvents = Lists.newArrayList(feedEvents);
            minRollupKeepLasts = null;
            uniqueSet = null;
            return;
        }
        uniqueSet = Sets.newHashSet();
        minRollupKeepLasts = Maps.newHashMap();
        this.feedEvents = processEventList(
                feedEvents, uniqueSet, minRollupKeepLasts);
    }

    public List<FeedEvent> getFeedEvents(){
        return feedEvents;
    }

    /**
     * Process the given list of events and determine the minimum positive
     * rollupKeep value for the list involved as well as sorting (if needed)
     * the list by descending timestamp and removing duplicates
     * @param eventList list of feed events to be processed
     * @param uniqueSet set that is used to determine if a given if a given
     *          feed event is a duplicate and should be filtered
     * @param minRollupKeepLastTarget a map of rollup keys to the minimum
     *          rollupKeepLast seen for that rollup key
     * @return the processed list of feed events after filtering and ensured
     *          sorting
     */
    @JsonIgnore
    private List<FeedEvent> processEventList(List<FeedEvent> eventList,
             Set<FeedEvent> uniqueSet,
             Map<String, int[]> minRollupKeepLastTarget) {

        List<FeedEvent> result = Lists.newArrayList();
        FeedEventData lastEvent = null;
        boolean needsSorting = false;

        for (FeedEvent event : eventList) {
            if (event == null) {
                continue;
            }

            FeedEventData eventData = event.getEvent();

            if (eventData == null) {
                continue;
            }

            // filter duplicates
            if (uniqueSet.add(event)) {
                result.add(event);
            }
            else {
                continue;
            }

            // if the previous event occurred before the current one, then
            // we need to sort because this feed should be kept in reverse
            // chronological order
            if (lastEvent != null) {
                if (lastEvent.getCreatedDate().isBefore(
                        eventData.getCreatedDate())) {
                    needsSorting = true;
                }
            }

            lastEvent = eventData;

            String rollupKey = eventData.getRollupKey();
            if (Strings.isNullOrEmpty(rollupKey)) {
                continue;
            }

            int[] minRollupKeep = minRollupKeepLastTarget.get(rollupKey);

            if (minRollupKeep == null) {
                minRollupKeep = new int[] {Integer.MAX_VALUE};
                minRollupKeepLastTarget.put(rollupKey, minRollupKeep);
            }

            if (eventData.isRollupKeepLastPresent()) {
                minRollupKeep[0]
                        = minRollupKeep[0] < eventData.getRollupKeepLast()
                        ? minRollupKeep[0]
                        : eventData.getRollupKeepLast();
            }
        }

        if (needsSorting) {
            Collections.sort(result, FeedEvent.COMPARATOR);
        }

        return result;
    }

    /**
     * Merge the old list of feed events together with the new list of feed
     * events and limit the result to the maxFeedEvents most recent.  This
     * respects events with the same rollupKeys as being the single event
     * @param newFeedEvents
     * @param maxFeedEvents
     */
    @JsonIgnore
    public void addFeedEvents(List<FeedEvent> newFeedEvents,
            int maxFeedEvents) {

        List result = Lists.newArrayList();

        if (newFeedEvents == null || newFeedEvents.isEmpty()) {
            return; // nothing to do
        }

        List<FeedEvent> newEventList = processEventList(newFeedEvents,
                uniqueSet, minRollupKeepLasts);

        Iterator<FeedEvent> oldEventIterator = feedEvents.iterator();
        Iterator<FeedEvent> newEventIterator = newEventList.iterator();

        Map<String, int[]> rollupKeyEventCount = Maps.newHashMap();
        int groupedEventCount = 0;

        Iterator<FeedEvent> currIterator = newEventIterator;

        // Iterage backwards through first the old then the new feed events
        while (currIterator.hasNext()) {

            FeedEvent curr = currIterator.next();

            // Swap iterators if we hit the end of the new list
            if (currIterator == newEventIterator
                    && !currIterator.hasNext()) {
                currIterator = oldEventIterator;
            }

            if (curr == null) {
                continue;
            }

            FeedEventData eventData = curr.getEvent();

            if (eventData == null) {
                continue;
            }

            String rollupKey = eventData.getRollupKey();

            // Additional single events are only allowed when the event count
            // is below the maxFeedEvents count
            if (Strings.isNullOrEmpty(rollupKey)) {
                if (groupedEventCount < maxFeedEvents) {
                    groupedEventCount++;
                    result.add(curr);
                }
                continue;
            }

            int[] rollupKeyCount = rollupKeyEventCount.get(rollupKey);
            int[] minRollupKeepLast = minRollupKeepLasts.get(rollupKey);

            // New rolled event keys are only allowed when the event count
            // is below the maxFeedEvents count
            if (rollupKeyCount == null) {
                if (groupedEventCount < maxFeedEvents) {
                    rollupKeyCount = new int[] {0};
                    rollupKeyEventCount.put(rollupKey, rollupKeyCount);
                    groupedEventCount++;
                }
                else {
                    continue;
                }
            }

            // If this roll up key has a count bigger than or equal to the
            // minimum rollupKeepLast, then ignore this event
            if (rollupKeyCount[0] >= minRollupKeepLast[0]) {
                continue;
            }

            rollupKeyCount[0]++;

            result.add(curr);
        }

        feedEvents = result;
    }

    @JsonIgnore
    public boolean deleteFeedEvent(final String feedEventId) {

        boolean deleted = false;

        if (feedEventId == null) {
            return deleted;
        }

        Iterator<FeedEvent> it = feedEvents.iterator();

        while (it.hasNext()) {
            FeedEvent curr = it.next();

            if (curr != null
                    && curr.getEvent() != null
                    && feedEventId.equals(curr.getEvent().getFeedEventId())) {
                it.remove();
                uniqueSet.remove(curr);
                deleted = true;
                break;
            }
        }

        return deleted;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((feedEvents == null) ? 0 : feedEvents.hashCode());
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
        Feed other = (Feed) obj;
        if (feedEvents == null) {
            if (other.feedEvents != null)
                return false;
        }
        else if (!feedEvents.equals(other.feedEvents))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "Feeds [feedEvents=" + feedEvents + "]";
    }
}
