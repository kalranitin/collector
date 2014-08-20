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
package com.ning.metrics.collector.processing.feed;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ning.metrics.collector.processing.db.model.Feed;
import com.ning.metrics.collector.processing.db.model.FeedEvent;
import com.ning.metrics.collector.processing.db.model.FeedEventData;
import com.ning.metrics.collector.processing.db.model.RolledUpFeedEvent;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeedRollUpProcessor {

    private static final Logger log = LoggerFactory.getLogger(FeedRollUpProcessor.class);

    public Feed applyRollUp(final Feed feed, final Map<String, Object> filterMap) {
        final List<FeedEvent> compiledFeedEventList = Lists.newArrayList();

        // filter out all events which do not match "any" of the provided key value pair
        List<FeedEvent> feedEventList = (filterMap == null || filterMap.isEmpty()) ? Lists.newArrayList(feed.getFeedEvents()) : Lists.newArrayList(Iterables.filter(feed.getFeedEvents(), FeedEvent.isAnyKeyValuMatching(filterMap)));

        if (feedEventList.isEmpty()) {
            return new Feed(compiledFeedEventList);
        }

        ArrayListMultimap<String, FeedEvent> rolledEventMultiMap
                = ArrayListMultimap.create();
        Iterator<FeedEvent> iterator = feedEventList.iterator();
        Set<String> removalTargetSet = new HashSet<String>();

        while (iterator.hasNext()) {
            FeedEvent feedEvent = iterator.next();

            // Do not include suppress types of events
            if (Objects.equal(FeedEventData.EVENT_TYPE_SUPPRESS,
                    feedEvent.getEvent().getEventType())) {
                List<String> removalTargets
                        = feedEvent.getEvent().getRemovalTargets();

                // add non-null entries to set of removal targets
                if (removalTargets != null) {
                    for (String removalTarget : removalTargets) {
                        if (!Strings.isNullOrEmpty(removalTarget)) {
                            removalTargetSet.add(removalTarget);
                        }
                    }
                }

                continue;
            }

            // If any of the removal targets are matching then the specific event is to be suppressed
            if (containsAnyRemovalTargets(removalTargetSet
                    , feedEvent.getEvent().getRemovalTargets())) {
                continue;
            }

            if (feedEvent.getEvent() != null && !Strings.isNullOrEmpty(feedEvent.getEvent().getRollupKey())) {

                String rollupKey = feedEvent.getEvent().getRollupKey();
                List<FeedEvent> rolledUpEventList
                        = rolledEventMultiMap.get(rollupKey);
                FeedEvent compareFeedEvent = null;

                // a null rolled event build indicates a new rollup key
                if (rolledUpEventList.isEmpty()) {
                    RolledUpFeedEvent rolledEvent
                            = RolledUpFeedEvent.createForAssembly(
                                    rollupKey, rolledUpEventList);
                    compiledFeedEventList.add(rolledEvent);
                }
                else {
                    compareFeedEvent = rolledUpEventList.get(0);
                }

                // If there is an event to compare this one to, only roll up
                // this event if it is within 24 hours from the most recent
                // and if
                if (compareFeedEvent == null
                        || feedEvent.getEvent().getCreatedDate().plusHours(24)
                        .isAfter(compareFeedEvent.getEvent().getCreatedDate())) {
                    // event been iterated upon is a candidate for roll up
                    rolledUpEventList.add(feedEvent);
                }

                // otherwise ignore the old, would-be-rolled event
                continue;
            }

            // An event that makes it through to the bottom of the while loop
            // should be added to the compiled list of events
            compiledFeedEventList.add(feedEvent);
        }

        // return a new unprocessed feed using the given list of feed events
        return new Feed(compiledFeedEventList, true);
    }

    /**
     * This method determines if the given list of targets to test contains any
     * of the given removal targets
     * @param removalTargets strings indicating events that need to be removed
     *          from the feed
     * @param targetsToTest this is the list of of removal targets for a single
     *          event.  If any of these match the set of removal targets, true
     *          will be returned
     * @return
     */
    private boolean containsAnyRemovalTargets(Set<String> removalTargets,
            List<String> targetsToTest) {
        if (removalTargets == null
                || targetsToTest == null
                || removalTargets.isEmpty()
                || targetsToTest.isEmpty()) {
            return false;
        }

        for (String targetToTest : targetsToTest) {
            if (targetToTest != null) {
                if (removalTargets.contains(targetToTest)) {
                    return true;
                }
            }
        }

        return false;
    }

}
