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

import com.ning.metrics.collector.processing.db.model.Feed;
import com.ning.metrics.collector.processing.db.model.FeedEvent;
import com.ning.metrics.collector.processing.db.model.FeedEventData;
import com.ning.metrics.collector.processing.db.model.RolledUpFeedEvent;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FeedRollUpProcessor
{
    private static final Logger log = LoggerFactory.getLogger(FeedRollUpProcessor.class);


    public Feed applyRollUp(final Feed feed, final Map<String,Object> filterMap){
        final List<FeedEvent> compiledFeedEventList = Lists.newArrayList();

        // filter out all events which do not match "any" of the provided key value pair
        List<FeedEvent> feedEventList = (filterMap == null || filterMap.isEmpty())?Lists.newArrayList(feed.getFeedEvents()) : Lists.newArrayList(Iterables.filter(feed.getFeedEvents(), FeedEvent.isAnyKeyValuMatching(filterMap)));

        if(feedEventList.isEmpty())
        {
            return new Feed(compiledFeedEventList);
        }

        FeedEventComparator feedEventComparator = new FeedEventComparator();
        Collections.sort(feedEventList,feedEventComparator);

        HashMap<String, ArrayList<FeedEvent>> rolledEventMultiMap =
                new HashMap<String, ArrayList<FeedEvent>>();
        Iterator<FeedEvent> iterator = feedEventList.iterator();
        Set<String> removalTargetSet = new HashSet<String>();

        while(iterator.hasNext()){
            FeedEvent feedEvent = iterator.next();

            // Do not include suppress types of events
            if(Objects.equal(FeedEventData.EVENT_TYPE_SUPPRESS, feedEvent.getEvent().getEventType()))
            {
                removalTargetSet.addAll(feedEvent.getEvent().getRemovalTargets());
                continue;
            }

            // If any of the removal targets are matching then the specific event is to be suppressed
            if(!removalTargetSet.isEmpty() && !Sets.intersection(removalTargetSet, ImmutableSet.copyOf(feedEvent.getEvent().getRemovalTargets())).isEmpty())
            {
                continue;
            }

            if(feedEvent.getEvent() != null && !Strings.isNullOrEmpty(feedEvent.getEvent().getRollupKey())){

                String rollupKey = feedEvent.getEvent().getRollupKey();
                ArrayList<FeedEvent> rolledUpEventList =
                        rolledEventMultiMap.get(rollupKey);
                FeedEvent compareFeedEvent = null;

                // a null rolled event build indicates a new rollup key
                if(rolledUpEventList == null) {
                    rolledUpEventList = Lists.newArrayList();
                    RolledUpFeedEvent rolledEvent =
                            RolledUpFeedEvent.createForAssembly(
                                    rollupKey, rolledUpEventList);
                    rolledEventMultiMap.put(rollupKey, rolledUpEventList);
                    compiledFeedEventList.add(rolledEvent);
                }
                else {
                    compareFeedEvent = rolledUpEventList.get(0);
                }

                // If there is an event to compare this one to, only roll
                if(compareFeedEvent == null ||
                        feedEvent.getEvent().getCreatedDate().plusHours(24)
                        .isAfter(compareFeedEvent.getEvent().getCreatedDate()))
                {
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

        return new Feed(compiledFeedEventList);
    }

    private static final class FeedEventComparator implements Comparator<FeedEvent>{

        @Override
        public int compare(FeedEvent feedEvent1, FeedEvent feedEvent2)
        {
            // Sort feed events by descending date
            return feedEvent2.getEvent().getCreatedDate().compareTo(feedEvent1.getEvent().getCreatedDate());
        }

    }

}
