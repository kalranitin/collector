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
package com.ning.metrics.collector.processing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.ning.metrics.collector.processing.db.model.CounterEvent;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.joda.time.DateTime;

/**
 * This class is effectively a buffer for aggregating multiple individual
 * counter events of similar types into a single counter event. The goal here is
 * to alleviate the load on the database by reducing the number of individual
 * atomic increment calls.
 *
 * @author kguthrie
 */
public class CounterEventAggregator {

    private ConcurrentHashMap<AggregatedCounterKey, AggregatedCounter> curr;
    private ConcurrentHashMap<AggregatedCounterKey, AggregatedCounter> swap;
    private ConcurrentHashMap<AggregatedCounterKey, AggregatedCounter> idle;

    public CounterEventAggregator() {
        curr = new ConcurrentHashMap<>();
        swap = new ConcurrentHashMap<>();
        idle = new ConcurrentHashMap<>();
    }

    /**
     * Add the given counter event to those to be aggregated
     *
     * @param event
     */
    public void addEvent(CounterEvent event) {

        String counterGroup = event.getAppId();

        for (CounterEventData counterEventData : event.getCounterEvents()) {
            addEventCounterData(counterGroup, counterEventData);
        }

    }

    private void addEventCounterData(
            String counterGroup, CounterEventData data) {

        int identifier = data.getIdentifierCategory();
        DateTime eventDate = data.getCreatedDate();
        String eventDateString = data.getFormattedDate();
        String uniqueId = data.getUniqueIdentifier();

        for (Map.Entry<String, Integer> entry : data.getCounters().entrySet()) {
            String counter = entry.getKey();
            int count = entry.getValue();
            addEventCounter(counterGroup, counter, identifier,
                    uniqueId, eventDateString, eventDate, count);
        }

    }

    private void addEventCounter(String counterGroup, String counterName,
            int idenfier, String uniqueId, String eventDateString,
            DateTime eventDate, int count) {

        ConcurrentHashMap<AggregatedCounterKey, AggregatedCounter> mapInUse
                = curr;

        AggregatedCounterKey counterKey = new AggregatedCounterKey(
                counterGroup, eventDateString, eventDate,
                idenfier, uniqueId);
        AggregatedCounter counter = new AggregatedCounter();
        AggregatedCounter existingCounter;


        if ((existingCounter
                = mapInUse.putIfAbsent(counterKey, counter)) != null) {
            counter = existingCounter;
        }

        counter.addCount(counterName, count);
    }

    /**
     * flush the buffer and return the aggregated results since the last flush
     *
     * @return
     */
    public synchronized Iterable<CounterEvent> flush() {

        List<CounterEvent> result = Lists.newArrayList();

        // Swap the maps;
        ConcurrentHashMap<AggregatedCounterKey, AggregatedCounter> flush = curr;
        curr = idle;
        idle = swap;
        swap = flush;

        // Iterate through the non-active map, and create counter event for each
        // aggregated counter
        for (Map.Entry<AggregatedCounterKey, AggregatedCounter> e
                : flush.entrySet()) {
            AggregatedCounterKey key = e.getKey();
            AggregatedCounter val = e.getValue();

            result.add(convert(key, val));
        }

        return ImmutableList.copyOf(result);
    }

    /**
     * convert an aggregated counter's components into a count event
     *
     * @param key
     * @param counter
     * @return
     */
    private CounterEvent convert(AggregatedCounterKey key,
            AggregatedCounter counter) {

        Map<String, Integer> simpleMap = new HashMap<>();

        for (Map.Entry<String, AtomicInteger> e
                : counter.getCounts().entrySet()) {
            simpleMap.put(e.getKey(), e.getValue().get());
        }

        CounterEventData data = new CounterEventData(key.getUniqueId(),
                key.getIdentifierCategory(), key.getCounterDate(),
                simpleMap);

        List<CounterEventData> dataList = Lists.newLinkedList();
        dataList.add(data);

        return new CounterEvent(key.getCounterGroup(), dataList);
    }

    /**
     * Class for containing the key information about an aggregated counter
     */
    private static final class AggregatedCounterKey {

        private final String counterGroup;
        private final String counterDateString;
        private final int identifierCategory;
        private final String uniqueId;
        private final DateTime counterDate;

        public AggregatedCounterKey(String counterGroup,
                String counterDateString, DateTime counterDate,
                int identifierCategory, String uniqueId) {
            this.counterGroup = counterGroup;
            this.counterDateString = counterDateString;
            this.counterDate = counterDate;
            this.identifierCategory = identifierCategory;
            this.uniqueId = uniqueId;
        }

        /**
         * @return the counterGroup
         */
        public String getCounterGroup() {
            return counterGroup;
        }

        /**
         * @return the counterDateString
         */
        public String getCounterDateString() {
            return counterDateString;
        }

        /**
         * @return the identifierCategory
         */
        public int getIdentifier() {
            return getIdentifierCategory();
        }

        /**
         * @return the identifierCategory
         */
        public int getIdentifierCategory() {
            return identifierCategory;
        }

        /**
         * @return the uniqueId
         */
        public String getUniqueId() {
            return uniqueId;
        }

        /**
         * @return the counterDate
         */
        public DateTime getCounterDate() {
            return counterDate;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final AggregatedCounterKey other = (AggregatedCounterKey) obj;
            if ((this.counterGroup == null) ? (other.counterGroup != null) : !this.counterGroup.equals(other.counterGroup)) {
                return false;
            }
            if ((this.counterDateString == null) ? (other.counterDateString != null) : !this.counterDateString.equals(other.counterDateString)) {
                return false;
            }
            if (this.identifierCategory != other.identifierCategory) {
                return false;
            }
            if ((this.uniqueId == null) ? (other.uniqueId != null) : !this.uniqueId.equals(other.uniqueId)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 97 * hash + (this.counterGroup != null ? this.counterGroup.hashCode() : 0);
            hash = 97 * hash + (this.counterDateString != null ? this.counterDateString.hashCode() : 0);
            hash = 97 * hash + this.identifierCategory;
            hash = 97 * hash + (this.uniqueId != null ? this.uniqueId.hashCode() : 0);
            return hash;
        }

    }

    /**
     * Class containing the actual aggregated counts
     */
    private static final class AggregatedCounter {

        private ConcurrentHashMap<String, AtomicInteger> counts;
        private boolean initialized;

        public AggregatedCounter() {
            this.counts = null;
            this.initialized = false;
        }

        /**
         * @return the counts
         */
        public Map<String, AtomicInteger> getCounts() {
            return counts;
        }

        /**
         * Increment the given counter in this aggregate by the given increment
         *
         * @param counter
         * @param increment
         */
        public void addCount(String counter, int increment) {

            if (!initialized) {
                synchronized (this) {
                    if (!initialized) {
                        counts = new ConcurrentHashMap<>();
                        initialized = true;
                    }
                }
            }

            AtomicInteger count = new AtomicInteger(increment);

            // if count for that key was already present, then increment its
            // count by the given increment
            if ((count = counts.putIfAbsent(counter, count)) != null) {
                count.addAndGet(increment);
            }
        }
    }

}
