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
package com.ning.metrics.collector.processing.counter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * Container for the unique identifier counts aka distribution for a rolled-up
 * counter.  This class will
 * @author kguthrie
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonSerialize(using = CounterDistribution.Serializer.class)
public class CounterDistribution implements Map<String, Integer> {

    /**
     * Custom serializer for when Distribution is written to json.  This
     * will take into account the distribution limit of the serialization
     */
    public static class Serializer extends JsonSerializer<CounterDistribution> {

        @Override
        public void serialize(CounterDistribution value, JsonGenerator jgen,
                SerializerProvider provider)
                        throws IOException, JsonProcessingException {

            int index = 0;

            jgen.writeStartObject();

            for (Map.Entry<String, Integer> e : value.entrySet()) {

                if (value.getSerializationLimit() != null
                        && value.getSerializationLimit() > 0
                        && index++ >= value.getSerializationLimit()) {
                    break;
                }

                jgen.writeNumberField(e.getKey(), e.getValue());
            }

            jgen.writeEndObject();
        }

    }

    /**
     * Class that wraps a distribution entry.  Instances of this class will
     * contain the uniqueId and count for the distribution
     */
    public static class Entry implements Map.Entry<String, Integer>{
        private final String uniqueId;
        private int count;

        public Entry(String uniqueId, int count) {
            this.uniqueId = uniqueId;
            this.count = count;
        }

        @Override
        public String getKey() {
            return uniqueId;
        }

        @Override
        public Integer getValue() {
            return count;
        }

        @Override
        public Integer setValue(Integer value) {
            int oldVal = count;
            count = value;
            return oldVal;
        }

        /**
         * increment the count for the given entry
         * @param inc
         * @return the old value
         */
        public int increment(int inc) {
            int oldVal = count;
            count += inc;
            return oldVal;
        }

    }

    private final PresortedMap<Entry, Entry> presortedMap;
    private Map<Entry, Entry> postSortedMap;
    private Map<String, Entry> entryLocator;
    private Integer serializationLimit;

    public CounterDistribution() {
        serializationLimit = null;
        entryLocator = null;
        presortedMap = new PresortedMap<Entry, Entry>(new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {
                int result = o2.getValue() - o1.getValue();

                if (result == 0) {
                    result = o1.getKey().compareTo(o2.getKey());
                }

                return result;
            }
        });

        postSortedMap = null;
    }

    /**
     * Get the limit on the number of distribution elements returned in the json
     * serialization
     * @return the serializationLimit
     */
    public Integer getSerializationLimit() {
        return serializationLimit;
    }

    /**
     * Set the limit on the number of distribution elements returned in the json
     * serialization
     * @param serializationLimit the serializationLimit to set
     */
    public void setSerializationLimit(Integer serializationLimit) {
        this.serializationLimit = serializationLimit;
    }

    /**
     * Add the given unique id with the given count to the distribution with the
     * assumption that the presorted entries are added in order
     * @param uniqueId
     * @param count
     */
    public void putPresortedEntry(String uniqueId, int count) {
        assert(postSortedMap == null);

        Entry curr = new Entry(uniqueId, count);
        presortedMap.put(curr, curr);
    }

    /**
     * ensure that the distribution's storage has been updated to allow for
     * new entries to be added, old entries to be updated, and keys to be
     * queried
     */
    private void ensureSorted() {
        if (postSortedMap == null) {
            postSortedMap = Maps.newTreeMap(presortedMap);
            entryLocator = Maps.newHashMap();

            for (Entry e : presortedMap.values()) {
                entryLocator.put(e.getKey(), e);
            }
        }
    }

    /**
     * add an entry to the distribution that might not by new or have the lowest
     * count (IE not presorted).  If this is the first time this method is
     * called, the internal structure used for maintaining a sorted map will be
     * populated and the new distribution entry will be added
     * @param uniqueId
     * @param count
     * @return the old count for the given unique id or null if absent before
     */
    public Integer increment(String uniqueId, int count) {
        ensureSorted();

        Entry curr;
        Entry existing = entryLocator.get(uniqueId);
        Integer result = null;

        if (existing != null) {
            postSortedMap.remove(existing);
            int oldValue = existing.increment(count);
            result = oldValue;
            curr = existing;
        }
        else {
            curr = new Entry(uniqueId, count);
            entryLocator.put(uniqueId, curr);
        }

        postSortedMap.put(curr, curr);

        return result;
    }

    // Direct putting is not supported.  Use increment instead
    @Override
    public Integer put(String key, Integer value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int size() {
        if (postSortedMap != null) {
            return postSortedMap.size();
        }

        return presortedMap.size();
    }

    @Override
    public boolean isEmpty() {
        if (postSortedMap != null) {
            return postSortedMap.isEmpty();
        }

        return presortedMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        ensureSorted();
        return entryLocator.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Integer get(Object key) {
        ensureSorted();
        Entry resultEntry = entryLocator.get(key);
        return resultEntry == null ? null : resultEntry.getValue();
    }

    @Override
    public Integer remove(Object key) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void putAll(Map<? extends String, ? extends Integer> m) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clear() {
        if (postSortedMap != null) {
            postSortedMap.clear();
        }

        presortedMap.clear();
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Collection<Integer> values() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<Map.Entry<String, Integer>> entrySet() {
        if (postSortedMap != null) {
            return new IterableToSetAdaptor(
                    postSortedMap.values());
        }
        return (Set)presortedMap.getValueSet();
    }

}
