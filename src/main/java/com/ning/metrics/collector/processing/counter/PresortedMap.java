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

import com.google.common.collect.Lists;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * This is a sorted map that can be used to bootstrap a tree map without having
 * to sort every entry on insert.  This map uses an internal immutable set
 * builder to keep track of the entries and their order.  This class is designed
 * specifically to work with TreeMap (Java 1.6), so only the bare minimum of
 * functionality has been added.
 *
 * The only methods needed to accomplish our thin goals are
 *
 * int size()
 * Set<Map.Entry<K, V>> entrySet()
 *
 * and even the entry set produced above only needs to have the iterator method
 * implemented.  To that end, this class has an internal implementation of Set
 * that is transparent and only has the method
 *
 * Iterator<Map.Entry<K, V>> iterator()
 *
 * implemented
 *
 * @author kguthrie
 * @param <K>
 * @param <V>
 */
public class PresortedMap<K, V> implements SortedMap<K, V> {

    private final Comparator<? super K> comparator;
    private final List<Map.Entry<K, V>> entries;
    private final IterableToSetAdaptor entrySet;
    private final List<V> values;
    private final IterableToSetAdaptor valueSet;

    public PresortedMap(Comparator<? super K> comparator) {
        this.comparator = comparator;
        entries = Lists.newArrayList();
        entrySet = new IterableToSetAdaptor(entries);
        values = Lists.newArrayList();
        valueSet = new IterableToSetAdaptor(values);
    }

    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public K firstKey() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public K lastKey() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Collection<V> values() {
        return values;
    }

    /**
     * Get the values as a set;
     * @return
     */
    public Set<V> getValueSet() {
        return valueSet;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return entrySet;
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public V get(Object key) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public V put(K key, V value) {
        entries.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        values.add(value);
        return null;
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clear() {
        entries.clear();
        values.clear();
    }

}
