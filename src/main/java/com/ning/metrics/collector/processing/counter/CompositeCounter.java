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

import java.util.Arrays;

/**
 * Storage class for information to describe a "composite" counter.  This means
 * a counter that is made up of a linear (for now) combination of other actual
 * counters.  This can be used for things like cost or fitness calculation, so
 * if a counter event represents a cost of $10 and another represents a cost of
 * $5 you can easily produced a composite counter of of total cost with a weight
 * of 10 on the first and a weight of 5 on the second.  This is especially
 * useful if you wanted to select the uniqueIds with the highest total cost
 * @author kguthrie
 */
public class CompositeCounter {

    private final String name;
    private final String[] compositeEvents;
    private final int[] compositeWeights;

    public CompositeCounter(String name, String[] compositeEvents,
            int[] compositeWeights) {
        this.name = name;
        this.compositeEvents = compositeEvents;
        this.compositeWeights = compositeWeights;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the compositeEvents
     */
    public String[] getCompositeEvents() {
        return compositeEvents;
    }

    /**
     * @return the compositeWeights
     */
    public int[] getCompositeWeights() {
        return compositeWeights;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + (this.name != null ? this.name.hashCode() : 0);
        hash = 37 * hash + Arrays.deepHashCode(this.compositeEvents);
        hash = 37 * hash + Arrays.hashCode(this.compositeWeights);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final CompositeCounter other = (CompositeCounter) obj;
        if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
            return false;
        }
        if (!Arrays.deepEquals(this.compositeEvents, other.compositeEvents)) {
            return false;
        }
        if (!Arrays.equals(this.compositeWeights, other.compositeWeights)) {
            return false;
        }
        return true;
    }



}
