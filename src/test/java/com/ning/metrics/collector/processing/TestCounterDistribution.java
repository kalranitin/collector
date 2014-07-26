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

import com.ning.metrics.collector.processing.counter.CounterDistribution;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author kguthrie
 */
@Test(groups = {"fast"})
public class TestCounterDistribution {

    CounterDistribution distribution;

    @BeforeMethod
    public void setup() throws Exception {
        distribution = new CounterDistribution();
    }

    @Test
    void testAddPresortedOnly() {
        distribution.putPresortedEntry("01", 3);
        distribution.putPresortedEntry("03", 1);

        String[] keySorting = new String[] {
            "01", "03"
        };

        int[] valueSorting = new int[] {
            3, 1
        };

        int index = 0;
        for (Map.Entry<String, Integer> e : distribution.entrySet()) {
            Assert.assertEquals(e.getKey(), keySorting[index]);
            Assert.assertEquals(e.getValue().intValue(), valueSorting[index]);
            index++;
        }
    }

    @Test
    public void testAddPresortedThenSorted() {
        distribution.putPresortedEntry("01", 3);
        distribution.putPresortedEntry("03", 1);

        distribution.increment("02", 2);

        String[] keySorting = new String[] {
            "01", "02", "03"
        };

        int[] valueSorting = new int[] {
            3, 2, 1
        };

        int index = 0;
        for (Map.Entry<String, Integer> e : distribution.entrySet()) {
            Assert.assertEquals(e.getKey(), keySorting[index]);
            Assert.assertEquals(e.getValue().intValue(), valueSorting[index]);
            index++;
        }
    }

    @Test
    public void testUniquenessDetection() {
        distribution.putPresortedEntry("01", 1);
        distribution.putPresortedEntry("03", 1);

        distribution.increment("02", 1);

        String[] keySorting = new String[] {
            "01", "02", "03"
        };

        int[] valueSorting = new int[] {
            1, 1, 1
        };

        int index = 0;
        for (Map.Entry<String, Integer> e : distribution.entrySet()) {
            Assert.assertEquals(e.getKey(), keySorting[index]);
            Assert.assertEquals(e.getValue().intValue(), valueSorting[index]);
            index++;
        }
    }

    @Test
    public void testUpdateUniquenessDetection() {
        distribution.putPresortedEntry("01", 1);
        distribution.putPresortedEntry("03", 1);

        distribution.increment("02", 1);
        distribution.increment("03", 1);

        String[] keySorting = new String[] {
            "03", "01", "02"
        };

        int[] valueSorting = new int[] {
            2, 1, 1
        };

        int index = 0;
        for (Map.Entry<String, Integer> e : distribution.entrySet()) {
            Assert.assertEquals(e.getKey(), keySorting[index]);
            Assert.assertEquals(e.getValue().intValue(), valueSorting[index]);
            index++;
        }
    }

}
