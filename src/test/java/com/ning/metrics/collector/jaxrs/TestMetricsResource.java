/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ning.metrics.collector.jaxrs;

import com.google.common.base.Optional;
import com.ning.metrics.collector.processing.counter.CompositeCounter;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author kguthrie
 */
public class TestMetricsResource {

    private Resource resource = new Resource();

    public TestMetricsResource() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}

    @Test
    public void testParseUniqueIdSet() {
        Optional<Set<String>> result = resource.parseUniqueIdSet(null);

        Assert.assertNotNull(result);
        Assert.assertFalse(result.isPresent());

        result = resource.parseUniqueIdSet("");

        Assert.assertNotNull(result);
        Assert.assertFalse(result.isPresent());

        result = resource.parseUniqueIdSet("\t \n");

        Assert.assertNotNull(result);
        Assert.assertFalse(result.isPresent());

        result = resource.parseUniqueIdSet("member1");

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(1, result.get().size());
        Assert.assertTrue(result.get().contains("member1"));

        result = resource.parseUniqueIdSet("member1,member2;member3:member4");

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(4, result.get().size());
        Assert.assertTrue(result.get().contains("member1"));
        Assert.assertTrue(result.get().contains("member2"));
        Assert.assertTrue(result.get().contains("member3"));
        Assert.assertTrue(result.get().contains("member4"));
    }

    @Test
    public void testParseCompositeCounterIfPresent() {
        Optional<CompositeCounter> result =
                resource.parseCompositeCounterIfPresent(null);

        Assert.assertNotNull(result);
        Assert.assertFalse(result.isPresent());

        result = resource.parseCompositeCounterIfPresent("");

        Assert.assertNotNull(result);
        Assert.assertFalse(result.isPresent());

        result = resource.parseCompositeCounterIfPresent("\t \n");

        Assert.assertNotNull(result);
        Assert.assertFalse(result.isPresent());

        result = resource.parseCompositeCounterIfPresent("simpleCounter");

        Assert.assertNotNull(result);
        Assert.assertFalse(result.isPresent());

        result = resource.parseCompositeCounterIfPresent(
                "simpleCounter:simpleCounter"); // dumb but legit

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(new CompositeCounter("simpleCounter",
                new String[] {"simpleCounter"}, new int[] {1}), result.get());

        result = resource.parseCompositeCounterIfPresent(
                "composite:simpleCounter*2");

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(new CompositeCounter("composite",
                new String[] {"simpleCounter"}, new int[] {2}), result.get());

        result = resource.parseCompositeCounterIfPresent(
                "composite:3*simpleCounter");

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(new CompositeCounter("composite",
                new String[] {"simpleCounter"}, new int[] {3}), result.get());

        result = resource.parseCompositeCounterIfPresent(
                "composite:simpleCounter*2+anotherCounter");

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(new CompositeCounter("composite",
                new String[] {"simpleCounter", "anotherCounter"},
                new int[] {2, 1}), result.get());

        result = resource.parseCompositeCounterIfPresent(
                "composite:simpleCounter*2+2*anotherCounter*2");

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(new CompositeCounter("composite",
                new String[] {"simpleCounter", "anotherCounter"},
                new int[] {2, 4}), result.get());
    }

    private static class Resource extends MetricsResource {

        public Resource() {
            super(null, null);
        }

        @Override
        public Optional<Set<String>> parseUniqueIdSet(String uniqueIdParam) {
            return super.parseUniqueIdSet(uniqueIdParam);
        }

        @Override
        public Optional<CompositeCounter> parseCompositeCounterIfPresent(
            String counterTypeParam) {
            return super.parseCompositeCounterIfPresent(counterTypeParam);
        }

    }
}
