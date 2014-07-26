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
package com.ning.metrics.collector.jaxrs;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.ning.metrics.collector.processing.counter.CompositeCounter;
import com.ning.metrics.collector.processing.counter.RollUpCounterProcessor;
import com.ning.metrics.collector.processing.db.CounterStorage;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/rest/1.0/metrics")
public class MetricsResource
{
    private static final Logger log = LoggerFactory.getLogger(MetricsResource.class);
    private final CounterStorage counterStorage;
    private final RollUpCounterProcessor rollUpCounterProcessor;
    private final Pattern compositeCounterPattern;
    private final Pattern compositeCounterComponentPattern;


    @Inject
    public MetricsResource(final CounterStorage counterStorage, final RollUpCounterProcessor rollUpCounterProcessor)
    {
        this.counterStorage = counterStorage;
        this.rollUpCounterProcessor = rollUpCounterProcessor;

        // This pattern will match a complete string of the form
        //
        // compositeName:counterName1*weight1+counterName2*weight2+...
        //
        // There is a little bit of slop built in so that weights and counter
        // names can come in any order or not be present at all. Mostly this is
        // just here to induce eye strain.  The only capturing groups here are
        // for the name of the composite counter and for the entire formula
        // for the value.  The individual components of the formula will be
        /// determined using the next regex
        //
        // Oh, my eyes!!!
        this.compositeCounterPattern = Pattern.compile(
                "^([a-z\\_][\\w\\-]*)\\:((?:(?:\\d+\\*)?[a-z\\_][\\w\\-]*"
                        + "(?:\\*\\d+)?)(?:[\\+ ](?:\\d+\\*)?[a-z_][\\w\\-]*"
                        + "(?:\\*\\d+)?)*)$", Pattern.CASE_INSENSITIVE);

        // This is the pattern that allows us to iterate over the formula for
        // a copmosite key and capture its coeficients and counter name
        this.compositeCounterComponentPattern = Pattern.compile(
                "(?:(?:(\\d+)\\*)?([a-z\\_][\\w\\-]*)(?:\\*(\\d+))?)"
                        + "(?:$|[ \\+])");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{namespace}")
    public List<RolledUpCounter> getRolledUpCounter(
            @PathParam("namespace") String namespace,
            @QueryParam("fromDate") String fromDate,
            @QueryParam("toDate") String toDate,
            @QueryParam("aggregateByMonth") String aggregateByMonth,
            @QueryParam("includeDistribution") String includeDistribution,
            @QueryParam("counterType") List<String> counterTypes,
            @DefaultValue("") @QueryParam("uniqueIds") String uniqueIds,
            @DefaultValue("0") @QueryParam("distributionLimit") Integer distributionLimit)
    {
        if(Strings.isNullOrEmpty(namespace)) {
            return ImmutableList.of();
        }

        Set<String> counterTypesSet = null;
        Set<CompositeCounter> compositeCounterSet = null;

        // If counter types has data in it, then iterate through it and add
        // composite counters to the composite list and non-composite counters
        // to the regular list
        if (counterTypes != null && !counterTypes.isEmpty()) {
            counterTypesSet = Sets.newHashSet();
            compositeCounterSet = Sets.newHashSet();

            for (String counterTypeParam : counterTypes) {
                Optional<CompositeCounter> compositeOpt =
                        parseCompositeCounterIfPresent(counterTypeParam);

                // If a composite can be made from the parameter, then add it
                // to the list, and add its component parameters to the regular
                // counter list
                if (compositeOpt.isPresent()) {
                    CompositeCounter composite = compositeOpt.get();
                    compositeCounterSet.add(composite);

                    counterTypesSet.addAll(Arrays.asList(
                            composite.getCompositeEvents()));
                }
                else { // Otherwise just add it to the regular set
                    counterTypesSet.add(counterTypeParam);
                }

            }

            // If either set is not actually used, then set them back to null.
            // This is just a better way of indicating absence
            if (counterTypesSet.isEmpty()) {
                counterTypesSet = null;
            }
            if (compositeCounterSet.isEmpty()) {
                compositeCounterSet = null;
            }
        }

        return rollUpCounterProcessor.loadAggregatedRolledUpCounters(
                namespace,
                Optional.fromNullable(fromDate),
                Optional.fromNullable(toDate),
                Optional.fromNullable(counterTypesSet),
                Optional.fromNullable(compositeCounterSet),
                "y".equalsIgnoreCase(aggregateByMonth),
                false,
                !"y".equalsIgnoreCase(includeDistribution),
                parseUniqueIdSet(uniqueIds),
                Optional.fromNullable(distributionLimit));
    }


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/aggregate/{namespace}")
    public List<RolledUpCounter> getAggregatedRolledUpCounter(
            @PathParam("namespace") final String namespace,
            @QueryParam("fromDate") final String fromDate,
            @QueryParam("toDate") final String toDate,
            @QueryParam("aggregateByMonth") final String aggregateByMonth,
            @QueryParam("includeDistribution") final String includeDistribution,
            @QueryParam("counterType") final List<String> counterTypes,
            @DefaultValue("") @QueryParam("uniqueIds") final String uniqueIds,
            @DefaultValue("0") @QueryParam("distributionLimit")
                    final Integer distributionLimit) {

        if(Strings.isNullOrEmpty(namespace)) {
            return ImmutableList.of();
        }

        Set<String> counterTypesSet = null;
        Set<CompositeCounter> compositeCounterSet = null;

        // If counter types has data in it, then iterate through it and add
        // composite counters to the composite list and non-composite counters
        // to the regular list
        if (counterTypes != null && !counterTypes.isEmpty()) {
            counterTypesSet = Sets.newHashSet();
            compositeCounterSet = Sets.newHashSet();

            for (String counterTypeParam : counterTypes) {
                Optional<CompositeCounter> compositeOpt =
                        parseCompositeCounterIfPresent(counterTypeParam);

                // If a composite can be made from the parameter, then add it
                // to the list, and add its component parameters to the regular
                // counter list
                if (compositeOpt.isPresent()) {
                    CompositeCounter composite = compositeOpt.get();
                    compositeCounterSet.add(composite);

                    counterTypesSet.addAll(Arrays.asList(
                            composite.getCompositeEvents()));
                }
                else { // Otherwise just add it to the regular set
                    counterTypesSet.add(counterTypeParam);
                }

            }

            // If either set is not actually used, then set them back to null.
            // This is just a better way of indicating absence
            if (counterTypesSet.isEmpty()) {
                counterTypesSet = null;
            }
            if (compositeCounterSet.isEmpty()) {
                compositeCounterSet = null;
            }
        }

        return rollUpCounterProcessor.loadAggregatedRolledUpCounters(
                namespace,
                Optional.fromNullable(fromDate),
                Optional.fromNullable(toDate),
                Optional.fromNullable(counterTypesSet),
                Optional.fromNullable(compositeCounterSet),
                "y".equalsIgnoreCase(aggregateByMonth),
                true,
                !"y".equalsIgnoreCase(includeDistribution),
                parseUniqueIdSet(uniqueIds),
                Optional.fromNullable(distributionLimit));
    }

    /**
     * This method will turn a string containing a set of unique ids into a
     * set of those unique Ids split apart on a few basic url-safe characters.
     * the result is rapped in an optional so that the result is always
     * guaranteed to be not null
     * @param uniqueIdParam
     * @return
     */
    protected Optional<Set<String>> parseUniqueIdSet(String uniqueIdParam) {
        Set<String> result = null;

        if (uniqueIdParam != null && !uniqueIdParam.trim().isEmpty()) {
            result = Sets.newHashSet(uniqueIdParam.split(","));
        }

        return Optional.fromNullable(result);
    }


    /**
     * parse the composite counter definitions from the given counter type param
     * if once can be parsed.  Wrap the result in an optional so that the return
     * is always now null
     * @param counterTypeParam
     * @return
     */
    protected Optional<CompositeCounter> parseCompositeCounterIfPresent(
            String counterTypeParam) {
        CompositeCounter result = null;

        if (Strings.isNullOrEmpty(counterTypeParam)) {
            return Optional.fromNullable(result);
        }

        Matcher matcher = compositeCounterPattern.matcher(counterTypeParam);

        if (matcher.find()) {
            // first non-optional capture group is the composite name
            String compositeName = matcher.group(1);

            String formula = matcher.group(2);

            List<String> counterNamesList = Lists.newArrayList();
            List<Integer> weightsList = Lists.newArrayList();

            Matcher componentMatcher =
                    compositeCounterComponentPattern.matcher(formula);

            while (componentMatcher.find()) {
                // A component and weight will have the form
                // coefficientBefore*counterName*coefficientAfter
                // both coefficients are optional.  If both are present their
                // product is used as the actual weight

                String coeffientBeforeStr = componentMatcher.group(1);
                String counterName = componentMatcher.group(2);
                String coeffientAfterStr = componentMatcher.group(3);

                // RegEx requires these to be either absent or purely digits
                // I.E. Integer.parseInt won't fail
                int coefficientBefore =
                        Strings.isNullOrEmpty(coeffientBeforeStr)
                        ? 1
                        : Integer.parseInt(coeffientBeforeStr);

                int coefficientAfter =
                        Strings.isNullOrEmpty(coeffientAfterStr)
                        ? 1
                        : Integer.parseInt(coeffientAfterStr);

                weightsList.add(coefficientAfter * coefficientBefore);

                counterNamesList.add(counterName);
            }

            String[] componentCounterNames
                    = counterNamesList.toArray(new String[]{});

            int[] weights = new int[weightsList.size()];

            for (int i = 0; i < weights.length; i++) {
                weights[i] = weightsList.get(i);
            }

            result = new CompositeCounter(compositeName,
                    componentCounterNames, weights);
        }

        return Optional.fromNullable(result);
    }
}
