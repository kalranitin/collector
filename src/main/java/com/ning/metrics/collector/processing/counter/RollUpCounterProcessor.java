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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.db.DatabaseCounterStorage;
import com.ning.metrics.collector.processing.db.DatabaseCounterStorage.CounterEventDataMapper;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;
import com.ning.metrics.collector.processing.db.model.RolledUpCounterData;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollUpCounterProcessor
{
    private static final Logger log = LoggerFactory.getLogger(RollUpCounterProcessor.class);
    private final IDBI dbi;
    private final CollectorConfig config;
    private final DatabaseCounterStorage counterStorage;
    private final ObjectMapper mapper;
    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final static Ordering<RolledUpCounter> orderingRolledUpCounterByDate = new Ordering<RolledUpCounter>() {

        @Override
        public int compare(RolledUpCounter left, RolledUpCounter right)
        {
            return left.getFromDate().compareTo(right.getFromDate());
        }};

    @Inject
    public RollUpCounterProcessor(final IDBI dbi, final DatabaseCounterStorage counterStorage, final CollectorConfig config, ObjectMapper mapper)
    {
        this.dbi = dbi;
        this.counterStorage = counterStorage;
        this.config = config;
        this.mapper = mapper;
    }

    public void rollUpStreamingDailyCounters(final CounterSubscription counterSubscription)
    {
        try {
            if (!isProcessing.compareAndSet(false, true)) {
                log.info("Asked to do counter roll up, but we're already processing!");
                return;
            }

            log.info(String.format("Running roll up process for Counter Subscription [%s]", counterSubscription.getAppId()));

            final DateTime toDateTime = new DateTime(DateTimeZone.UTC);
            Map<String, RolledUpCounter> rolledUpCounterMap = streamAndProcessDailyCounterData(counterSubscription,toDateTime);

            postRollUpProcess(counterSubscription, toDateTime, rolledUpCounterMap);

            log.info(String.format("Roll up process for Counter Subscription [%s] completed successfully!", counterSubscription.getAppId()));
        }
        catch (Exception e) {
            log.error(String.format("Exception occurred while performing counter roll up for [%s]", counterSubscription.getAppId()),e);
        }
        finally{
            isProcessing.set(false);
        }

    }

    private Map<String, RolledUpCounter> streamAndProcessDailyCounterData(final CounterSubscription counterSubscription,final DateTime toDateTime)
    {
        return dbi.withHandle(new HandleCallback<Map<String,RolledUpCounter>>() {

            @Override
            public Map<String, RolledUpCounter> withHandle(Handle handle) throws Exception
            {
                final String queryStr = "select metrics from metrics_daily where subscription_id = :subscriptionId"
                        +" and created_date <= :toDateTime";

                Query<Map<String, Object>> query = handle.createQuery(queryStr)
                        .bind("subscriptionId", counterSubscription.getId())
                        .setFetchSize(Integer.MIN_VALUE);

                query.bind("toDateTime", DatabaseCounterStorage.DAILY_METRICS_STORAGE_DATE_FORMATER.print(toDateTime));

                Map<String,RolledUpCounter> rolledUpCounterMap = new ConcurrentHashMap<String, RolledUpCounter>();

                ResultIterator<CounterEventData> streamingIterator = null;

                try {
                    streamingIterator = query.map(new CounterEventDataMapper(mapper)).iterator();

                    if(Objects.equal(null, streamingIterator))
                    {
                        return rolledUpCounterMap;
                    }

                    while(streamingIterator.hasNext())
                    {
                        processCounterEventData(counterSubscription, rolledUpCounterMap, streamingIterator.next());
                    }
                }
                catch (Exception e) {
                    log.error(String.format("Exception occurred while streaming and rolling up daily counter for app id: %s", counterSubscription.getAppId()), e);
                }
                finally {
                    if (streamingIterator != null) {
                        streamingIterator.close();
                    }
                }

                return rolledUpCounterMap;
            }});
    }

    public void rollUpDailyCounters(final CounterSubscription counterSubscription){
        try {
            if (!isProcessing.compareAndSet(false, true)) {
                log.info("Asked to do counter roll up, but we're already processing!");
                return;
            }
            final DateTime toDateTime = new DateTime(DateTimeZone.UTC);
            final Integer recordFetchLimit = config.getMaxCounterEventFetchCount();
            Integer recordOffSetCounter = 0;
            boolean fetchMoreRecords = true;

            Map<String, RolledUpCounter> rolledUpCounterMap = new ConcurrentHashMap<String, RolledUpCounter>();

            log.info(String.format("Running roll up process for Counter Subscription [%s]", counterSubscription.getAppId()));

            while(fetchMoreRecords)
            {
             // Load daily counters stored for the respective subscription limiting to now() and getMaxCounterEventFetchCount
                Iterator<CounterEventData> dailyCounterList = counterStorage.loadDailyMetrics(counterSubscription.getId(), toDateTime, recordFetchLimit, recordOffSetCounter).iterator();
                if(Objects.equal(null, dailyCounterList) || !dailyCounterList.hasNext())
                {
                    fetchMoreRecords = false;
                    break;
                }

                log.info(String.format("Processing counter events for %s on offset %d", counterSubscription.getAppId(), recordOffSetCounter));

                // increment recordOffset to fetch next getMaxCounterEventFetchCount
                recordOffSetCounter += recordFetchLimit;

                while(dailyCounterList.hasNext())
                {
                    processCounterEventData(counterSubscription, rolledUpCounterMap, dailyCounterList.next());
                }

                log.info(String.format("Roll up completed %s on offset %d", counterSubscription.getAppId(), recordOffSetCounter));

            }

            postRollUpProcess(counterSubscription, toDateTime, rolledUpCounterMap);

            log.info(String.format("Roll up process for Counter Subscription [%s] completed successfully!", counterSubscription.getAppId()));
        }
        catch (Exception e) {
            log.error(String.format("Exception occurred while performing counter roll up for [%s]", counterSubscription.getAppId()),e);
        }
        finally{
            isProcessing.set(false);
        }

    }

    private void postRollUpProcess(final CounterSubscription counterSubscription, final DateTime toDateTime, Map<String, RolledUpCounter> rolledUpCounterMap)
    {
        if(!rolledUpCounterMap.isEmpty())
        {
            log.info(String.format("Evaluating Uniques and updating roll up counter for %s", counterSubscription.getAppId()));
            for(RolledUpCounter rolledUpCounter : rolledUpCounterMap.values())
            {
                // Evaluate Uniqes for rolled up counters
                rolledUpCounter.evaluateUniques();

                //Save
                counterStorage.insertOrUpdateRolledUpCounter(counterSubscription.getId(), rolledUpCounter);
            }

            log.info(String.format("Deleting daily counters for %s which are <= %s", counterSubscription.getAppId(), toDateTime));
            // Delete daily metrics which have been accounted for the roll up.
            // There may be more additions done since this process started which is why the evaluation time is passed on.
            counterStorage.deleteDailyMetrics(counterSubscription.getId(), toDateTime);
        }
    }

    private void processCounterEventData(final CounterSubscription counterSubscription, Map<String, RolledUpCounter> rolledUpCounterMap, final CounterEventData counterEventData)
    {
        final String rolledUpCounterKey = counterSubscription.getAppId()+counterEventData.getFormattedDate();

        RolledUpCounter rolledUpCounter = rolledUpCounterMap.get(rolledUpCounterKey);

        if(Objects.equal(null, rolledUpCounter))
        {
            rolledUpCounter = counterStorage.loadRolledUpCounterById(rolledUpCounterKey, false, Optional.of(0));
            if(Objects.equal(null, rolledUpCounter))
            {
                rolledUpCounter = new RolledUpCounter(counterSubscription.getAppId(), counterEventData.getCreatedDate(), counterEventData.getCreatedDate());
            }
        }

        rolledUpCounter.updateRolledUpCounterData(counterEventData, counterSubscription.getIdentifierDistribution().get(counterEventData.getIdentifierCategory()));
        rolledUpCounterMap.put(rolledUpCounterKey, rolledUpCounter);
    }

    public List<RolledUpCounter> loadAggregatedRolledUpCounters(
            final String appId, final Optional<String> fromDateOpt,
            final Optional<String> toDateOpt,
            final Optional<Set<String>> counterTypesOpt,
            final Optional<Set<CompositeCounter>> compositeCountersOpt,
            final boolean aggregateByMonth,
            final boolean aggregateEntireRange,
            final boolean excludeDistribution,
            final Optional<Set<String>> uniqueIdsOpt,
            final Optional<Integer> distributionLimit)
    {
        CounterSubscription counterSubscription = counterStorage.loadCounterSubscription(appId);
        if(counterSubscription == null)
        {
            return Lists.newArrayList();
        }
        DateTime fromDate = fromDateOpt.isPresent()?new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis(fromDateOpt.get()),DateTimeZone.UTC):null;
        DateTime toDate = toDateOpt.isPresent()?new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis(toDateOpt.get()),DateTimeZone.UTC):null;

        // If we aggregate the entire date range don't want to limit the
        // distribution returned in the individual time slices.  Instead
        // We want to get everything from each time slice and sort and limit on
        // the aggregated result.
        //
        // Similarly if we are asking for any composite counters, we don't
        // want to limit the distribution before we can accurately compute the
        // the composites for each time slice
        //
        // We don't need to limit the distribution if a set of unique
        // ids is given
        boolean overrideDistributionLimit =
                (aggregateEntireRange
                        || (uniqueIdsOpt != null
                                && uniqueIdsOpt.isPresent()
                                && !uniqueIdsOpt.get().isEmpty())
                        || (compositeCountersOpt != null
                                && compositeCountersOpt.isPresent()
                                && !compositeCountersOpt.get().isEmpty()));

        // For the method that aggregates over all time, the distribution needs
        // to not be sent if we are adding any composite counters afterward.
        boolean overrideDistributionLimitInAggregator =
                (compositeCountersOpt != null
                        && compositeCountersOpt.isPresent()
                        && !compositeCountersOpt.get().isEmpty());


        // If we want to aggregate the entire range correctly we need to reteive
        // the distribution for each time slice regardless of whether the caller
        // requested it.  Without the distribution of each time slice, we cannot
        // accurately determine the aggregate unique count.  uniqueIds have no
        // bearing on this.
        boolean overrideExcludeDistribution = aggregateEntireRange;

        List<RolledUpCounter> rolledUpCounterResult =
                counterStorage.loadRolledUpCounters(
                        counterSubscription.getId(), fromDate, toDate,
                        counterTypesOpt,
                        (overrideExcludeDistribution
                                ? false : excludeDistribution),
                        (overrideDistributionLimit ? null : distributionLimit),
                        uniqueIdsOpt);

        if(Objects.equal(null, rolledUpCounterResult)
                || rolledUpCounterResult.isEmpty())
        {
            return Lists.newArrayList();
        }

        if(aggregateByMonth)
        {
            // TODO - Write the routine to aggregate the data by month
        }

        if (aggregateEntireRange) {
            RolledUpCounter aggregate =
                    aggregateEntireRange(rolledUpCounterResult,
                            excludeDistribution,
                            overrideDistributionLimitInAggregator
                                    ? null : distributionLimit);

            rolledUpCounterResult = ImmutableList.of(aggregate);
        }

        // Generate composite counters for each timeslice.  Performing this
        // opertion here allows us to operate on the normal list if we are not
        // aggregating or the aggregated list if we are.
        if (compositeCountersOpt != null
                && compositeCountersOpt.isPresent()
                && !compositeCountersOpt.get().isEmpty()) {
            addCompositeCounters(compositeCountersOpt.get(),
                    rolledUpCounterResult, distributionLimit);
        }

        return orderingRolledUpCounterByDate.immutableSortedCopy(rolledUpCounterResult);
    }

    /**
     * This method will take the list of time sliced counters loaded directly
     * from the datastore and combine them into a single aggregated rolled-up
     * counter.  This method could potentially be used to generate the monthly
     * rollups for permanent storage.
     * @param timeSlicedCounters
     * @param excludedDistribution
     * @param distributionLimit
     * @return
     */
    protected RolledUpCounter aggregateEntireRange(
            List<RolledUpCounter> timeSlicedCounters,
            boolean excludedDistribution, Optional<Integer> distributionLimit) {

        if (timeSlicedCounters == null || timeSlicedCounters.isEmpty()) {
            return null;
        }

        RolledUpCounter first = null;
        RolledUpCounter last = null;
        Table<String, String, RolledUpCounterData> resultSummary =
                HashBasedTable.create() ;

        // Walk all the time slices
        for (RolledUpCounter currCounter : timeSlicedCounters) {
            if (first == null) {
                first = currCounter;
            }

            last = currCounter;

            Table<String, String, RolledUpCounterData> currSummary =
                    currCounter.getCounterSummary();

            Map<String, Map<String, RolledUpCounterData>> currRowMap =
                    currSummary.rowMap();

            // Walk all the rows and columns and aggregate the data in each
            // cell.  This corresponds to walking all the categoryIds and
            // counterNames
            for (Map.Entry<String, Map<String, RolledUpCounterData>> rowEntry :
                    currRowMap.entrySet()) {
                String categoryId = rowEntry.getKey();

                for (Map.Entry<String, RolledUpCounterData> colEntry :
                        rowEntry.getValue().entrySet()) {
                    String counterName = colEntry.getKey();

                    RolledUpCounterData data = colEntry.getValue();

                    aggregateSingleRolledUpCounterDatum(data, categoryId,
                            counterName, resultSummary);
                }
            }
        }


        // Walk all the results in the aggregate and apply the post-processing
        // options
        for (RolledUpCounterData data : resultSummary.values()) {
            if (excludedDistribution) {
                data.truncateDistribution();
            }
            else if (distributionLimit != null
                    && distributionLimit.isPresent()) {
                data.applyDistributionLimit(distributionLimit.get());
            }
        }

        // This is not neccesary, but dispels the warning about first and last
        // possibly being null.  Really first and last can only be null if the
        // list is empty, and that was the first thing we checked for.
        if (first == null || last == null) {
            return null;
        }

        return new RolledUpCounter(first.getAppId(), first.getFromDate(),
                last.getToDate(), resultSummary);
    }

    /**
     * Aggregate a single rolled-up counter data element into a running
     * aggregate table of row=categoryId, column=counterName
     * @param datum single slice of rolled-up data
     * @param categoryId rowKey
     * @param counterName columnKey
     * @param aggregate running aggregation of all time-sliced counter data.  It
     *                  is into this table that the datum will be added
     */
    protected void aggregateSingleRolledUpCounterDatum(
            RolledUpCounterData datum, String categoryId, String counterName,
            Table<String, String, RolledUpCounterData> aggregate) {

        RolledUpCounterData aggregateData = aggregate.get(
                categoryId, counterName);

        // This might be the first time we've seen this counter & category combo
        if (aggregateData == null) {
            aggregateData = new RolledUpCounterData(counterName, 0,
                    (Map)Maps.newHashMap());
            aggregate.put(categoryId, counterName, aggregateData);
        }

        aggregateData.incrementCounter(datum.getTotalCount());

        for (Map.Entry<String, Integer> distributionEntry
                : datum.getDistribution().entrySet()) {
            String uniqueId = distributionEntry.getKey();
            int increment = distributionEntry.getValue();

            aggregateData.incrementDistributionCounter(uniqueId, increment);
        }
    }

    /**
     * iterate through all the time-sliced, rolled-up counters in the list and
     * generate new counter names within each of the
     * @param composites
     * @param timeSlicedCounters
     * @param distributionLimit
     */
    protected void addCompositeCounters(Set<CompositeCounter> composites,
            List<RolledUpCounter> timeSlicedCounters,
            Optional<Integer> distributionLimit) {

        if (timeSlicedCounters == null || timeSlicedCounters.isEmpty()) {
            return;
        }

        // Walk all the time slices
        for (RolledUpCounter currCounter : timeSlicedCounters) {
            Table<String, String, RolledUpCounterData> currSummary =
                    currCounter.getCounterSummary();

            Map<String, Map<String, RolledUpCounterData>> currRowMap =
                    currSummary.rowMap();

            // Walk all the rows and columns and aggregate the data in each
            // cell.  This corresponds to walking all the categoryIds and
            // counterNames
            for (Map.Entry<String, Map<String, RolledUpCounterData>> rowEntry :
                    currRowMap.entrySet()) {
                String categoryId = rowEntry.getKey();

                Map<String, RolledUpCounterData> countersInCategory
                        = rowEntry.getValue();
                Iterable<RolledUpCounterData> rolledUpComposites
                        = addCompositeCountersToCategory(composites,
                                countersInCategory, distributionLimit);

                for (RolledUpCounterData rolledUpComposite
                        : rolledUpComposites) {
                    currSummary.put(categoryId,
                            rolledUpComposite.getCounterName(),
                            rolledUpComposite);
                }
            }
        }
    }

    /**
     * Generate a composite counter for each given descriptor within the given
     * category, and return the group of generated composite counters as an
     * iterable
     * @param composites
     * @param countersInCategory
     * @param distributionLimit
     * @return
     */
    protected Iterable<RolledUpCounterData> addCompositeCountersToCategory(
            Set<CompositeCounter> composites,
            Map<String, RolledUpCounterData> countersInCategory,
            Optional<Integer> distributionLimit) {

        List<RolledUpCounterData> result
                = Lists.newArrayListWithExpectedSize(composites.size());

        for (CompositeCounter composite : composites) {

            RolledUpCounterData currResult =
                    new RolledUpCounterData(composite.getName(), 0,
                            (Map)Maps.newHashMap());

            result.add(currResult);

            for (int i = 0; i < composite.getCompositeWeights().length; i++) {
                int weight = composite.getCompositeWeights()[i];
                String componentName = composite.getCompositeEvents()[i];

                RolledUpCounterData component
                        = countersInCategory.get(componentName);

                if (component == null) {
                    continue;
                }

                currResult.incrementCounter(weight * component.getTotalCount());

                for (Map.Entry<String, Integer> uniqueCounter :
                        component.getDistribution().entrySet()) {
                    currResult.incrementDistributionCounter(
                            uniqueCounter.getKey(),
                            weight * uniqueCounter.getValue());
                }
            }

            if (distributionLimit != null
                    && distributionLimit.isPresent()) {
                currResult.applyDistributionLimit(distributionLimit.get());
            }
        }

        return result;
    }
}
