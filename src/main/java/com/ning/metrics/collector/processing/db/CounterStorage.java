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
package com.ning.metrics.collector.processing.db;

import com.google.common.base.Optional;
import com.google.common.collect.Multimap;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;
import java.util.List;
import java.util.Set;
import org.joda.time.DateTime;

public interface CounterStorage
{
    public Long createCounterSubscription(final CounterSubscription counterSubscription);
    public Long updateCounterSubscription(final CounterSubscription counterSubscription, final Long id);
    public CounterSubscription loadCounterSubscription(final String appId);
    public CounterSubscription loadCounterSubscriptionById(final Long subscriptionId);

    public void insertDailyMetrics(final Multimap<Long, CounterEventData> dailyCounters);
    public List<CounterEventData> loadDailyMetrics(final Long subscriptionId, final DateTime toDateTime, final Integer limit, final Integer offset);
    public List<CounterEventData> loadGroupedDailyMetrics(final Long subscriptionId, final DateTime toDateTime);
    public boolean deleteDailyMetrics(final Long subscriptionId, final DateTime toDateTime);
    public List<Long> getSubscritionIdsFromDailyMetrics();

    public String insertOrUpdateRolledUpCounter(final Long subscriptionId, final RolledUpCounter rolledUpCounter);
    public RolledUpCounter loadRolledUpCounterById(final String id, final boolean excludeDistribution, final Optional<Integer> distributionLimit);
    public List<RolledUpCounter> loadRolledUpCounters(final Long subscriptionId,
            final DateTime fromDate, final DateTime toDate,
            final Optional<Set<String>> fetchCounterNames,
            final boolean excludeDistribution,
            final Optional<Integer> distributionLimit,
            final Optional<Set<String>> unqiueIds);
    public int cleanExpiredRolledUpCounterEvents(final DateTime toDateTime);

    public void cleanUp();

}
