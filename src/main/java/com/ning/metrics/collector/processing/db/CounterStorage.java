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

import com.google.common.collect.Multimap;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;

import org.joda.time.DateTime;

import java.util.List;

public interface CounterStorage
{
    public Long createCounterSubscription(final CounterSubscription counterSubscription);
    public CounterSubscription loadCounterSubscription(final String appId);
    
    public void insertDailyMetrics(final Multimap<Long, CounterEventData> dailyCounters);
    public List<CounterEventData> loadCounterEventData(final Long subscriptionId, final DateTime createdDate);
    public boolean deleteDailyMetrics(final List<Long> dailyMetricsIds);
    
    public void insertOrUpdateRolledUpCounter(final Long subscriptionId, final RolledUpCounter rolledUpCounter, final DateTime createdDate);
    public RolledUpCounter loadRolledUpCounterById(final String id);
    public List<RolledUpCounter> loadRolledUpCounters(final Long subscriptionId, final DateTime fromDate, final DateTime toDate);
    
}
