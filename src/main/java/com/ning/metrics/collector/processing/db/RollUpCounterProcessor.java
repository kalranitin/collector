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
package com.ning.metrics.collector.processing.db;

import com.google.common.base.Objects;
import com.google.inject.Inject;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RollUpCounterProcessor
{
    private static final Logger log = LoggerFactory.getLogger(RollUpCounterProcessor.class);
    private final CollectorConfig config;
    private final DatabaseCounterStorage counterStorage;
    
    @Inject
    public RollUpCounterProcessor(final DatabaseCounterStorage counterStorage, final CollectorConfig config)
    {
        this.counterStorage = counterStorage;
        this.config = config;
    }
    
    public void rollUpDailyCounters(final CounterSubscription counterSubscription){
        final DateTime toDateTime = new DateTime(DateTimeZone.UTC);
        final Integer recordFetchLimit = config.getMaxCounterEventFetchCount();
        Integer recordOffSetCounter = 0;
        boolean fetchMoreRecords = true;
        
        Map<String,RolledUpCounter> rolledUpCounterMap = new ConcurrentHashMap<String, RolledUpCounter>();
        
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
            
            // increment recordOffset to fetch next getMaxCounterEventFetchCount
            recordOffSetCounter += recordFetchLimit;
            
            while(dailyCounterList.hasNext())
            {
                final CounterEventData counterEventData = dailyCounterList.next();
                final String rolledUpCounterKey = counterSubscription.getAppId()+counterEventData.getFormattedDate();
                
                RolledUpCounter rolledUpCounter = rolledUpCounterMap.get(rolledUpCounterKey);
                
                if(Objects.equal(null, rolledUpCounter))
                {
                    rolledUpCounter = counterStorage.loadRolledUpCounterById(rolledUpCounterKey);
                    if(Objects.equal(null, rolledUpCounter))
                    {
                        rolledUpCounter = new RolledUpCounter(counterSubscription.getAppId(), counterEventData.getCreatedDate(), counterEventData.getCreatedDate());
                    }
                }
                
                rolledUpCounter.updateRolledUpCounterData(counterEventData, counterSubscription.getIdentifierDistribution().get(counterEventData.getIdentifierCategory()));
                rolledUpCounterMap.put(rolledUpCounterKey, rolledUpCounter);
            }
            
        }
        
        if(!rolledUpCounterMap.isEmpty())
        {
            for(RolledUpCounter rolledUpCounter : rolledUpCounterMap.values())
            {
                // Evaluate Uniqes for rolled up counters
                rolledUpCounter.evaluateUniques();
                
                //Save
                counterStorage.insertOrUpdateRolledUpCounter(counterSubscription.getId(), rolledUpCounter);
            }
            
            // Delete daily metrics which have been accounted for the roll up. 
            // There may be more additions done since this process started which is why the evaluation time is passed on.
            counterStorage.deleteDailyMetrics(counterSubscription.getId(), toDateTime);
        }   
        
        log.info(String.format("Roll up process for Counter Subscription [%s] completed successfully!", counterSubscription.getAppId()));
        
    }

}
