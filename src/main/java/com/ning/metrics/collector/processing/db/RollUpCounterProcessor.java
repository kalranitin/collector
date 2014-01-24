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

import java.util.List;
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
        DateTime toDateTime = new DateTime(DateTimeZone.UTC);
        
        // Load daily counters stored for the respective subscription limiting to now()
        List<CounterEventData> dailyCounterList = counterStorage.loadGroupedDailyMetrics(counterSubscription.getId(), toDateTime);
        Map<String,RolledUpCounter> rolledUpCounterMap = new ConcurrentHashMap<String, RolledUpCounter>();
        
        if(!Objects.equal(null, dailyCounterList) && !dailyCounterList.isEmpty())
        {
            for(CounterEventData counterEventData : dailyCounterList)
            {
                RolledUpCounter rolledUpCounter = rolledUpCounterMap.get(counterSubscription.getAppId()+counterEventData.getFormattedDate());
                if(Objects.equal(null, rolledUpCounter))
                {
                    rolledUpCounter = counterStorage.loadRolledUpCounterById(counterSubscription.getAppId()+counterEventData.getFormattedDate());
                    if(Objects.equal(null, rolledUpCounter))
                    {
                        rolledUpCounter = new RolledUpCounter(counterSubscription.getAppId(), counterEventData.getCreatedDate(), counterEventData.getCreatedDate());
                    }
                }
                
                rolledUpCounter.updateRolledUpCounterData(counterEventData, counterSubscription.getIdentifierDistribution().get(counterEventData.getIdentifierCategory()));
                rolledUpCounterMap.put(counterSubscription.getAppId()+counterEventData.getFormattedDate(), rolledUpCounter);
                
            }
            
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
    }

}
