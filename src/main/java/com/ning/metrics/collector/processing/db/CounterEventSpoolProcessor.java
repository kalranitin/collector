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

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.EventSpoolProcessor;
import com.ning.metrics.collector.processing.SerializationType;
import com.ning.metrics.collector.processing.db.model.CounterEvent;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;
import com.ning.metrics.collector.processing.quartz.CounterEventCleanUpJob;
import com.ning.metrics.collector.processing.quartz.CounterEventScannerJob;
import com.ning.metrics.serialization.event.Event;
import com.ning.metrics.serialization.event.EventDeserializer;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class CounterEventSpoolProcessor implements EventSpoolProcessor
{
    private static final Logger log = LoggerFactory.getLogger(CounterEventSpoolProcessor.class);
    private final CollectorConfig config;
    private final CounterStorage counterStorage;
    private final ObjectMapper mapper;
    private static final String PROCESSOR_NAME = "CounterEventDBWriter";
    private final CounterEventCacheProcessor counterEventCacheProcessor;
    private final Scheduler quartzScheduler;
    private final AtomicBoolean isCronJobScheduled = new AtomicBoolean(false);
    private final AtomicBoolean isCleanupCronJobScheduled = new AtomicBoolean(false);

    @Inject
    public CounterEventSpoolProcessor(final CollectorConfig config, final CounterStorage counterStorage, final Scheduler quartzScheduler, final CounterEventCacheProcessor counterEventCacheProcessor, final ObjectMapper mapper) throws SchedulerException
    {
        this.config = config;
        this.counterStorage = counterStorage;
        this.counterEventCacheProcessor = counterEventCacheProcessor;
        this.mapper = mapper;
        
        this.quartzScheduler = quartzScheduler;
        if(!quartzScheduler.isStarted())
        {
            quartzScheduler.start();
            scheduleCounterEventRollUpCronJob();
            scheduleRollupEventCleanupCronJob();
        }
    } 

    @Override
    public void processEventFile(final String eventName, final SerializationType serializationType, final File file, final String outputPath) throws IOException
    {
        // File has Smile type of events
        EventDeserializer eventDeserializer = serializationType.getDeSerializer(new FileInputStream(file));
        boolean counterEventsProcessed = false;
        
        /*Add all eligible counter events to the buffer which would be drained periodically based on the size*/
        while(eventDeserializer.hasNextEvent())
        {
            Event event = eventDeserializer.getNextEvent();
            log.debug(String.format("Recieved DB Event to store with name as %s ",event.getName()));
            
            if(DBStorageTypes.COUNTER_EVENT.getDbStorageType().equalsIgnoreCase(event.getName()))
            {
               log.debug(String.format("DB Event body to store is %s",event.getData()));
               
               CounterEvent counterEvent = mapper.readValue(event.getData().toString(), CounterEvent.class);
               
               if(Strings.isNullOrEmpty(counterEvent.getAppId()) || Objects.equal(null, counterEvent.getCounterEvents()) || counterEvent.getCounterEvents().isEmpty())
               {
                   continue;
               }
               
               final CounterSubscription counterSubscription = counterStorage.loadCounterSubscription(counterEvent.getAppId());
               if(Objects.equal(null, counterSubscription))
               {
                   continue;
               }
               
               for(CounterEventData counterEventData : counterEvent.getCounterEvents())
               {   
                   this.counterEventCacheProcessor.addCounterEventData(counterSubscription.getId(), counterEventData);
               }
               
               counterEventsProcessed = true;
            }
        }
        
        if(counterEventsProcessed)
        {
            this.counterEventCacheProcessor.processRemainingCounters();
            
            if (!isCronJobScheduled.get()) {
                
                try {
                    scheduleCounterEventRollUpCronJob();
                }
                catch (SchedulerException e) {
                   log.error("Exception occurred while scheduling cron job for counter roll ups.",e);
                }
            }
            
        }
        
    }
    

    @Override
    public void close()
    {
        counterEventCacheProcessor.cleanUp();
        counterStorage.cleanUp();   
        
        log.info("Shutting Down Quartz Scheduler");
        try {
            if(!quartzScheduler.isShutdown())
            {
                quartzScheduler.shutdown(true);
            }
            
        }
        catch (SchedulerException e) {
            log.error("Unexpected error while shutting down Quartz Scheduler!",e);
        }
        log.info("Quartz Scheduler shutdown success");
    }
    
    private void scheduleCounterEventRollUpCronJob() throws SchedulerException
    {
        if(this.quartzScheduler.isStarted() && !isCronJobScheduled.get())
        {
            final JobKey jobKey = new JobKey("counterProcessorCronJob", "counterProcessorCronJobGroup");
            
            if(!this.quartzScheduler.checkExists(jobKey))
            {
                final CronTrigger cronTrigger = newTrigger()
                        .withIdentity("counterProcessorCronTrigger", "counterProcessorCronTriggerGroup")
                        .withSchedule(CronScheduleBuilder.cronSchedule(config.getCounterRollUpProcessorCronExpression()).withMisfireHandlingInstructionDoNothing())
                        .build();
                
                quartzScheduler.scheduleJob(newJob(CounterEventScannerJob.class).withIdentity(jobKey).build()
                    ,cronTrigger);
            }
            
            isCronJobScheduled.set(true);
            
        }
    }
    
    private void scheduleRollupEventCleanupCronJob() throws SchedulerException
    {
        if(this.quartzScheduler.isStarted() && !isCleanupCronJobScheduled.get())
        {
            final JobKey jobKey = new JobKey("rolledCountersCleanupCronJob", "rolledCountersCleanupCronJobGroup");
            
            if(!this.quartzScheduler.checkExists(jobKey))
            {
                final CronTrigger cronTrigger = newTrigger()
                        .withIdentity("rolledCountersCleanupCronTrigger", "rolledCountersCleanupCronTriggerGroup")
                        .withSchedule(CronScheduleBuilder.cronSchedule(config.getRolledUpCounterCleanupCronExpression()).withMisfireHandlingInstructionDoNothing())
                        .build();
                
                quartzScheduler.scheduleJob(newJob(CounterEventCleanUpJob.class).withIdentity(jobKey).build()
                    ,cronTrigger);
            }
            
            isCleanupCronJobScheduled.set(true);
            
        }
    }

    @Override
    public String getProcessorName()
    {
        return PROCESSOR_NAME;
    }
    
}