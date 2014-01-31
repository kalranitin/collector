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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.EventSpoolProcessor;
import com.ning.metrics.collector.processing.SerializationType;
import com.ning.metrics.collector.processing.db.model.CounterEvent;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;
import com.ning.metrics.serialization.event.Event;
import com.ning.metrics.serialization.event.EventDeserializer;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class CounterEventSpoolProcessor implements EventSpoolProcessor
{
    private static final Logger log = LoggerFactory.getLogger(CounterEventSpoolProcessor.class);
    private final CollectorConfig config;
    private final CounterStorage counterStorage;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String PROCESSOR_NAME = "CounterEventDBWriter";
    private final CounterEventCacheProcessor counterEventCacheProcessor;

    @Inject
    public CounterEventSpoolProcessor(final CollectorConfig config, final CounterStorage counterStorage, final Scheduler quartzScheduler, final CounterEventCacheProcessor counterEventCacheProcessor) throws SchedulerException
    {
        this.config = config;
        this.counterStorage = counterStorage;
        this.counterEventCacheProcessor = counterEventCacheProcessor;
        
        mapper.registerModule(new JodaModule());
        mapper.registerModule(new GuavaModule());
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
        }
        
    }
    

    @Override
    public void close()
    {
        counterEventCacheProcessor.cleanUp();
        counterStorage.cleanUp();             
    }

    @Override
    public String getProcessorName()
    {
        return PROCESSOR_NAME;
    }
    
}