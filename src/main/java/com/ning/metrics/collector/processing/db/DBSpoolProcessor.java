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

import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.EventSpoolProcessor;
import com.ning.metrics.collector.processing.SerializationType;
import com.ning.metrics.collector.processing.db.model.Subscription;
import com.ning.metrics.serialization.event.Event;
import com.ning.metrics.serialization.event.EventDeserializer;
import com.ning.metrics.serialization.smile.SmileEnvelopeEventDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import org.skife.jdbi.v2.IDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class DBSpoolProcessor implements EventSpoolProcessor
{
    private static final Logger log = LoggerFactory.getLogger(DBSpoolProcessor.class);
    private final IDBI dbi;
    private final CollectorConfig config;
    private final SubscriptionStorage subscriptionStorage;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String PROCESSOR_NAME = "DBWriter";
    
    @Inject
    public DBSpoolProcessor(final IDBI dbi, final CollectorConfig config, final SubscriptionStorage subscriptionStorage)
    {
        this.dbi = dbi;
        this.config = config;
        this.subscriptionStorage = subscriptionStorage;
    }

    @Override
    public void processEventFile(final String eventName, final SerializationType serializationType, final File file, final String outputPath) throws IOException
    {
        // File has Smile type of events
        EventDeserializer eventDeserializer = new SmileEnvelopeEventDeserializer(new FileInputStream(file),false);
        
        /*This would handle insertion of Subscriptions and Channel Events. 
         * The subscriptions  would be stored as they come by, however for channel events
         * the storage would be done in bulk after the complete file is read, 
         * since channel events depend upon the subscriptions*/
        while(eventDeserializer.hasNextEvent())
        {
            Event event = eventDeserializer.getNextEvent();
            log.info(String.format("Recieved DB Event to store with name as %s ",event.getName()));
            log.info(String.format("DB Event body to store is %s",event.getData()));
            
            if(event.getName().equalsIgnoreCase(DBStorageTypes.CHANNEL_EVENT.getDbStorageType()))
            {
                // TODO Store events in batch
            }
            
        }
        
        
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String getProcessorName()
    {
        return PROCESSOR_NAME;
    }
    
    /*@Monitored(description = "Number of dropped events", monitoringType = {MonitoringType.VALUE, MonitoringType.RATE})
    public long getDroppedEvents()
    {
        long droppedEvents = 0;
        for (final EventQueueStats localStats : stats.values()) {
            droppedEvents += localStats.getDroppedEvents();
        }
        return droppedEvents;
    }*/

}
