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
import com.ning.metrics.collector.processing.db.model.ChannelEvent;
import com.ning.metrics.collector.processing.db.model.ChannelEventData;
import com.ning.metrics.collector.processing.db.model.Subscription;
import com.ning.metrics.serialization.event.Event;
import com.ning.metrics.serialization.event.EventDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.mogwee.executors.LoggingExecutor;
import com.mogwee.executors.NamedThreadFactory;

import org.skife.config.TimeSpan;
import org.skife.jdbi.v2.IDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DBSpoolProcessor implements EventSpoolProcessor
{
    private static final Logger log = LoggerFactory.getLogger(DBSpoolProcessor.class);
    private final IDBI dbi;
    private final CollectorConfig config;
    private final SubscriptionStorage subscriptionStorage;
    private final ChannelEventStorage channelEventStorage;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String PROCESSOR_NAME = "DBWriter";
    private final BlockingQueue<ChannelEvent> eventStorageBuffer;
    private final ExecutorService executorService;
    private final TimeSpan executorShutdownTimeOut;
    
    @Inject
    public DBSpoolProcessor(final IDBI dbi, final CollectorConfig config, final SubscriptionStorage subscriptionStorage, final ChannelEventStorage channelEventStorage)
    {
        this.dbi = dbi;
        this.config = config;
        this.subscriptionStorage = subscriptionStorage;
        this.channelEventStorage = channelEventStorage;
        this.eventStorageBuffer = new ArrayBlockingQueue<ChannelEvent>(1000, false);
        this.executorShutdownTimeOut = config.getSpoolWriterExecutorShutdownTime();
        this.executorService = new LoggingExecutor(1, 1 , Long.MAX_VALUE, TimeUnit.DAYS, new ArrayBlockingQueue<Runnable>(2), new NamedThreadFactory("ChannelEvents-Storage-Threads"),new ThreadPoolExecutor.CallerRunsPolicy());
        this.executorService.submit(new ChannelInserter(this.executorService, this));
    } 

    @Override
    public void processEventFile(final String eventName, final SerializationType serializationType, final File file, final String outputPath) throws IOException
    {
        // File has Smile type of events
        EventDeserializer eventDeserializer = serializationType.getDeSerializer(new FileInputStream(file));
        
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
               ChannelEventData channelEventData = mapper.readValue(event.getData().toString(), ChannelEventData.class);
               
               for(String target : channelEventData.getTargets()){
                   Set<Subscription> subscriptions = subscriptionStorage.load(target);
                   for(Subscription subscription : subscriptions)
                   {
                       addToBuffer(event.getName(),new ChannelEvent(channelEventData, 
                                                           subscription.getChannel(), 
                                                           subscription.getId(), 
                                                           subscription.getMetadata()));
                   }                   
               }
            }            
        }
        
    }
    
    private void addToBuffer(String eventName, ChannelEvent channelEvent) {
        try {
            eventStorageBuffer.put(channelEvent);
        }
        catch (InterruptedException e) {
            log.warn(String.format("Could not add event %s to the buffer", eventName),e);
        }
    }
    
    public void flushChannelEventsToDB(){
        try {
            List<ChannelEvent> channelEventList = Lists.newArrayListWithCapacity(eventStorageBuffer.size());
            int count;
            boolean inserted = false;
            do {
                count = eventStorageBuffer.drainTo(channelEventList,1000);
                if(count > 0){
                    inserted = true;
                    channelEventStorage.insert(channelEventList);
                    log.debug(String.format("Inserted %d events successfully!", count));
                    channelEventList.clear();
                }
            }
            while (count > 0);
            
            if (!inserted) {
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        catch (Exception e) {
            log.warn("unexpected exception trying to insert events!",e);
        }
    }

    @Override
    public void close()
    {
        try {
            channelEventStorage.cleanUp();
            subscriptionStorage.cleanUp();  
        }
        finally{
            log.info("Shutting Down Executor Service for Channel Storage");
            executorService.shutdown();
            
            try {
                executorService.awaitTermination(executorShutdownTimeOut.getPeriod(), executorShutdownTimeOut.getUnit());
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            executorService.shutdownNow();
        }              
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
    
    private static class ChannelInserter implements Runnable{

        private final ExecutorService es;
        private final DBSpoolProcessor dbSpoolProcessor;
        
        public ChannelInserter(ExecutorService es,DBSpoolProcessor dbSpoolProcessor){
            this.es = es;
            this.dbSpoolProcessor = dbSpoolProcessor;
        }
        
        @Override
        public void run()
        {
            dbSpoolProcessor.flushChannelEventsToDB();
            es.submit(this);
            
        }
        
    }

}
