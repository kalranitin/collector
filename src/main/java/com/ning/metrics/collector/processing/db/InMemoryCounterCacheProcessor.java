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
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import com.google.inject.Inject;
import com.mogwee.executors.LoggingExecutor;
import com.mogwee.executors.NamedThreadFactory;
import com.ning.arecibo.jmx.Monitored;
import com.ning.arecibo.jmx.MonitoringType;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;

import org.skife.config.TimeSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class InMemoryCounterCacheProcessor implements CounterEventCacheProcessor
{
    private static final Logger log = LoggerFactory.getLogger(InMemoryCounterCacheProcessor.class);
    final Cache<String, Optional<CounterSubscription>> counterSubscriptionByAppId;
    final Cache<Long, Queue<CounterEventData>> counterEventsBySubscriptionId;
    private final ExecutorService executorService;
    private final TimeSpan executorShutdownTimeOut;
    final TimeSpan cacheExpiryTime;
    final TimeSpan counterEventDBFlushTime;
    
    @Inject
    public InMemoryCounterCacheProcessor(final CollectorConfig config, final CounterStorage counterStorage)
    {
        this.cacheExpiryTime = config.getSubscriptionCacheTimeout();
        this.executorShutdownTimeOut = config.getSpoolWriterExecutorShutdownTime();
        this.counterEventDBFlushTime = config.getCounterEventDBFlushTime();
        
        this.counterSubscriptionByAppId = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(cacheExpiryTime.getPeriod(),cacheExpiryTime.getUnit())
                .recordStats()
                .build();
        
        this.executorService = new LoggingExecutor(1, 1 , Long.MAX_VALUE, TimeUnit.DAYS, new ArrayBlockingQueue<Runnable>(2), new NamedThreadFactory("CounterEvents-Storage-Threads"),new ThreadPoolExecutor.CallerRunsPolicy());
        
        
        this.counterEventsBySubscriptionId = CacheBuilder.newBuilder()
                .expireAfterAccess(counterEventDBFlushTime.getPeriod(),counterEventDBFlushTime.getUnit())
                .maximumWeight(config.getMaxCounterEventFlushCacheCount())                
                .weigher(new Weigher<Long, Queue<CounterEventData>>() 
                    {
                        @Override
                        public int weigh(Long key, Queue<CounterEventData> value)
                        {
                           return value.size();
                        }
                    })
                .removalListener(RemovalListeners.asynchronous(new RemovalListener<Long, Queue<CounterEventData>>() {

                    @Override
                    public void onRemoval(RemovalNotification<Long, Queue<CounterEventData>> removalNotification)
                    {
                        if(!Objects.equal(removalNotification.getCause(), RemovalCause.REPLACED) && !Objects.equal(null, removalNotification.getValue()) && !removalNotification.getValue().isEmpty())
                        {   
                            Multimap<Long, CounterEventData> multimap = ArrayListMultimap.create();
                            List<CounterEventData> counterEventDataList = Lists.newArrayList(removalNotification.getValue());
                            removalNotification.getValue().clear();
                            Map<String,CounterEventData> groupMap = new ConcurrentHashMap<String, CounterEventData>();
                            
                            for(CounterEventData counterEventData : counterEventDataList)
                            {
                                CounterEventData groupedData = groupMap.get(counterEventData.getUniqueIdentifier()+counterEventData.getFormattedDate());
                                
                                if(Objects.equal(null, groupedData))
                                {
                                    groupMap.put(counterEventData.getUniqueIdentifier()+counterEventData.getFormattedDate(), counterEventData);
                                    continue;
                                }
                                
                                groupedData.mergeCounters(counterEventData.getCounters());
                                groupMap.put(counterEventData.getUniqueIdentifier()+counterEventData.getFormattedDate(), groupedData);
                            }
                            
                            multimap.putAll(removalNotification.getKey(), groupMap.values());
                            
                            counterStorage.insertDailyMetrics(multimap);
                            
                        }
                    }},  this.executorService))
                .recordStats()
                .build();
        
    }
    
    @Override
    public Optional<CounterSubscription> getCounterSubscription(final String appId)
    {
        Optional<CounterSubscription> counterSubscription =  this.counterSubscriptionByAppId.getIfPresent(appId);
        return counterSubscription == null?Optional.<CounterSubscription>absent():counterSubscription;
    }
    
    @Override
    public void addCounterSubscription(final String appId, final Optional<CounterSubscription> counterSubscription)
    {
        this.counterSubscriptionByAppId.put(appId, counterSubscription);
    }
    
    @Override
    public void addCounterEventData(final Long subscriptionId, final CounterEventData counterEventData)
    {
        Queue<CounterEventData> counterEventDataQueue = this.counterEventsBySubscriptionId.getIfPresent(subscriptionId);
        if(Objects.equal(null, counterEventDataQueue))
        {
            counterEventDataQueue = Queues.newConcurrentLinkedQueue();
        }
        
        counterEventDataQueue.offer(counterEventData);
        this.counterEventsBySubscriptionId.put(subscriptionId, counterEventDataQueue);
    }
    
    @Override
    public void cleanUp()
    {
        this.counterEventsBySubscriptionId.cleanUp();
        this.counterSubscriptionByAppId.cleanUp();
        this.counterEventsBySubscriptionId.invalidateAll();
        this.counterSubscriptionByAppId.invalidateAll();
        
        log.info("Shutting Down Executor Service for Feed Event Storage");
        executorService.shutdown();
        
        try {
            executorService.awaitTermination(executorShutdownTimeOut.getPeriod(), executorShutdownTimeOut.getUnit());
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        executorService.shutdownNow();
    }
    
    @Monitored(description = "Number of counter subscriptions in buffer", monitoringType = {MonitoringType.VALUE})
    public long getTopicSubscriptionsInCache(){
        return counterSubscriptionByAppId.size();
    }
    
    @Monitored(description = "The number of times Counter Subscription lookup methods have returned a cached value", monitoringType = {MonitoringType.VALUE})
    public long getTopicSubscriptionsCacheHitCount(){
        return counterSubscriptionByAppId.stats().hitCount();
    }
    
    @Monitored(description = "The ratio of counter subscription cache requests which were hits", monitoringType = {MonitoringType.VALUE})
    public double getTopicSubscriptionsCacheHitRate(){
        return counterSubscriptionByAppId.stats().hitRate();
    }
    
    @Monitored(description = "The total number of times that Counter Subscription lookup methods attempted to load new values", monitoringType = {MonitoringType.VALUE})
    public long getTopicSubscriptionsCacheLoadCount(){
        return counterSubscriptionByAppId.stats().loadCount();
    }
    
    @Monitored(description = "The number of times Counter Subscription lookup methods have returned an uncached (newly loaded) value, or null", monitoringType = {MonitoringType.VALUE})
    public long getTopicSubscriptionsCacheMissCount(){
        return counterSubscriptionByAppId.stats().missCount();
    }
    
    @Monitored(description = "The ratio of Counter Subscription requests which were misses", monitoringType = {MonitoringType.VALUE})
    public double getTopicSubscriptionsCacheMissRate(){
        return counterSubscriptionByAppId.stats().missRate();
    }
    
    @Monitored(description = "The number of times Counter Subscription lookup methods have returned either a cached or uncached value", monitoringType = {MonitoringType.VALUE})
    public long getTopicSubscriptionsCacheRequestCount(){
        return counterSubscriptionByAppId.stats().requestCount();
    }
    
    @Monitored(description = "Number of Counter Events in buffer", monitoringType = {MonitoringType.VALUE})
    public long getFeedSubscriptionsInCache(){
        return counterEventsBySubscriptionId.size();
    }
    
    @Monitored(description = "The number of times Counter Events lookup methods have returned a cached value", monitoringType = {MonitoringType.VALUE})
    public long getFeedSubscriptionsCacheHitCount(){
        return counterEventsBySubscriptionId.stats().hitCount();
    }
    
    @Monitored(description = "The ratio of Counter Events requests which were hits", monitoringType = {MonitoringType.VALUE})
    public double getFeedSubscriptionsCacheHitRate(){
        return counterEventsBySubscriptionId.stats().hitRate();
    }
    
    @Monitored(description = "The total number of times that Counter Events lookup methods attempted to load new values", monitoringType = {MonitoringType.VALUE})
    public long getFeedSubscriptionsCacheLoadCount(){
        return counterEventsBySubscriptionId.stats().loadCount();
    }
    
    @Monitored(description = "The number of times Counter Events lookup methods have returned an uncached (newly loaded) value, or null", monitoringType = {MonitoringType.VALUE})
    public long getFeedSubscriptionsCacheMissCount(){
        return counterEventsBySubscriptionId.stats().missCount();
    }
    
    @Monitored(description = "The ratio of Counter Events requests which were misses", monitoringType = {MonitoringType.VALUE})
    public double getFeedSubscriptionsCacheMissRate(){
        return counterEventsBySubscriptionId.stats().missRate();
    }
    
    @Monitored(description = "The number of times Counter Events lookup methods have returned either a cached or uncached value", monitoringType = {MonitoringType.VALUE})
    public long getFeedSubscriptionsCacheRequestCount(){
        return counterEventsBySubscriptionId.stats().requestCount();
    }

}
