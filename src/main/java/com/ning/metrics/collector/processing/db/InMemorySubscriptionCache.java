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

import com.ning.arecibo.jmx.Monitored;
import com.ning.arecibo.jmx.MonitoringType;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.db.model.Subscription;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;

import org.skife.config.TimeSpan;

import java.util.HashSet;
import java.util.Set;

public class InMemorySubscriptionCache implements SubscriptionCache
{
    final Cache<String, Set<Subscription>> cache;
    final TimeSpan cacheExpiryTime;
    
    @Inject
    public InMemorySubscriptionCache(CollectorConfig config){
        this.cacheExpiryTime = config.getSubscriptionCacheTimeout();
        
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(config.getMaxSubscriptionCacheCount())
                .expireAfterAccess(cacheExpiryTime.getPeriod(),cacheExpiryTime.getUnit())
                .recordStats()
                .build();
    }

    @Override
    public Set<Subscription> loadSubscriptions(String target)
    {
        Set<Subscription> subscriptions = cache.getIfPresent(target);
        return subscriptions == null?new HashSet<Subscription>():subscriptions;
    }

    @Override
    public void addSubscriptions(String target, Set<Subscription> subscriptions)
    {
        cache.put(target, subscriptions);
    }

    @Override
    public void removeSubscriptions(String target)
    {
        cache.invalidate(target);        
    }

    @Override
    public void cleanUp()
    {
        cache.invalidateAll();
        cache.cleanUp();
    }
    
    @Monitored(description = "Number of subscriptions in buffer", monitoringType = {MonitoringType.VALUE})
    public long getSubscriptionsInCache(){
        return cache.size();
    }
    
    @Monitored(description = "The number of times Cache lookup methods have returned a cached value", monitoringType = {MonitoringType.VALUE})
    public long getSubscriptionsCacheHitCount(){
        return cache.stats().hitCount();
    }
    
    @Monitored(description = "The ratio of cache requests which were hits", monitoringType = {MonitoringType.VALUE})
    public double getSubscriptionsCacheHitRate(){
        return cache.stats().hitRate();
    }
    
    @Monitored(description = "The total number of times that Cache lookup methods attempted to load new values", monitoringType = {MonitoringType.VALUE})
    public long getSubscriptionsCacheLoadCount(){
        return cache.stats().loadCount();
    }
    
    @Monitored(description = "The number of times Cache lookup methods have returned an uncached (newly loaded) value, or null", monitoringType = {MonitoringType.VALUE})
    public long getSubscriptionsCacheMissCount(){
        return cache.stats().missCount();
    }
    
    @Monitored(description = "The ratio of cache requests which were misses", monitoringType = {MonitoringType.VALUE})
    public double getSubscriptionsCacheMissRate(){
        return cache.stats().missRate();
    }
    
    @Monitored(description = "The number of times Cache lookup methods have returned either a cached or uncached value", monitoringType = {MonitoringType.VALUE})
    public long getSubscriptionsCacheRequestCount(){
        return cache.stats().requestCount();
    }

}
