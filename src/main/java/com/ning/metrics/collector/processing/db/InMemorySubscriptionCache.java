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
import com.ning.metrics.collector.processing.db.model.Subscription;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class InMemorySubscriptionCache implements SubscriptionCache
{
    final Cache<String, Set<Subscription>> cache;
    
    @Inject
    public InMemorySubscriptionCache(CollectorConfig config){
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(24, TimeUnit.HOURS)
                .build();
    }

    @Override
    public Set<Subscription> loadSubscriptions(String target)
    {
        return cache.getIfPresent(target);
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

}
