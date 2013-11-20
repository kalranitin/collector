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
import java.util.Collection;

import org.skife.config.TimeSpan;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class InMemorySubscriptionCache implements SubscriptionCache
{
    final Cache<String, Subscription> subscriptionByTopicCache;
    final Cache<String, Set<Subscription>> subscriptionByFeedCache;
    final TimeSpan cacheExpiryTime;
    
    // This subscription is used to cache a known emptyResult for a given topic
    final Subscription knownEmptyResult;
    
    @Inject
    public InMemorySubscriptionCache(CollectorConfig config){
        this.cacheExpiryTime = config.getSubscriptionCacheTimeout();
        
        this.subscriptionByTopicCache = CacheBuilder.newBuilder()
                .maximumSize(config.getMaxSubscriptionCacheCount())
                .expireAfterAccess(cacheExpiryTime.getPeriod(),cacheExpiryTime.getUnit())
                .recordStats()
                .build();
        
        this.subscriptionByFeedCache = CacheBuilder.newBuilder()
                .maximumSize(config.getMaxSubscriptionCacheCount())
                .expireAfterAccess(cacheExpiryTime.getPeriod(),cacheExpiryTime.getUnit())
                .recordStats()
                .build();
        
        knownEmptyResult = new Subscription("placeholder", null, null);
    }

    /**
     * Loads Subscriptions for each of the topics in the given set.  If a cached
     * result is found for a topic in the set, then that topic is removed from
     * the original set.  This allows the caller to know the set of topics
     * that are not in the cache
     * @param topics
     * @return 
     */
    @Override
    public Set<Subscription> loadTopicSubscriptions(Set<String> topics)
    {
        Subscription subscription;
        String topic;
        Set<Subscription> result = new HashSet<Subscription>();
        
        Iterator<String> topicIt = topics.iterator();
        
        while (topicIt.hasNext()) {
            
            topic = topicIt.next();
            subscription = subscriptionByTopicCache.getIfPresent(topic);
            
            // If no subscription is found in the cache, there's nothing to do
            if (subscription == null) {
                continue;
            }
            
            // otherwise, we know the subscription for the given topic and we
            // can remove it from the original set 
            topicIt.remove();

            // If the stored subscription for the topic is our placeholder for
            // a known empty result, then we are done
            if (subscription == knownEmptyResult) {
                continue;
            }
            
            // otherwise, add the cached subscription to the result
            result.add(subscription);
        }
        
        return result;
    }

    /**
     * Add the given set of subscriptions to the cache based on their topics,
     * and add a known-empty-result placeholder to the cache for any topics that
     * were queried but no corresponding subscription was found
     * @param topicsQueried
     * @param subscriptions 
     */
    @Override
    public void addTopicSubscriptions(Set<String> topicsQueried, 
            Collection<Subscription> subscriptions)
    {
        String topic;
        
        // Add topics and subcsriptions to the cache as key-value pairs
        for (Subscription subscription : subscriptions) {
            topic = subscription.getTopic();
            topicsQueried.remove(topic);
            subscriptionByTopicCache.put(topic, subscription);
        }
       
        // Add known-empty-result placeholders for any of the queried topics 
        // that returned to subscription
        for(String emptyTopic : topicsQueried) {
            subscriptionByTopicCache.put(emptyTopic, knownEmptyResult);
        }
    }

    @Override
    public void removeTopicSubscriptions(String topic)
    {
        subscriptionByTopicCache.invalidate(topic);        
    }
    
    @Override
    public Set<Subscription> loadFeedSubscriptions(String feed)
    {
        Set<Subscription> subscriptions = subscriptionByFeedCache.getIfPresent(feed);
        return subscriptions == null?new HashSet<Subscription>():subscriptions;
    }

    @Override
    public void addFeedSubscriptions(String feed, Set<Subscription> subscriptions)
    {
        subscriptionByFeedCache.put(feed, subscriptions);
    }

    @Override
    public void removeFeedSubscriptions(String feed)
    {
        subscriptionByFeedCache.invalidate(feed);        
    }

    @Override
    public void cleanUp()
    {
        subscriptionByTopicCache.invalidateAll();
        subscriptionByFeedCache.invalidateAll();
        subscriptionByTopicCache.cleanUp();
        subscriptionByFeedCache.cleanUp();
    }
    
    @Monitored(description = "Number of topic subscriptions in buffer", monitoringType = {MonitoringType.VALUE})
    public long getTopicSubscriptionsInCache(){
        return subscriptionByTopicCache.size();
    }
    
    @Monitored(description = "The number of times Topic Cache lookup methods have returned a cached value", monitoringType = {MonitoringType.VALUE})
    public long getTopicSubscriptionsCacheHitCount(){
        return subscriptionByTopicCache.stats().hitCount();
    }
    
    @Monitored(description = "The ratio of topic cache requests which were hits", monitoringType = {MonitoringType.VALUE})
    public double getTopicSubscriptionsCacheHitRate(){
        return subscriptionByTopicCache.stats().hitRate();
    }
    
    @Monitored(description = "The total number of times that Topic Cache lookup methods attempted to load new values", monitoringType = {MonitoringType.VALUE})
    public long getTopicSubscriptionsCacheLoadCount(){
        return subscriptionByTopicCache.stats().loadCount();
    }
    
    @Monitored(description = "The number of times Topic Cache lookup methods have returned an uncached (newly loaded) value, or null", monitoringType = {MonitoringType.VALUE})
    public long getTopicSubscriptionsCacheMissCount(){
        return subscriptionByTopicCache.stats().missCount();
    }
    
    @Monitored(description = "The ratio of topic cache requests which were misses", monitoringType = {MonitoringType.VALUE})
    public double getTopicSubscriptionsCacheMissRate(){
        return subscriptionByTopicCache.stats().missRate();
    }
    
    @Monitored(description = "The number of times Topic Cache lookup methods have returned either a cached or uncached value", monitoringType = {MonitoringType.VALUE})
    public long getTopicSubscriptionsCacheRequestCount(){
        return subscriptionByTopicCache.stats().requestCount();
    }
    
    @Monitored(description = "Number of feed subscriptions in buffer", monitoringType = {MonitoringType.VALUE})
    public long getFeedSubscriptionsInCache(){
        return subscriptionByFeedCache.size();
    }
    
    @Monitored(description = "The number of times Feed Cache lookup methods have returned a cached value", monitoringType = {MonitoringType.VALUE})
    public long getFeedSubscriptionsCacheHitCount(){
        return subscriptionByFeedCache.stats().hitCount();
    }
    
    @Monitored(description = "The ratio of feed cache requests which were hits", monitoringType = {MonitoringType.VALUE})
    public double getFeedSubscriptionsCacheHitRate(){
        return subscriptionByFeedCache.stats().hitRate();
    }
    
    @Monitored(description = "The total number of times that Feed Cache lookup methods attempted to load new values", monitoringType = {MonitoringType.VALUE})
    public long getFeedSubscriptionsCacheLoadCount(){
        return subscriptionByFeedCache.stats().loadCount();
    }
    
    @Monitored(description = "The number of times Feed Cache lookup methods have returned an uncached (newly loaded) value, or null", monitoringType = {MonitoringType.VALUE})
    public long getFeedSubscriptionsCacheMissCount(){
        return subscriptionByFeedCache.stats().missCount();
    }
    
    @Monitored(description = "The ratio of feed cache requests which were misses", monitoringType = {MonitoringType.VALUE})
    public double getFeedSubscriptionsCacheMissRate(){
        return subscriptionByFeedCache.stats().missRate();
    }
    
    @Monitored(description = "The number of times Feed Cache lookup methods have returned either a cached or uncached value", monitoringType = {MonitoringType.VALUE})
    public long getFeedSubscriptionsCacheRequestCount(){
        return subscriptionByFeedCache.stats().requestCount();
    }

}
