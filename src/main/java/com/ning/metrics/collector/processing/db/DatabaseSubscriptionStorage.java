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

import com.ning.metrics.collector.processing.db.model.FeedEventMetaData;
import com.ning.metrics.collector.processing.db.model.Subscription;
import com.ning.metrics.collector.processing.db.util.InClauseExpander;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.LongMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DatabaseSubscriptionStorage implements SubscriptionStorage
{
    public static final Splitter WHITESPACE_SPLITTER = Splitter.on(" ");
    public static final Joiner WHITESPACE_JOINER = Joiner.on(" ");

    private static final ObjectMapper mapper = new ObjectMapper();

    private final IDBI dbi;
    private final SubscriptionCache subscriptionCache;
    
    @Inject
    public DatabaseSubscriptionStorage(final IDBI dbi, SubscriptionCache subscriptionCache)
    {
        this.dbi = dbi;
        this.subscriptionCache = subscriptionCache;
    }

    @Override
    public Long insert(final Subscription subscription)
    {
        Long result = dbi.withHandle(new HandleCallback<Long>()
        {
            @Override
            public Long withHandle(Handle handle) throws Exception
            {
                return handle.createStatement("insert into subscriptions (topic, metadata, channel) values (:topic, :metadata, :channel)")
                             .bind("topic", subscription.getTopic())
                             .bind("metadata", mapper.writeValueAsString(subscription.getMetadata()))
                             .bind("channel", subscription.getChannel())
                             .executeAndReturnGeneratedKeys(LongMapper.FIRST)
                             .first();
            }
        });
        
        if(!Strings.isNullOrEmpty(subscription.getMetadata().getFeed()))
        {
            subscriptionCache.removeFeedSubscriptions(subscription.getMetadata().getFeed());            
        }
        if(!Strings.isNullOrEmpty(subscription.getTopic()))
        {
            subscriptionCache.removeTopicSubscriptions(subscription.getTopic());
        }
        
        return result;
    }

    @Override
    public Set<Subscription> loadByTopic(final String topicQuery)
    {
        final Set<String> topicSubQueries = decomposeTopicQuery(topicQuery);
        
        final Map<String, Optional<Subscription>> cachedResults = 
                subscriptionCache.loadTopicSubscriptions(topicSubQueries);
        
        final Set<Subscription> result = new HashSet<Subscription>();
        
        // Iterate through the results from the cache, and remove any topics
        // that were found from the list of topics left to query, and add
        // the non-null subscriptions to the list of results
        for (String topic : cachedResults.keySet()) {
            if(cachedResults.get(topic).isPresent())
            {
                result.add(cachedResults.get(topic).get());
                topicSubQueries.remove(topic);
            }       
        }
        
        // all topics that are found in the cache will be removed, so if no
        // topic subqueries are left, we are done
        if(!topicSubQueries.isEmpty())
        {
        
            Collection<Subscription> dbResults = dbi.withHandle(
                    new HandleCallback<Collection<Subscription>>()
            {
                @Override
                public Collection<Subscription> withHandle(Handle handle) 
                        throws Exception
                {

                    final Set<Subscription> dbResult = new HashSet<Subscription>();
                    
                    InClauseExpander in = new InClauseExpander(topicSubQueries);
                    
                    Iterator<Subscription> subscriptionsIter =  
                            handle.createQuery(
                                    "select id, metadata, channel, topic from subscriptions where topic in (" + in.getExpansion() + ")")
                                    .bindNamedArgumentFinder(in)
                                    .map(new SubscriptionMapper())
                                    .iterator();
                    
                    
                    if(subscriptionsIter != null)
                    {
                        while(subscriptionsIter.hasNext())
                        {
                            Subscription dbSubscription = subscriptionsIter.next();
                            dbResult.add(dbSubscription);
                            
                            subscriptionCache.addTopicSubscriptions(dbSubscription.getTopic(), Optional.of(dbSubscription));
                            topicSubQueries.remove(dbSubscription.getTopic());
                        }
                    }

                    return dbResult;
                }
            });
            
            
            // Add the database results to the results from the cache
            result.addAll(dbResults);
            
            // Add empty subscriptions to the cache
            if(!topicSubQueries.isEmpty())
            {
                subscriptionCache.addEmptyTopicSubscriptions(topicSubQueries);
            }
        }
        
        return ImmutableSet.copyOf(result);
    }
    
    /**
     * Break down the given space-delimited topic string of the form "a b c" 
     * into a list of the subqueries it contains, {"a", "a b", "a b c"}
     * @param topicQuery
     * @return 
     */
    private Set<String> decomposeTopicQuery(String topicQuery) {
        Set<String> result = new HashSet<String>();
        
        String last = null;
        
        for(String topic : WHITESPACE_SPLITTER.split(topicQuery)) {
            last = (last == null) 
                    ? topic 
                    : WHITESPACE_JOINER.join(last, topic);
            result.add(last);
        }
        
        return result;
    }
    
    public Set<Subscription> loadByFeed(final String feed)
    {
        Set<Subscription> subscriptions = subscriptionCache.loadFeedSubscriptions(feed);
        if(subscriptions != null && !subscriptions.isEmpty())
        {
            return subscriptions;
        }
        
        return dbi.withHandle(new HandleCallback<Set<Subscription>>()
        {
            @Override
            public Set<Subscription> withHandle(Handle handle) throws Exception
            {
                FeedEventMetaData metadata = new FeedEventMetaData(feed);
                Set<Subscription> subscriptions =  ImmutableSet.copyOf(handle.createQuery("select id, metadata, channel, topic from subscriptions where metadata = :metadata")
                                                 .bind("metadata",mapper.writeValueAsString(metadata))
                                                 .map(new SubscriptionMapper())
                                                 .list());
                
                subscriptionCache.addFeedSubscriptions(feed, subscriptions);
                
                return subscriptions;
            }
        });
    }
    
    @Override
    public Set<Subscription> loadByStartsWithTopic(final String topic)
    {
        return dbi.withHandle(new HandleCallback<Set<Subscription>>()
        {
            @Override
            public Set<Subscription> withHandle(Handle handle) throws Exception
            {
                Set<Subscription> subscriptions =  ImmutableSet.copyOf(handle.createQuery("select id, metadata, channel, topic from subscriptions where topic like :topic")
                                                 .bind("topic",topic+"%")
                                                 .map(new SubscriptionMapper())
                                                 .list());
                
                return subscriptions;
            }
        });
    }

    @Override
    public boolean deleteSubscriptionById(final Long id)
    {
        return dbi.withHandle(new HandleCallback<Boolean>()
        {
            @Override
            public Boolean withHandle(Handle handle) throws Exception
            {
                Subscription subscription = handle.createQuery("select id, metadata, channel, topic from subscriptions where id = :id")
                        .bind("id", id)
                        .map(new SubscriptionMapper())
                        .first();
                
                if(Objects.equal(null, subscription))
                {
                    return true;
                }
                else
                {
                    if(!Objects.equal(null, subscription.getMetadata()) && !Strings.isNullOrEmpty(subscription.getMetadata().getFeed()))
                    {
                        subscriptionCache.removeFeedSubscriptions(subscription.getMetadata().getFeed());
                    }
                    if(!Strings.isNullOrEmpty(subscription.getTopic()))
                    {
                        subscriptionCache.removeTopicSubscriptions(subscription.getTopic());
                    }
                    
                    return 1 == handle.createStatement("delete from subscriptions where id = :id")
                            .bind("id", id)
                            .execute();
                }
                
            }
        });
    }

    @Override
    public Subscription loadSubscriptionById(final Long id)
    {
        return dbi.withHandle(new HandleCallback<Subscription>()
        {
            @Override
            public Subscription withHandle(Handle handle) throws Exception
            {
                return handle.createQuery("select id, topic, metadata, channel from subscriptions where id = :id")
                             .bind("id", id)
                             .map(new SubscriptionMapper())
                             .first();
            }
        });
    }

    public static class SubscriptionMapper implements ResultSetMapper<Subscription>
    {

        private final Optional<String> topic;

        public SubscriptionMapper(String topic)
        {
            this.topic = Optional.of(topic);
        }

        public SubscriptionMapper()
        {
            this.topic = Optional.absent();
        }

        @Override
        public Subscription map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                FeedEventMetaData meta = mapper.readValue(r.getString("metadata"), FeedEventMetaData.class);
                return new Subscription(r.getLong("id"),
                    topic.or(new ResultSetStringSupplier(r, "topic")),
                                        meta,
                                        r.getString("channel"));

            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Error handling not implemented!", e);
            }
        }
    }

    private static class ResultSetStringSupplier implements Supplier<String>
    {

        private final ResultSet rs;

        private final String fieldName;

        ResultSetStringSupplier(ResultSet rs, String fieldName)
        {
            this.rs = rs;
            this.fieldName = fieldName;
        }

        @Override
        public String get()
        {
            try {
                return rs.getString(fieldName);
            }
            catch (SQLException e) {
                throw new UnsupportedOperationException("Not Yet Implemented!", e);
            }
        }
    }

    @Override
    public void cleanUp()
    {
        subscriptionCache.cleanUp();        
    }
}
