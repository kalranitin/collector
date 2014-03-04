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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;

import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;
import com.ning.metrics.collector.processing.db.util.MySqlLock;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.skife.config.TimeSpan;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.LongMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DatabaseCounterStorage implements CounterStorage
{
    private static final Logger log = LoggerFactory.getLogger(DatabaseCounterStorage.class);
    private final IDBI dbi;
    private final CollectorConfig config;
    private final Lock dbLock;
    private final ObjectMapper mapper;
    public static final DateTimeFormatter DAILY_METRICS_STORAGE_DATE_FORMATER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
    final Cache<String, Optional<CounterSubscription>> counterSubscriptionByAppId;
    // while serialization and deserialization of multimap the keys are converted to String while we need Integer
    final static TypeReference<ArrayListMultimap<Integer,String>> multimapIntegerKeyTypeRef = new TypeReference<ArrayListMultimap<Integer,String>>() {};
    final TimeSpan cacheExpiryTime;
    
    @Inject
    public DatabaseCounterStorage(final IDBI dbi, final CollectorConfig config, final ObjectMapper mapper)
    {
        this.dbi = dbi;
        this.config = config;
        this.dbLock = new MySqlLock("counter-event-storage", dbi);
        this.cacheExpiryTime = config.getSubscriptionCacheTimeout();
        this.mapper = mapper;
        this.counterSubscriptionByAppId = CacheBuilder.newBuilder()
                .maximumSize(config.getMaxCounterSubscriptionCacheCount())
                .expireAfterAccess(cacheExpiryTime.getPeriod(),cacheExpiryTime.getUnit())
                .recordStats()
                .build();
    }
    
    private Optional<CounterSubscription> getCounterSubscription(final String appId)
    {
        Optional<CounterSubscription> counterSubscription =  this.counterSubscriptionByAppId.getIfPresent(appId);
        return counterSubscription == null?Optional.<CounterSubscription>absent():counterSubscription;
    }
    
    private void addCounterSubscription(final String appId, final Optional<CounterSubscription> counterSubscription)
    {
        this.counterSubscriptionByAppId.put(appId, counterSubscription);
    }

    @Override
    public Long createCounterSubscription(final CounterSubscription counterSubscription)
    {
        return dbi.withHandle(new HandleCallback<Long>()
            {
                @Override
                public Long withHandle(Handle handle) throws Exception
                {
                    return handle.createStatement("insert into metrics_subscription (identifier, distribution_for) values (:identifier, :distributionFor)")
                                 .bind("identifier", counterSubscription.getAppId())
                                 .bind("distributionFor", mapper.writerWithType(multimapIntegerKeyTypeRef).writeValueAsString(counterSubscription.getIdentifierDistribution()))
                                 .executeAndReturnGeneratedKeys(LongMapper.FIRST)
                                 .first();
                }
            });
    }
    
    @Override
    public Long updateCounterSubscription(final CounterSubscription counterSubscription, final Long id)
    {
    	return dbi.withHandle(new HandleCallback<Long>()
        {
            @Override
            public Long withHandle(Handle handle) throws Exception
            {
                handle.createStatement("update metrics_subscription set  distribution_for = :distributionFor where id = :id")
                             .bind("distributionFor", mapper.writerWithType(multimapIntegerKeyTypeRef).writeValueAsString(counterSubscription.getIdentifierDistribution()))
                             .bind("id", id)
                             .execute();
                
                addCounterSubscription(counterSubscription.getAppId(), Optional.fromNullable(counterSubscription));
                
                return id;
            }
        });
    }

    @Override
    public CounterSubscription loadCounterSubscription(final String appId)
    {
        Optional<CounterSubscription> cachedResult = getCounterSubscription(appId);
        
        if(cachedResult.isPresent())
        {
            return cachedResult.get();
        }
        
        return dbi.withHandle(new HandleCallback<CounterSubscription>()
            {
            
                @Override
                public CounterSubscription withHandle(Handle handle) throws Exception
                {
                    CounterSubscription counterSubscription = handle.createQuery("select id, identifier, distribution_for from metrics_subscription where identifier = :appId")
                                 .bind("appId", appId)
                                 .map(new CounterSubscriptionMapper(mapper))
                                 .first();
                    
                    addCounterSubscription(appId, Optional.fromNullable(counterSubscription));
                    
                    return counterSubscription;
                }
            });
    }
    
    @Override
    public CounterSubscription loadCounterSubscriptionById(final Long subscriptionId)
    {   
        return dbi.withHandle(new HandleCallback<CounterSubscription>()
            {
            
                @Override
                public CounterSubscription withHandle(Handle handle) throws Exception
                {
                    CounterSubscription counterSubscription = handle.createQuery("select id, identifier, distribution_for from metrics_subscription where id = :subscriptionId")
                                 .bind("subscriptionId", subscriptionId)
                                 .map(new CounterSubscriptionMapper(mapper))
                                 .first();
                    
                    return counterSubscription;
                }
            });
    }

    @Override
    public void insertDailyMetrics(final Multimap<Long, CounterEventData> dailyCounters)
    {
       dbi.withHandle(new HandleCallback<Void>() {

        @Override
        public Void withHandle(Handle handle) throws Exception
        {
            PreparedBatch batch = handle.prepareBatch("insert into metrics_daily (subscription_id,metrics,created_date) values (:subscriptionId, :metrics, :createdDate)");
            for(Entry<Long, CounterEventData> entry : dailyCounters.entries())
            {
                batch.bind("subscriptionId", entry.getKey())
                .bind("metrics", mapper.writeValueAsString(entry.getValue()))
                .bind("createdDate", DAILY_METRICS_STORAGE_DATE_FORMATER.print(entry.getValue().getCreatedDate()))
                .add();
            }
            
            batch.execute();
            
            return null;
        }});         
    }

    @Override
    public List<CounterEventData> loadDailyMetrics(final Long subscriptionId, final DateTime toDateTime, final Integer limit, final Integer offset)
    {   
       return dbi.withHandle(new HandleCallback<List<CounterEventData>>() {

        @Override
        public List<CounterEventData> withHandle(Handle handle) throws Exception
        {
            final Optional<DateTime> toDateTimeOptional = Optional.fromNullable(toDateTime);
            final Optional<Integer> limitOptional = Optional.fromNullable(limit);
            final Optional<Integer> offsetOptional = Optional.fromNullable(offset);
            
            final String queryStr = "select metrics from metrics_daily where subscription_id = :subscriptionId" 
            +(toDateTimeOptional.isPresent()?" and created_date <= :toDateTime":"")
            +(limitOptional.isPresent()?" limit :limit":"")
            +(limitOptional.isPresent() && offsetOptional.isPresent()?" offset :offset":"");
            
            Query<Map<String, Object>> query =  handle.createQuery(queryStr)
                    .bind("subscriptionId", subscriptionId);
            
            if(toDateTimeOptional.isPresent())
            {
                query.bind("toDateTime", DAILY_METRICS_STORAGE_DATE_FORMATER.print(toDateTimeOptional.get()));
            }
            if(limitOptional.isPresent())
            {
                query.bind("limit", limitOptional.get());
                
                if(offsetOptional.isPresent())
                {
                    query.bind("offset", offsetOptional.get());
                }
            }
            
            return ImmutableList.copyOf(query.map(new CounterEventDataMapper(mapper)).list());
            
        }});
    }
    
    @Override
    public List<CounterEventData> loadGroupedDailyMetrics(final Long subscriptionId, final DateTime toDateTime){
        return dbi.withHandle(new HandleCallback<List<CounterEventData>>() {

            @Override
            public List<CounterEventData> withHandle(Handle handle) throws Exception
            {
                final String queryStr = "select metrics from metrics_daily where subscription_id = :subscriptionId"+(Objects.equal(null, toDateTime)?"":" and created_date <= :toDateTime");
                
                Query<Map<String, Object>> query =  handle.createQuery(queryStr)
                        .bind("subscriptionId", subscriptionId);
                
                if(!Objects.equal(null, toDateTime))
                {
                    query.bind("toDateTime", DAILY_METRICS_STORAGE_DATE_FORMATER.print(toDateTime));
                }
                
                Map<String,CounterEventData> groupMap = new ConcurrentHashMap<String, CounterEventData>();
                
                ResultIterator<CounterEventData> rs = query.map(new CounterEventDataMapper(mapper)).iterator();
                
                try {
                    while(rs.hasNext())
                    {
                        CounterEventData counterEventData = rs.next();
                        final String counterKey = counterEventData.getUniqueIdentifier()+counterEventData.getFormattedDate();
                        CounterEventData groupedData = groupMap.get(counterKey);
                        
                        if(Objects.equal(null, groupedData))
                        {
                            groupMap.put(counterKey, counterEventData);
                            continue;
                        }
                        
                        groupedData.mergeCounters(counterEventData.getCounters());
                        groupMap.put(counterKey, groupedData);  
                    }
                }
                finally{
                    rs.close(); 
                }
                
                
                
                return ImmutableList.copyOf(groupMap.values());
                
            }});
    }

    @Override
    public boolean deleteDailyMetrics(final Long subscriptionId, final DateTime toDateTime){
        int deleted = dbi.withHandle(new HandleCallback<Integer>() {
            
            @Override
            public Integer withHandle(Handle handle) throws Exception
            {
                String queryStr = "delete from metrics_daily where subscription_id = :subscriptionId"+(Objects.equal(null, toDateTime)?"":" and created_date <= :toDateTime");
                
                Update query =  handle.createStatement(queryStr)
                        .bind("subscriptionId", subscriptionId);
                
                if(!Objects.equal(null, toDateTime))
                {
                    query.bind("toDateTime", DAILY_METRICS_STORAGE_DATE_FORMATER.print(toDateTime));
                }
                
                return query.execute();
            }});
        
        return deleted > 0;
    }
    
    
    @Override
    public List<Long> getSubscritionIdsFromDailyMetrics(){
        return dbi.withHandle(new HandleCallback<List<Long>>() {

            @Override
            public List<Long> withHandle(Handle handle) throws Exception
            {
                return ImmutableList.copyOf(handle.createQuery("select distinct(subscription_id) from metrics_daily").map(LongMapper.FIRST).list());
                
            }});
    }

    @Override
    public String insertOrUpdateRolledUpCounter(final Long subscriptionId, final RolledUpCounter rolledUpCounter)
    {
        return dbi.withHandle(new HandleCallback<String>() {

            @Override
            public String withHandle(Handle handle) throws Exception
            {
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                final GZIPOutputStream zipStream = new GZIPOutputStream(outputStream);
                mapper.writeValue(zipStream, rolledUpCounter);
                zipStream.finish();
                
                final String dateStr = rolledUpCounter.getFormattedDate();
                // Create id as "network_111_2014-01-20"
                final String id = rolledUpCounter.getAppId()+dateStr;
                
                handle.createStatement("INSERT INTO metrics_daily_roll_up (id,subscription_id, metrics,created_date) VALUES (:id, :subscriptionId, :metrics, :createdDate) ON DUPLICATE KEY UPDATE metrics = :metrics")
                .bind("id", id)
                .bind("subscriptionId", subscriptionId)
                .bind("metrics", outputStream.toByteArray())
                .bind("createdDate", dateStr)
                .execute();
                
                return id;
            }});          
    }

    @Override
    public RolledUpCounter loadRolledUpCounterById(final String id, final boolean exceludeDistribution)
    {
        return dbi.withHandle(new HandleCallback<RolledUpCounter>()
            {
                @Override
                public RolledUpCounter withHandle(Handle handle) throws Exception
                {
                    Optional<Set<String>> optional = Optional.absent();
                    
                    return handle.createQuery("select metrics from metrics_daily_roll_up where id = :id")
                                 .bind("id", id)
                                 .map(new RolledUpCounterMapper(mapper,optional,exceludeDistribution))
                                 .first();
                }
            });
    }

    @Override
    public List<RolledUpCounter> loadRolledUpCounters(final Long subscriptionId, final DateTime fromDate, final DateTime toDate, final Optional<Set<String>> fetchCounterNames, final boolean excludeDistribution)
    {
        return dbi.withHandle(new HandleCallback<List<RolledUpCounter>>() {

            @Override
            public List<RolledUpCounter> withHandle(Handle handle) throws Exception
            {
                final String queryStr = "select metrics from metrics_daily_roll_up where subscription_id = :subscriptionId"
            +(Objects.equal(null, fromDate)?"":" and created_date >= :fromDate")
            +(Objects.equal(null, toDate)?"":" and created_date <= :toDate");
                
                Query<Map<String, Object>> query =  handle.createQuery(queryStr)
                        .bind("subscriptionId", subscriptionId);
                
                
                if(!Objects.equal(null, fromDate))
                {
                    query.bind("fromDate", RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.print(fromDate));
                }
                if(!Objects.equal(null, toDate))
                {
                    query.bind("toDate", RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.print(toDate));
                }
                
                return ImmutableList.copyOf(query.map(new RolledUpCounterMapper(mapper, fetchCounterNames,excludeDistribution)).list());
                
            }});
    }
    
    public int cleanExpiredRolledUpCounterEvents(final DateTime toDateTime)
    {
        int deleted = dbi.withHandle(new HandleCallback<Integer>() {
            
            @Override
            public Integer withHandle(Handle handle) throws Exception
            {
                String queryStr = "delete from metrics_daily_roll_up where created_date <= :toDateTime";
                
                Update query =  handle.createStatement(queryStr)
                        .bind("toDateTime", RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.print(toDateTime));
                
                return query.execute();
            }});
        
        return deleted;
    }
    
    public static class CounterSubscriptionMapper implements ResultSetMapper<CounterSubscription>
    {
        private final ObjectMapper mapper;
        
        public CounterSubscriptionMapper(final ObjectMapper mapper){
            this.mapper = mapper;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public CounterSubscription map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                return new CounterSubscription(r.getLong("id"), r.getString("identifier"), (ArrayListMultimap<Integer, String>) mapper.readValue(r.getString("distribution_for"), multimapIntegerKeyTypeRef));
            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Error handling not implemented!", e);
            }
        }
        
    }
    
    public static class CounterEventDataMapper implements ResultSetMapper<CounterEventData>
    {
        private final ObjectMapper mapper;
        
        public CounterEventDataMapper(final ObjectMapper mapper){
            this.mapper = mapper;
        }
        
        @Override
        public CounterEventData map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                return mapper.readValue(r.getString("metrics"), CounterEventData.class);
            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Error handling not implemented!", e);
            }
        }
        
    }
    
    public class RolledUpCounterMapper implements ResultSetMapper<RolledUpCounter>
    {
        private final ObjectMapper mapper;
        private final Optional<Set<String>> fetchCounterNames;
        private final boolean excludeDistribution;
        
        public RolledUpCounterMapper(final ObjectMapper mapper, final Optional<Set<String>> fetchCounterNames, final boolean excludeDistribution){
            this.mapper = mapper;
            this.fetchCounterNames = fetchCounterNames;
            this.excludeDistribution = excludeDistribution;
        }
        
        @Override
        public RolledUpCounter map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                GZIPInputStream zipStream = new GZIPInputStream(r.getBinaryStream("metrics"));
                RolledUpCounter rolledUpCounter = mapper.readValue(zipStream, RolledUpCounter.class);
                
                if(!Objects.equal(null, rolledUpCounter) && !Objects.equal(null, fetchCounterNames) && fetchCounterNames.isPresent())
                {
                    rolledUpCounter.aggregateCounterDataFor(fetchCounterNames.get(), excludeDistribution);
                }
                else if(!Objects.equal(null, rolledUpCounter) && excludeDistribution)
                {
                    rolledUpCounter.aggregateCounterDataFor(null, excludeDistribution);
                }
                
                return rolledUpCounter; 
            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Error handling not implemented!", e);
            }
        }
        
    }
    
    @Override
    public void cleanUp()
    {
        this.counterSubscriptionByAppId.cleanUp();
        this.counterSubscriptionByAppId.invalidateAll();
        dbLock.unlock();
    }

}
