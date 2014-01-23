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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
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
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
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
import java.util.concurrent.locks.Lock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DatabaseCounterStorage implements CounterStorage
{
    private static final Logger log = LoggerFactory.getLogger(DatabaseCounterStorage.class);
    private final IDBI dbi;
    private final CollectorConfig config;
    private final Lock dbLock;
    private static final ObjectMapper mapper = new ObjectMapper();
    
    @Inject
    public DatabaseCounterStorage(final IDBI dbi, final CollectorConfig config)
    {
        this.dbi = dbi;
        this.config = config;
        this.dbLock = new MySqlLock("counter-event-storage", dbi);
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        mapper.registerModule(new JodaModule());
        mapper.registerModule(new GuavaModule());
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
                                 .bind("distributionFor", mapper.writeValueAsString(counterSubscription.getIdentifierDistribution()))
                                 .executeAndReturnGeneratedKeys(LongMapper.FIRST)
                                 .first();
                }
            });
    }

    @Override
    public CounterSubscription loadCounterSubscription(final String appId)
    {
        return dbi.withHandle(new HandleCallback<CounterSubscription>()
            {
                @Override
                public CounterSubscription withHandle(Handle handle) throws Exception
                {
                    return handle.createQuery("select id, identifier, distribution_for from metrics_subscription where identifier = :appId")
                                 .bind("appId", appId)
                                 .map(new CounterSubscriptionMapper())
                                 .first();
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
                .bind("createdDate", entry.getValue().getFormattedDate())
                .add();
            }
            
            batch.execute();
            
            return null;
        }});         
    }

    @Override
    public List<CounterEventData> loadCounterEventData(final Long subscriptionId, final DateTime createdDate)
    {
       return dbi.withHandle(new HandleCallback<List<CounterEventData>>() {

        @Override
        public List<CounterEventData> withHandle(Handle handle) throws Exception
        {
            final String queryStr = "select metrics from metrics_daily where subscription_id = :subscriptionId"+(Objects.equal(null, createdDate)?"":" and created_date = :createdDate");
            
            Query<Map<String, Object>> query =  handle.createQuery(queryStr)
                    .bind("subscription_id", subscriptionId);
            
            if(!Objects.equal(null, createdDate))
            {
                query.bind("createdDate", createdDate);
            }
            
            return ImmutableList.copyOf(query.map(new CounterEventDataMapper()).list());
            
        }});
    }

    @Override
    public boolean deleteDailyMetrics(final List<Long> dailyMetricsIds)
    {
        if(dbLock.tryLock()){
            int deleted = dbi.withHandle(new HandleCallback<Integer>() {
                
                Joiner joiner = Joiner.on(",").skipNulls();
                
                @Override
                public Integer withHandle(Handle handle) throws Exception
                {
                    return handle.createStatement("delete from metrics_daily where id in (" + joiner.join(dailyMetricsIds) + ")").execute();
                }});
            
            return deleted > 0;
        }
        return false;
    }

    @Override
    public void insertOrUpdateRolledUpCounter(final Long subscriptionId, final RolledUpCounter rolledUpCounter, final DateTime createdDate)
    {
        dbi.withHandle(new HandleCallback<Void>() {

            @Override
            public Void withHandle(Handle handle) throws Exception
            {
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                final GZIPOutputStream zipStream = new GZIPOutputStream(outputStream);
                mapper.writeValue(zipStream, rolledUpCounter);
                zipStream.finish();
                
                final String dateStr = rolledUpCounter.getFormattedDate();
                final String id = rolledUpCounter.getAppId()+dateStr;
                
                handle.createStatement("INSERT INTO metrics_daily_roll_up (id,subscription_id, metrics,created_date) VALUES (:id, :subscriptionId, :metrics, :createdDate) ON DUPLICATE KEY UPDATE metrics = :metrics")
                .bind("id", id)
                .bind("subscriptionId", subscriptionId)
                .bind("metrics", outputStream.toByteArray())
                .bind("createdDate", dateStr)
                .execute();
                
                return null;
            }});          
    }

    @Override
    public RolledUpCounter loadRolledUpCounterById(final String id)
    {
        return dbi.withHandle(new HandleCallback<RolledUpCounter>()
            {
                @Override
                public RolledUpCounter withHandle(Handle handle) throws Exception
                {
                    return handle.createQuery("select metrics from metrics_daily_roll_up where id = :id")
                                 .bind("id", id)
                                 .map(new RolledUpCounterMapper())
                                 .first();
                }
            });
    }

    @Override
    public List<RolledUpCounter> loadRolledUpCounters(final Long subscriptionId, final DateTime fromDate, final DateTime toDate)
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    public static class CounterSubscriptionMapper implements ResultSetMapper<CounterSubscription>
    {
        @SuppressWarnings("unchecked")
        @Override
        public CounterSubscription map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                return new CounterSubscription(r.getLong("id"), r.getString("identifier"), mapper.readValue(r.getString("distribution_for"), ArrayListMultimap.class));
            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Error handling not implemented!", e);
            }
        }
        
    }
    
    public static class CounterEventDataMapper implements ResultSetMapper<CounterEventData>
    {
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
    
    public static class RolledUpCounterMapper implements ResultSetMapper<RolledUpCounter>
    {
        @Override
        public RolledUpCounter map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                GZIPInputStream zipStream = new GZIPInputStream(r.getBinaryStream("metrics"));
                return mapper.readValue(zipStream, RolledUpCounter.class);
            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Error handling not implemented!", e);
            }
        }
        
    }

}
