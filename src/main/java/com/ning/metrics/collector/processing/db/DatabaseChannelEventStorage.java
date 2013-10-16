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
import com.ning.metrics.collector.processing.db.model.ChannelEvent;
import com.ning.metrics.collector.processing.db.util.MySqlLock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;

public class DatabaseChannelEventStorage implements ChannelEventStorage
{
    private static final Logger log = LoggerFactory.getLogger(DatabaseChannelEventStorage.class);
    private final IDBI dbi;
    private final CollectorConfig config;
    private final Lock dbLock;
    private static final ObjectMapper mapper = new ObjectMapper();
    
    @Inject
    public DatabaseChannelEventStorage(IDBI dbi, CollectorConfig config)
    {
        this.dbi = dbi;
        this.config = config;
        this.dbLock = new MySqlLock("channel-event-deletion", dbi);
    }

    @Override
    public void insert(final Collection<ChannelEvent> channelEvents)
    {
        dbi.withHandle(new HandleCallback<Void>() {

            @Override
            public Void withHandle(Handle handle) throws Exception
            {
                PreparedBatch batch = handle.prepareBatch("insert into channel_events (channel, created_at, metadata, event, subscription_id) values (:channel, :now, :metadata, :event, :subscription_id)");
                
                for(ChannelEvent channelEvent : channelEvents){
                    batch.bind("channel", channelEvent.getChannel())
                    .bind("metadata", mapper.writeValueAsString(channelEvent.getMetadata()))
                    .bind("event", mapper.writeValueAsString(channelEvent.getEvent()))
                    .bind("now", DateTimeUtils.getInstantMillis(new DateTime(DateTimeZone.UTC)))
                    .bind("subscription_id", channelEvent.getSubscriptionId())
                    .add();
                }
                
                batch.execute();
                
                return null;
            }});
    }

    @Override
    public List<ChannelEvent> load(final String channel, final int offset, final int count)
    {
        return dbi.withHandle(new HandleCallback<List<ChannelEvent>>() {

            @Override
            public List<ChannelEvent> withHandle(Handle handle) throws Exception
            {
                return ImmutableList.copyOf(
                    handle.createQuery("select offset, channel, metadata, event, subscription_id from channel_events where channel = :channel and offset > :offset order by offset limit :count")
                    .bind("channel", channel)
                    .bind("offset", offset)
                    .bind("count", count)
                    .setFetchSize(count)
                    .setMaxRows(count)
                    .map(new ChannelEventRowMapper())
                    .list());
            }
            
        });
    }
    
    public void cleanOldChannelEvents(){
        if(dbLock.tryLock()){
            
            int deleted = dbi.withHandle(new HandleCallback<Integer>() {

                @Override
                public Integer withHandle(Handle handle) throws Exception
                {
                    return handle.createStatement("delete from channel_events where created_at < :tillTimePeriod")
                            .bind("tillTimePeriod",DateTimeUtils.currentTimeMillis() - config.getChannelEventRetentionPeriod().getMillis())
                            .execute();
                }});
            
            log.info(String.format("%d Channel events deleted successfully", deleted));
        }
    }
    
    public static class ChannelEventRowMapper implements ResultSetMapper<ChannelEvent>{

        @Override
        public ChannelEvent map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                return new ChannelEvent(r.getInt("offset"),
                    r.getString("channel"),
                    r.getString("metadata"),
                    r.getString("event"),
                    r.getLong("subscription_id"));
            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Not Yet Implemented!", e);
            }
        }
        
    }

    @Override
    public void cleanUp()
    {
        dbLock.unlock();       
    }
    
    

}
