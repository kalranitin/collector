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
import com.ning.metrics.collector.processing.db.model.Feeds;
import com.ning.metrics.collector.processing.db.util.MySqlLock;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.exceptions.ResultSetException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DatabaseFeedStorage implements FeedStorage
{

    private static final Logger log = LoggerFactory.getLogger(DatabaseFeedStorage.class);
    private final IDBI dbi;
    private final CollectorConfig config;
    private final Lock dbLock;
    private static final ObjectMapper mapper = new ObjectMapper();
    
    @Inject
    public DatabaseFeedStorage(final IDBI dbi, final CollectorConfig config){
        this.dbi = dbi;
        this.config = config;
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        this.dbLock = new MySqlLock("feed-operation", dbi);
    }
        
    @Override
    public Feeds loadFeedByKey(final String key)
    {
        return dbi.withHandle(new HandleCallback<Feeds>() {

            @Override
            public Feeds withHandle(Handle handle) throws Exception
            {
                return handle.createQuery("SELECT feed FROM feeds WHERE feed_key = :key")
                        .bind("key", key)
                        .map(new FeedRowMapper())
                        .first();
            }});
    }

    @Override
    public void addOrUpdateFeed(final String key, final Feeds feed)
    {
        dbi.withHandle(new HandleCallback<Void>() {

            @Override
            public Void withHandle(Handle handle) throws Exception
            {
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                final GZIPOutputStream zipStream = new GZIPOutputStream(outputStream);
                mapper.writeValue(zipStream, feed);
                zipStream.finish();
                
                handle.createStatement("INSERT INTO feeds (feed_key, feed) VALUES (:key, :feed) ON DUPLICATE KEY UPDATE feed = :feed")
                .bind("key", key)
                .bind("feed", outputStream.toByteArray())
                .execute();
                
                return null;
            }});        
    }

    @Override
    public void deleteFeed(final String key)
    {
        dbi.withHandle(new HandleCallback<Void>() {

            @Override
            public Void withHandle(Handle handle) throws Exception
            {
                handle.createStatement("DELETE FROM feeds WHERE feed_key = :key")
                .bind("key", key)
                .execute();
                
                return null;
            }});
        
    }
    
    public static class FeedRowMapper implements ResultSetMapper<Feeds>{

        @Override
        public Feeds map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                GZIPInputStream zipStream = new GZIPInputStream(r.getBinaryStream("feed"));
                return mapper.readValue(zipStream, Feeds.class);
            }
            catch (IOException ex) {
                throw new ResultSetException("Cannot read feed from result set", ex, ctx);
            }
        }
        
    }

    @Override
    public void cleanUp()
    {
        dbLock.unlock();       
    }

}
