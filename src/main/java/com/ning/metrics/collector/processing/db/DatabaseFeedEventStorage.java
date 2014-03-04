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
import com.ning.metrics.collector.processing.db.model.FeedEvent;
import com.ning.metrics.collector.processing.db.util.InClauseExpander;
import com.ning.metrics.collector.processing.db.util.MySqlLock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
import java.util.UUID;
import java.util.concurrent.locks.Lock;

public class DatabaseFeedEventStorage implements FeedEventStorage
{
    private static final Logger log = LoggerFactory.getLogger(DatabaseFeedEventStorage.class);
    private final IDBI dbi;
    private final CollectorConfig config;
    private final Lock dbLock;
    private static final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public DatabaseFeedEventStorage(final IDBI dbi, final CollectorConfig config)
    {
        this.dbi = dbi;
        this.config = config;
        this.dbLock = new MySqlLock("feed-event-deletion", dbi);
    }

    @Override
    public List<String> insert(final Collection<FeedEvent> feedEvents)
    {
        return dbi.withHandle(new HandleCallback<List<String>>() {

            @Override
            public List<String> withHandle(Handle handle) throws Exception
            {
                final List<String> batchIdList = Lists.newArrayListWithCapacity(1);
                PreparedBatch batch = handle.prepareBatch("insert into feed_events (batch_id, channel, created_at, metadata, event, subscription_id) values (:batch_id, :channel, :now, :metadata, :event, :subscription_id)");
                String batchId = UUID.randomUUID().toString();
                batchIdList.add(batchId);
                for (FeedEvent feedEvent : feedEvents) {
                    batch.bind("batch_id", batchId)
                        .bind("channel", feedEvent.getChannel())
                        .bind("metadata", mapper.writeValueAsString(feedEvent.getMetadata()))
                        .bind("event", mapper.writeValueAsString(feedEvent.getEvent()))
                        .bind("now", DateTimeUtils.getInstantMillis(new DateTime(DateTimeZone.UTC)))
                        .bind("subscription_id", feedEvent.getSubscriptionId())
                        .add();
                }

                batch.execute();

                return batchIdList;
            }
        });
    }

    @Override
    public List<FeedEvent> loadFeedEventsByBatchId(final List<String> channelList, final List<String> batchIdList, final int count)
    {
        return dbi.withHandle(new HandleCallback<List<FeedEvent>>() {

            @Override
            public List<FeedEvent> withHandle(Handle handle) throws Exception
            {
                InClauseExpander idExpander = new InClauseExpander("_idPrefix_", batchIdList);
                InClauseExpander channelExpander = new InClauseExpander("_channelPrefix_", channelList);

                return ImmutableList.copyOf(handle.createQuery(
                    "select id, channel, metadata, event, subscription_id from feed_events where channel in (" + channelExpander.getExpansion() + ") and batch_id in (" + idExpander.getExpansion() + ") limit :count")
                    .bindNamedArgumentFinder(channelExpander)
                    .bindNamedArgumentFinder(idExpander)
                    .bind("count", count)
                    .setFetchSize(count)
                    .setMaxRows(count)
                    .map(new FeedEventRowMapper())
                    .list());
            }

        });
    }

    @Override
    public List<FeedEvent> loadFeedEventsByOffset(final String channel, final long eventOffsetId, final int count)
    {

        final int maxRowsToFetch = (count <= 0) ? config.getMaxFeedEventFetchCount() : count;

        return dbi.withHandle(new HandleCallback<List<FeedEvent>>() {

            @Override
            public List<FeedEvent> withHandle(Handle handle) throws Exception
            {
                return ImmutableList.copyOf(handle.createQuery("select id, channel, metadata, event, subscription_id from" + " feed_events where channel = :channel and id >= :eventOffsetId limit :count")
                    .bind("channel", channel)
                    .bind("eventOffsetId", eventOffsetId)
                    .bind("count", maxRowsToFetch)
                    .setFetchSize(maxRowsToFetch)
                    .setMaxRows(maxRowsToFetch)
                    .map(new FeedEventRowMapper())
                    .list());
            }
        });
    }

    public void cleanOldFeedEvents()
    {
        if (dbLock.tryLock()) {
            int deleted = dbi.withHandle(new HandleCallback<Integer>() {

                @Override
                public Integer withHandle(Handle handle) throws Exception
                {
                    return handle.createStatement("delete from feed_events where created_at < :tillTimePeriod").bind("tillTimePeriod", DateTimeUtils.currentTimeMillis() - config.getFeedEventRetentionPeriod().getMillis()).execute();
                }
            });

            log.info(String.format("%d Feed events deleted successfully", deleted));
        }
    }

    public static class FeedEventRowMapper implements ResultSetMapper<FeedEvent>
    {

        @Override
        public FeedEvent map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                return new FeedEvent(r.getLong("id"), r.getString("channel"), r.getString("metadata"), r.getString("event"), r.getLong("subscription_id"));
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
