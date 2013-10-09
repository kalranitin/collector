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

import com.ning.metrics.collector.processing.db.model.EventMetaData;
import com.ning.metrics.collector.processing.db.model.Subscription;
import com.ning.metrics.collector.processing.db.util.InClauseExpander;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.LongMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class SubscriptionStorageImpl implements SubscriptionStorage
{
    public static final Splitter WHITESPACE_SPLITTER = Splitter.on(" ");
    public static final Joiner WHITESPACE_JOINER = Joiner.on(" ");

    private static final ObjectMapper mapper = new ObjectMapper();

    private final IDBI dbi;
    
    @Inject
    public SubscriptionStorageImpl(final IDBI dbi)
    {
        this.dbi = dbi;
    }

    @Override
    public Long insert(final Subscription subscription)
    {
        return dbi.withHandle(new HandleCallback<Long>()
        {
            @Override
            public Long withHandle(Handle handle) throws Exception
            {
                return handle.createStatement("insert into subscriptions (target, metadata, channel) values (:target, :metadata, :channel)")
                             .bind("target", subscription.getTarget())
                             .bind("metadata", mapper.writeValueAsString(subscription.getMetadata()))
                             .bind("channel", subscription.getChannel())
                             .executeAndReturnGeneratedKeys(LongMapper.FIRST)
                             .first();
            }
        });
    }

    @Override
    public Set<Subscription> load(final String target)
    {
        return dbi.withHandle(new HandleCallback<Set<Subscription>>()
        {
            @Override
            public Set<Subscription> withHandle(Handle handle) throws Exception
            {
                // The logic is to look up all possible combinations from left to right separated by " "
                // e.g. target "a b c" should look up for subscriptions for "a", "a b", "a b c".
                Iterable<String> parts = WHITESPACE_SPLITTER.split(target);
                List<String> targets = Lists.newArrayList();
                List<String> reconsistuted_parts = Lists.newArrayList();
                for (String part : parts) {
                    reconsistuted_parts.add(part);
                    targets.add(WHITESPACE_JOINER.join(reconsistuted_parts));
                }
                InClauseExpander in = new InClauseExpander(targets);
                
                return ImmutableSet.copyOf(handle.createQuery("select id, metadata, channel, target from subscriptions where target in (" + in.getExpansion() + ')')
                                                 .bindNamedArgumentFinder(in)
                                                 .map(new SubscriptionMapper())
                                                 .list());
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
                return 1 == handle.createStatement("delete from subscriptions where id = :id")
                                  .bind("id", id)
                                  .execute();
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
                return handle.createQuery("select id, target, metadata, channel from subscriptions where id = :id")
                             .bind("id", id)
                             .map(new SubscriptionMapper())
                             .first();
            }
        });
    }

    public static class SubscriptionMapper implements ResultSetMapper<Subscription>
    {

        private final Optional<String> target;

        public SubscriptionMapper(String target)
        {
            this.target = Optional.of(target);
        }

        public SubscriptionMapper()
        {
            this.target = Optional.absent();
        }

        @Override
        public Subscription map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                EventMetaData meta = mapper.readValue(r.getString("metadata"), EventMetaData.class);
                return new Subscription(r.getLong("id"),
                                        target.or(new ResultSetStringSupplier(r, "target")),
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
}
