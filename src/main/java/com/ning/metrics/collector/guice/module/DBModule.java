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
package com.ning.metrics.collector.guice.module;

import com.ning.metrics.collector.guice.providers.CollectorDBIProvider;
import com.ning.metrics.collector.processing.db.DatabaseFeedStorage;
import com.ning.metrics.collector.processing.db.FeedEventProcessor;
import com.ning.metrics.collector.processing.db.FeedEventStorage;
import com.ning.metrics.collector.processing.db.DBSpoolProcessor;
import com.ning.metrics.collector.processing.db.DatabaseFeedEventStorage;
import com.ning.metrics.collector.processing.db.FeedStorage;
import com.ning.metrics.collector.processing.db.InMemorySubscriptionCache;
import com.ning.metrics.collector.processing.db.SubscriptionCache;
import com.ning.metrics.collector.processing.db.SubscriptionStorage;
import com.ning.metrics.collector.processing.db.DatabaseSubscriptionStorage;

import com.google.inject.Binder;
import com.google.inject.Module;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.weakref.jmx.guice.ExportBuilder;
import org.weakref.jmx.guice.MBeanModule;

public class DBModule implements Module
{

    @Override
    public void configure(final Binder binder)
    {
        // JMX exporter
        final ExportBuilder builder = MBeanModule.newExporter(binder);
        // Load mysql driver if needed
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (final Exception ignore) {
        }
        binder.bind(IDBI.class).to(DBI.class).asEagerSingleton();
        binder.bind(DBI.class).toProvider(CollectorDBIProvider.class).asEagerSingleton();
        
        builder.export(DBSpoolProcessor.class).as("com.ning.metrics.collector:name=DBSpoolProcessor");
        
        binder.bind(SubscriptionCache.class).to(InMemorySubscriptionCache.class).asEagerSingleton();
        
        binder.bind(SubscriptionStorage.class).to(DatabaseSubscriptionStorage.class).asEagerSingleton();  
        
        binder.bind(FeedEventStorage.class).to(DatabaseFeedEventStorage.class).asEagerSingleton();
        
        binder.bind(FeedStorage.class).to(DatabaseFeedStorage.class).asEagerSingleton();
        
        binder.bind(FeedEventProcessor.class).asEagerSingleton();
        
    }

}
