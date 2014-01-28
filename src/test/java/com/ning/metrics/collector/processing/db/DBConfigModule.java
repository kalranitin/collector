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
import com.ning.metrics.collector.binder.config.CollectorConfigurationObjectFactory;
import com.ning.metrics.collector.guice.providers.CollectorDBIProvider;

import com.google.inject.AbstractModule;

import org.skife.config.ConfigurationObjectFactory;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

public class DBConfigModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        ConfigurationObjectFactory configFactory = new CollectorConfigurationObjectFactory(System.getProperties());
        bind(ConfigurationObjectFactory.class).toInstance(configFactory);
        
        final CollectorConfig collectorConfig = configFactory.build(CollectorConfig.class);
        bind(CollectorConfig.class).toInstance(collectorConfig);
        
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (final Exception ignore) {
        }
        bind(IDBI.class).to(DBI.class).asEagerSingleton();
        bind(DBI.class).toProvider(CollectorDBIProvider.class).asEagerSingleton();
        
        bind(SubscriptionCache.class).to(InMemorySubscriptionCache.class).asEagerSingleton();        
        
        bind(SubscriptionStorage.class).to(DatabaseSubscriptionStorage.class).asEagerSingleton(); 
        bind(FeedEventStorage.class).to(DatabaseFeedEventStorage.class).asEagerSingleton();
        bind(FeedStorage.class).to(DatabaseFeedStorage.class).asEagerSingleton();
        bind(FeedEventProcessor.class).asEagerSingleton();
        
        bind(CounterEventCacheProcessor.class).to(InMemoryCounterCacheProcessor.class).asEagerSingleton(); 
        bind(CounterStorage.class).to(DatabaseCounterStorage.class).asEagerSingleton();
          
        
    }
}
