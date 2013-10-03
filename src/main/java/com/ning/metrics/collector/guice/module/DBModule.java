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

import com.google.inject.AbstractModule;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

public class DBModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        // Load mysql driver if needed
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (final Exception ignore) {
        }
        bind(IDBI.class).to(DBI.class).asEagerSingleton();
        bind(DBI.class).toProvider(CollectorDBIProvider.class).asEagerSingleton();
        
    }

}
