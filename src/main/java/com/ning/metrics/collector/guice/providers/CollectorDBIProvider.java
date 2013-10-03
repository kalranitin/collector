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
package com.ning.metrics.collector.guice.providers;

import com.ning.metrics.collector.binder.config.CollectorConfig;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.jdbi.InstrumentedTimingCollector;
import com.yammer.metrics.jdbi.strategies.BasicSqlNameStrategy;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.TimingCollector;
import org.skife.jdbi.v2.tweak.SQLLog;
import org.skife.jdbi.v2.tweak.TransactionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

public class CollectorDBIProvider implements Provider<DBI>
{
    private static final Logger logger = LoggerFactory.getLogger(CollectorDBIProvider.class);
        
    private final CollectorConfig config;
    private final MetricsRegistry metricsRegistry;
    private SQLLog sqlLog;
    
    @Inject
    public CollectorDBIProvider(final CollectorConfig config, final MetricsRegistry metricsRegistry)
    {
        this.config = config;
        this.metricsRegistry = metricsRegistry;
    }
    
    @Inject(optional = true)
    public void setSqlLog(final SQLLog sqlLog) {
        this.sqlLog = sqlLog;
    }

    @Override
    public DBI get()
    {
        final DBI dbi = new DBI(getDataSource());
        
        if (sqlLog != null) {
            dbi.setSQLLog(sqlLog);
        }
        
        if (config.getTransactionHandlerClass() != null) {
            logger.info("Using " + config.getTransactionHandlerClass() + " as a transaction handler class");
            try {
                dbi.setTransactionHandler((TransactionHandler) Class.forName(config.getTransactionHandlerClass()).newInstance());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        final BasicSqlNameStrategy basicSqlNameStrategy = new BasicSqlNameStrategy();
        final TimingCollector timingCollector = new InstrumentedTimingCollector(metricsRegistry, basicSqlNameStrategy, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        dbi.setTimingCollector(timingCollector);

        return dbi;
    }
    
    private DataSource getDataSource()
    {
        final ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setJdbcUrl(config.getJdbcUrl());
        cpds.setUser(config.getUsername());
        cpds.setPassword(config.getPassword());
        // http://www.mchange.com/projects/c3p0/#minPoolSize
        // Minimum number of Connections a pool will maintain at any given time.
        cpds.setMinPoolSize(config.getMinIdle());
        // http://www.mchange.com/projects/c3p0/#maxPoolSize
        // Maximum number of Connections a pool will maintain at any given time.
        cpds.setMaxPoolSize(config.getMaxActive());
        // http://www.mchange.com/projects/c3p0/#checkoutTimeout
        // The number of milliseconds a client calling getConnection() will wait for a Connection to be checked-in or
        // acquired when the pool is exhausted. Zero means wait indefinitely. Setting any positive value will cause the getConnection()
        // call to time-out and break with an SQLException after the specified number of milliseconds.
        cpds.setCheckoutTimeout((int)TimeUnit.MILLISECONDS.convert(config.getConnectionTimeout().getPeriod(), config.getConnectionTimeout().getUnit()));
        // http://www.mchange.com/projects/c3p0/#maxIdleTime
        // Seconds a Connection can remain pooled but unused before being discarded. Zero means idle connections never expire.
        //        cpds.setMaxIdleTime(toSeconds(config.getIdleMaxAge()));
        // http://www.mchange.com/projects/c3p0/#maxConnectionAge
        // Seconds, effectively a time to live. A Connection older than maxConnectionAge will be destroyed and purged from the pool.
        // This differs from maxIdleTime in that it refers to absolute age. Even a Connection which has not been much idle will be purged
        // from the pool if it exceeds maxConnectionAge. Zero means no maximum absolute age is enforced.
        //        cpds.setMaxConnectionAge(toSeconds(config.getMaxConnectionAge()));
        // http://www.mchange.com/projects/c3p0/#idleConnectionTestPeriod
        // If this is a number greater than 0, c3p0 will test all idle, pooled but unchecked-out connections, every this number of seconds.
        cpds.setIdleConnectionTestPeriod(60);
        
        return cpds;
    }

}
