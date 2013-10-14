/*
 * Copyright 2010-2011 Ning, Inc.
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

package com.ning.metrics.collector.processing.db.util;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.util.IntegerMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class MySqlLock implements Lock
{
    private static final Logger log = LoggerFactory.getLogger(MySqlLock.class);
    private final String name;

    private final IDBI dbi;

    private volatile Handle handle;

    public MySqlLock(final String name, final IDBI dbi)
    {
        this.name = name;
        this.dbi = dbi;
    }

    private boolean _lock(long duration, TimeUnit unit)
    {
        if (handle == null) {
            handle = dbi.open();
            int got_lock = handle.createQuery("select get_lock(:name, :time)")
                                 .bind("name", name)
                                 .bind("time", unit.toSeconds(duration))
                                 .map(IntegerMapper.FIRST)
                                 .first();
            if (got_lock == 1) {
                return true;
            }
            else {
                handle.close();
                handle = null;
                return false;

            }
        }
        else {
            // we already have the lock!
            return true;
        }
    }

    @Override
    public synchronized void lock()
    {
        int nbOfRetry = 0;
        while (!_lock(1, TimeUnit.SECONDS) && nbOfRetry < 10) {
            log.warn("Retrying as no lock has been achieved");
            nbOfRetry++;
        }
    }

    @Override
    public synchronized void lockInterruptibly() throws InterruptedException
    {
        throw new UnsupportedOperationException("Not Yet Implemented!");
    }

    @Override
    public synchronized boolean tryLock()
    {
        return _lock(0, TimeUnit.SECONDS);
    }

    @Override
    public synchronized boolean tryLock(final long time, final TimeUnit unit) throws InterruptedException
    {
        return _lock(time, unit);
    }

    @Override
    public synchronized void unlock()
    {
        if (handle != null) {
            handle.createQuery("select release_lock(:name)")
                  .bind("name", name)
                  .map(IntegerMapper.FIRST)
                  .first();
            handle.close();
            handle = null;
        }
    }

    @Override
    public synchronized Condition newCondition()
    {
        throw new UnsupportedOperationException("Not Yet Implemented!");
    }
}
