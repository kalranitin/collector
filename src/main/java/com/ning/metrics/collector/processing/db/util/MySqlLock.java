package com.ning.metrics.collector.processing.db.util;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.util.IntegerMapper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class MySqlLock implements Lock
{
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
        while (!_lock(1, TimeUnit.SECONDS)) {
            // try again
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
