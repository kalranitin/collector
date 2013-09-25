package com.ning.metrics.collector.hadoop.processing;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;

public class MockEventSpoolWriterModule implements Module
{
    @Override
    public void configure(final Binder binder)
    {
        binder.bind(PersistentWriterFactory.class).to(EventSpoolWriterFactory.class);
        Multibinder.newSetBinder(binder,EventSpoolProcessor.class);
    }
}