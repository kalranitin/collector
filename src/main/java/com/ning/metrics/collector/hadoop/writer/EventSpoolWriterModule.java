package com.ning.metrics.collector.hadoop.writer;

import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.hadoop.processing.EventSpoolProcessor;
import com.ning.metrics.collector.hadoop.processing.EventSpoolWriterFactory;
import com.ning.metrics.collector.hadoop.processing.PersistentWriterFactory;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.guice.ExportBuilder;
import org.weakref.jmx.guice.MBeanModule;

public class EventSpoolWriterModule implements Module
{
    Logger log = LoggerFactory.getLogger(EventSpoolWriterModule.class);
    
    private final CollectorConfig config;
    
    @Inject
    public EventSpoolWriterModule(final CollectorConfig config)
    {
        this.config = config;
    }
    
    @Override
    public void configure(final Binder binder)
    {
        final ExportBuilder builder = MBeanModule.newExporter(binder);
        
        binder.bind(PersistentWriterFactory.class).to(EventSpoolWriterFactory.class).asEagerSingleton();;

        Multibinder<EventSpoolProcessor> spoolDispatchers = Multibinder.newSetBinder(binder,EventSpoolProcessor.class);
        final String[] spoolDispatcherClasses = StringUtils.split(config.getSpoolWriterClassNames(), ",");
        
        for(String className : spoolDispatcherClasses)
        {
            try {            
                spoolDispatchers.addBinding().to(Class.forName(className).asSubclass(EventSpoolProcessor.class)).asEagerSingleton();
            }
            catch (Exception e) {
                log.error("Could not bind "+className,e);
            }
        }
        
        builder.export(EventSpoolWriterFactory.class).as("com.ning.metrics.collector:name=EventSpoolWriter");
    }
}