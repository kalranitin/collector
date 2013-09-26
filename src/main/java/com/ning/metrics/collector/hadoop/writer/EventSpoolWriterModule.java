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
package com.ning.metrics.collector.hadoop.writer;

import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.hadoop.processing.EventSpoolProcessor;
import com.ning.metrics.collector.hadoop.processing.EventSpoolWriterFactory;
import com.ning.metrics.collector.hadoop.processing.PersistentWriterFactory;

import com.google.common.base.Splitter;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.guice.ExportBuilder;
import org.weakref.jmx.guice.MBeanModule;

import java.util.List;

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
        
        final List<String> spoolDispatcherClasses = Splitter
                                                    .on(",").
                                                    trimResults().
                                                    omitEmptyStrings().
                                                    splitToList(config.getSpoolWriterClassNames());
        
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