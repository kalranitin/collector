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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.EventSpoolProcessor;
import com.ning.metrics.collector.processing.EventSpoolWriterFactory;
import com.ning.metrics.collector.processing.PersistentWriterFactory;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.guice.ExportBuilder;
import org.weakref.jmx.guice.MBeanModule;

public class EventSpoolWriterModule implements Module
{
    Logger log = LoggerFactory.getLogger(EventSpoolWriterModule.class);

    private final CollectorConfig config;

    private final Pattern perEventSpoolWriterClassesPattern;

    @Inject
    public EventSpoolWriterModule(final CollectorConfig config) {
        this.config = config;
        this.perEventSpoolWriterClassesPattern = Pattern.compile(
                "^collector\\.spoolWriter\\.(\\w+)\\.classes$");
    }

    /**
     * This method scans the system properties for properties with keys matching
     * the pattern for perEventSpoolWriterClasses
     * @return map of eventType (derived from property string) to list of
     *          spool writer class names
     */
    private Map<String, Iterable<String>> getSpoolWriterClassesByEventType() {

        Map<String, Iterable<String>> result = Maps.newHashMap();
        Splitter splitter = Splitter.on(",").trimResults().omitEmptyStrings();

        for (Map.Entry keyValue : System.getProperties().entrySet()) {

            Matcher matcher = perEventSpoolWriterClassesPattern.matcher(
                    (String)keyValue.getKey());

            if (matcher.matches()) {
                result.put(matcher.group(1),
                        splitter.split((String)keyValue.getValue()));
            }
        }

        return result;
    }

    @Override
    public void configure(final Binder binder)
    {
        final ExportBuilder builder = MBeanModule.newExporter(binder);

        binder.bind(PersistentWriterFactory.class)
                .to(EventSpoolWriterFactory.class).asEagerSingleton();

        Multibinder<EventSpoolProcessor> defaultSpoolProcessors
                = Multibinder.newSetBinder(binder,EventSpoolProcessor.class);

        MapBinder<String, EventSpoolProcessor> perEventSpoolDispatchers
                = MapBinder.newMapBinder(binder, String.class,
                        EventSpoolProcessor.class).permitDuplicates();

        String spoolWriterClasses = config.getSpoolWriterClassNames();
        final Iterable<String> spoolDispatcherClasses;

        if (!Strings.isNullOrEmpty(spoolWriterClasses)) {
            spoolDispatcherClasses
                    = Splitter.on(",")
                    .trimResults()
                    .omitEmptyStrings()
                    .split(spoolWriterClasses);
        }
        else {
            spoolDispatcherClasses = Lists.newArrayList();
        }

        // Bind the classes for default spoolClasses
        for(String className : spoolDispatcherClasses) {
            try {
                defaultSpoolProcessors .addBinding()
                        .to(Class.forName(className)
                                .asSubclass(EventSpoolProcessor.class))
                        .asEagerSingleton();
            }
            catch (ClassNotFoundException e) {
                log.error("Could not bind "+className,e);
            }
        }

        // Find and bind the classes used for per event spool writers
        Map<String, Iterable<String>> perEventSpoolWriterClasses
                = getSpoolWriterClassesByEventType();


        for (Map.Entry<String, Iterable<String>> eventToSpoolWriters
                : perEventSpoolWriterClasses.entrySet()) {
            for (String spoolWriterClass : eventToSpoolWriters.getValue()) {

                try {
                    perEventSpoolDispatchers
                            .addBinding(eventToSpoolWriters.getKey())
                            .to(Class.forName(spoolWriterClass)
                                    .asSubclass(EventSpoolProcessor.class));
                }
                catch (ClassNotFoundException ex) {
                    log.error("Could not bind " + spoolWriterClass, ex);
                }
            }
        }

        builder.export(EventSpoolWriterFactory.class).as(
                "com.ning.metrics.collector:name=EventSpoolWriter");
    }
}