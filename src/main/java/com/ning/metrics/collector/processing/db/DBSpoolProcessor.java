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
import com.ning.metrics.collector.endpoint.extractors.DeserializationType;
import com.ning.metrics.collector.processing.EventSpoolProcessor;
import com.ning.metrics.collector.processing.SerializationType;
import com.ning.metrics.serialization.event.Event;
import com.ning.metrics.serialization.event.EventDeserializer;
import com.ning.metrics.serialization.smile.SmileEnvelopeEventDeserializer;

import com.google.inject.Inject;

import org.skife.jdbi.v2.IDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DBSpoolProcessor implements EventSpoolProcessor
{
    private static final Logger log = LoggerFactory.getLogger(DBSpoolProcessor.class);
    private final IDBI dbi;
    private final CollectorConfig config;
    
    @Inject
    public DBSpoolProcessor(final IDBI dbi, final CollectorConfig config)
    {
        this.dbi = dbi;
        this.config = config;
    }

    @Override
    public void processEventFile(final String eventName, final SerializationType serializationType, final File file, final String outputPath) throws IOException
    {
        // TODO Check in config about the DeserializationType.
        EventDeserializer eventDeserializer = getEventDeserializer(DeserializationType.SMILE, new FileInputStream(file));
        
        while(eventDeserializer.hasNextEvent())
        {
            Event event = eventDeserializer.getNextEvent();
            log.info(String.format("Recieved DB Event to store with name as %s ",event.getName()));
            log.info(String.format("DB Event body to store is %s",event.getData()));
            
        }
        
        
    }
    
    private EventDeserializer getEventDeserializer(DeserializationType type, InputStream is) throws IOException
    {
        switch(type){
            case SMILE:
                return new SmileEnvelopeEventDeserializer(is, false);
            case JSON:
                return new SmileEnvelopeEventDeserializer(is, true);
            default:
                return new SmileEnvelopeEventDeserializer(is, false);
        }        
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String getProcessorName()
    {
        return "DBWriter";
    }

}
