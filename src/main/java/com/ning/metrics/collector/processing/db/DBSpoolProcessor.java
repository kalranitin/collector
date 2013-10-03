package com.ning.metrics.collector.processing.db;

import com.ning.metrics.collector.processing.EventSpoolProcessor;
import com.ning.metrics.collector.processing.SerializationType;

import com.google.inject.Inject;

import org.skife.jdbi.v2.IDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class DBSpoolProcessor implements EventSpoolProcessor
{
    private static final Logger log = LoggerFactory.getLogger(DBSpoolProcessor.class);
    private final IDBI dbi;
    
    @Inject
    public DBSpoolProcessor(final IDBI dbi)
    {
        this.dbi = dbi;
    }

    @Override
    public void processEventFile(final String eventName, final SerializationType serializationType, File file, String hadoopOutputPath) throws IOException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String getProcessorName()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
