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

package com.ning.metrics.collector.processing.hadoop;

import com.ning.arecibo.jmx.Monitored;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.EventSpoolProcessor;
import com.ning.metrics.collector.processing.LocalSpoolManager;
import com.ning.metrics.collector.processing.SerializationType;
import com.ning.metrics.serialization.hadoop.FileSystemAccess;

import com.google.inject.Inject;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.Managed;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class HadoopWriterFactory implements EventSpoolProcessor
{
    private static final Logger log = LoggerFactory.getLogger(HadoopWriterFactory.class);

    private final CollectorConfig config;
    private final FileSystemAccess hdfsAccess;
    private final AtomicBoolean flushEnabled;

    @Inject
    public HadoopWriterFactory(final FileSystemAccess hdfsAccess, final CollectorConfig config)
    {
        this.hdfsAccess = hdfsAccess;
        this.config = config;
        this.flushEnabled = new AtomicBoolean(config.isFlushEnabled());
    }
    
    @Override
    public void processEventFile(final String eventName, final SerializationType serializationType,  final File file, final String hadoopOutputPath) throws IOException{
        pushFileToHadoop(file, hadoopOutputPath); 
    }
    
    @Override
    public void close(){
     // Close HDFS layer
        hdfsAccess.close();
    }
    
    @Override
    public String getProcessorName(){
        return "HDFSWriter";
    }


    protected void pushFileToHadoop(final File file, final String outputPath) throws IOException
    {
        log.info(String.format("Flushing events to HDFS: [%s] -> [%s]", file.getAbsolutePath(), outputPath));
        hdfsAccess.get().copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(outputPath));
    }


    @Managed(description = "Whether files should be flushed to HDFS")
    public AtomicBoolean getFlushEnabled()
    {
        return flushEnabled;
    }

    @Managed(description = "Enable flush to HDFS")
    public void enableFlush()
    {
        flushEnabled.set(true);
    }

    @Managed(description = "Disable flush to HDFS")
    public void disableFlush()
    {
        flushEnabled.set(false);
    }

    @Monitored(description = "Number of local files not yet pushed to HDFS")
    public int nbLocalFiles()
    {
        return LocalSpoolManager.findFilesInSpoolDirectory(new File(config.getSpoolDirectoryName())).size();
    }
}
