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

package com.ning.metrics.collector.hadoop.processing;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Flat File Processor which has been written to the disk for all the events been captured.
 * <p/>
 * This processor is called during the flush time when the buffer is filled up. THe implementations
 * are supposed to pick the files and process them for e.g. write to hadoop or write to database etc.
 * <p/>
 * Each implentation is also supposed to handle it's own stats and filteration of the events 
 * based on the event name been passed on.
 */
public interface EventSpoolProcessor
{
    public void processEventFile(final String eventName, final SerializationType serializationType, final File file, final String outputPath) throws IOException;
    public void close();
    public String getProcessorName();
}
