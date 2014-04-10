/*
 * Copyright 2010-2014 Ning, Inc.
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
package com.ning.metrics.collector.processing.quartz;

import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.db.FeedEventStorage;

import com.google.inject.Inject;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class FeedEventCleanUpJob implements Job
{
    private static final Logger log = LoggerFactory.getLogger(FeedEventCleanUpJob.class);
    private final FeedEventStorage feedEventStorage;
    private final CollectorConfig config;
    
    @Inject
    public FeedEventCleanUpJob(final FeedEventStorage feedEventStorage, final CollectorConfig config)
    {
        this.feedEventStorage = feedEventStorage;
        this.config = config;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException
    {
        log.info("Starting clean up of expired feed events!!");
        
        int deletedFeedEvents = 1;
        
        while(deletedFeedEvents > 0)
        {
            deletedFeedEvents = feedEventStorage.cleanOldFeedEvents();
            log.info(String.format("Deleted %d expired feed events", deletedFeedEvents));
        }
        
        log.info("Expired feed event clean up done!!");
    }

}
