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
package com.ning.metrics.collector.processing.quartz;

import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.db.DatabaseFeedEventStorage;
import com.ning.metrics.collector.processing.db.FeedEventProcessor;
import com.ning.metrics.collector.processing.db.model.FeedEvent;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.inject.Inject;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class FeedUpdateQuartzJob implements Job
{
    public static final String FEED_EVENT_ID_LIST = "feedEventIdList";

    private static final String RETRY_COUNT = "retryCount";

    private static final Splitter SPLITTER = Splitter.on(',');

    private static final Logger log = LoggerFactory.getLogger(FeedUpdateQuartzJob.class);
    
    private final FeedEventProcessor feedEventProcessor;
    private final DatabaseFeedEventStorage databaseFeedEventStorage;
    private final CollectorConfig config;
    
    @Inject
    public FeedUpdateQuartzJob(FeedEventProcessor feedEventProcessor, final DatabaseFeedEventStorage databaseFeedEventStorage, CollectorConfig config)
    {
        this.feedEventProcessor = feedEventProcessor;
        this.databaseFeedEventStorage = databaseFeedEventStorage;
        this.config = config;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException
    {
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        int retryCount = 0;
        
        if(dataMap.containsKey(RETRY_COUNT))
        {
            retryCount = dataMap.getIntValue(RETRY_COUNT);
        }
        
        log.debug("Job Started with retry count as: "+retryCount);
        log.debug("Feed Event batch ID's in the data list: " + dataMap.get(FEED_EVENT_ID_LIST));
       
        
        if(retryCount > 2)
        {
            JobExecutionException e = new JobExecutionException("Retries exceeded",false);
            //unschedule it so that it doesn't run again
            e.setUnscheduleAllTriggers(true);
            throw e;
        }
        
        try {
            List<String> batchIdList = (List<String>) dataMap.get(FEED_EVENT_ID_LIST);
            
            if(!Objects.equal(null, batchIdList) && batchIdList.size() != 0){
                List<FeedEvent> feedEvents = databaseFeedEventStorage.loadFeedEventsByBatchId(SPLITTER.trimResults().omitEmptyStrings().splitToList(config.getFeedRollupChannelNames()), batchIdList, config.getMaxFeedEventFetchCount());
                feedEventProcessor.process(feedEvents);
                
            }
                 
        }
        catch (Exception e) {
            log.debug("Retrying "+retryCount);
            retryCount++;
            dataMap.putAsString(RETRY_COUNT, retryCount);
            JobExecutionException e2 = new JobExecutionException(e,true);
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e1) {
                log.error("Unexpected error while sleeping for retry of Job Execution for Feed Update ",e1);
            }
            e2.refireImmediately();
            throw e2;
        }

    }
}
