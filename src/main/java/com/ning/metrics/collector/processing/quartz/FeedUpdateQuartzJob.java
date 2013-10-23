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

import com.ning.metrics.collector.processing.db.DatabaseFeedEventStorage;
import com.ning.metrics.collector.processing.db.FeedEventProcessor;
import com.ning.metrics.collector.processing.db.model.FeedEvent;

import com.google.common.base.Objects;
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
    private static final Logger log = LoggerFactory.getLogger(FeedUpdateQuartzJob.class);
    
    private final FeedEventProcessor feedEventProcessor;
    private final DatabaseFeedEventStorage databaseFeedEventStorage;
    
    @Inject
    public FeedUpdateQuartzJob(FeedEventProcessor feedEventProcessor, final DatabaseFeedEventStorage databaseFeedEventStorage){
        this.feedEventProcessor = feedEventProcessor;
        this.databaseFeedEventStorage = databaseFeedEventStorage;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException
    {
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        int retryCount = 0;
        
        if(dataMap.containsKey("retryCount"))
        {
            retryCount = dataMap.getIntValue("retryCount");
        }
        
        log.debug("Job Started with retry count as: "+retryCount);
        log.debug("Feed Event ID's in the data list: "+dataMap.get("feedEventIdList"));
       
        
        if(retryCount > 2)
        {
            JobExecutionException e = new JobExecutionException("Retries exceeded",false);
            //unschedule it so that it doesn't run again
            e.setUnscheduleAllTriggers(true);
            throw e;
        }
        
        try {
            List<String> feedEventIdList = (List<String>) dataMap.get("feedEventIdList");
            
            if(!Objects.equal(null, feedEventIdList) && feedEventIdList.size() != 0){
                // TODO make the channels as a configurable list for which the feeds is to be collected
                // TODO make the max count size as configurable
                List<FeedEvent> feedEvents = databaseFeedEventStorage.load("activity", feedEventIdList, 1000);
                feedEventProcessor.process(feedEvents);
                
            }
                 
        }
        catch (Exception e) {
            log.debug("Retrying "+retryCount);
            retryCount++;
            dataMap.putAsString("retryCount", retryCount);
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
