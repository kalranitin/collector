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

import com.google.common.base.Objects;
import com.google.inject.Inject;
import com.ning.metrics.collector.processing.db.CounterStorage;
import java.util.List;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import static org.quartz.JobBuilder.newJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import org.quartz.SimpleTrigger;
import static org.quartz.TriggerBuilder.newTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class CounterEventScannerJob implements Job
{
    private static final Logger log = LoggerFactory.getLogger(CounterEventScannerJob.class);

    private final CounterStorage counterStorage;
    private final Scheduler quartzScheduler;

    @Inject
    public CounterEventScannerJob(final CounterStorage counterStorage, final Scheduler quartzScheduler)  throws SchedulerException
    {
        this.counterStorage = counterStorage;
        this.quartzScheduler = quartzScheduler;
        if(!quartzScheduler.isStarted())
        {
            quartzScheduler.start();
        }
    }

    /**
     * On execution this method unbuffers the counters in buffered storage and
     * creates separate, independently-executable jobs to roll up the the
     * individual events into a queriable form
     * @param context
     * @throws JobExecutionException
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException
    {
        try {
            if(this.quartzScheduler.isStarted())
            {
                List<String> bufferedNamespaces =
                        counterStorage.getNamespacesFromMetricsBuffer();

                if(Objects.equal(null, bufferedNamespaces) || bufferedNamespaces.isEmpty())
                {
                    log.info("No Collecter Events in daily queue");
                    return;
                }

                for(String namespace : bufferedNamespaces)
                {
                    final JobKey jobKey = new JobKey("counterProcessorJob_"+namespace, "counterProcessorJobGroup");

                    final SimpleTrigger trigger = (SimpleTrigger)newTrigger()
                            .withIdentity("counterProcessorJobTrigger_"+namespace, "counterProcessorTriggerGroup")
                            .withSchedule(simpleSchedule().withMisfireHandlingInstructionFireNow())
                            .build();

                    if(!quartzScheduler.checkExists(jobKey))
                    {
                        JobDataMap jobMap = new JobDataMap();
                        jobMap.put("namespace", namespace);

                        quartzScheduler.scheduleJob(
                            newJob(CounterProcessorRollUpJob.class).withIdentity(jobKey).usingJobData(jobMap).build()
                            ,trigger);
                    }
                }
            }

        }
        catch (Exception e) {
            log.warn("unexpected exception trying to schedule Quartz job for counter roll up processing!",e);
        }
    }

}
