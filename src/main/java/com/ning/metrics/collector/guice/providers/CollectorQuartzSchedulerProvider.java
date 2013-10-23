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
package com.ning.metrics.collector.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.JobFactory;

public class CollectorQuartzSchedulerProvider implements Provider<Scheduler>
{
    private final JobFactory jobFactory;
    
    @Inject
    public CollectorQuartzSchedulerProvider(final JobFactory jobFactory){
        this.jobFactory = jobFactory;
    }

    @Override
    public Scheduler get()
    {
        final Scheduler scheduler;
        
        try {
            SchedulerFactory schedulerFactory = new StdSchedulerFactory();  
            scheduler = schedulerFactory.getScheduler();
            scheduler.setJobFactory(this.jobFactory);
        }
        catch (SchedulerException e) {
            throw new RuntimeException(e);
        }  
        
        return scheduler;
        
    }

}
