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
package com.ning.metrics.collector.jaxrs;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.ning.metrics.collector.processing.counter.RollUpCounterProcessor;
import com.ning.metrics.collector.processing.db.CounterStorage;
import com.ning.metrics.collector.processing.db.model.CounterSubscription;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("/rest/1.0/metrics")
public class MetricsResource
{
    private static final Logger log = LoggerFactory.getLogger(MetricsResource.class);
    private final CounterStorage counterStorage;
    private final RollUpCounterProcessor rollUpCounterProcessor;
    
    @Inject
    public MetricsResource(final CounterStorage counterStorage, final RollUpCounterProcessor rollUpCounterProcessor)
    {
        this.counterStorage = counterStorage;
        this.rollUpCounterProcessor = rollUpCounterProcessor;
    }
    
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/subscription")
    public Response createOrUpdateCounterSubscription(final CounterSubscription counterSubscription, @Context UriInfo ui)
    {
    	CounterSubscription dbCounterSubscription = counterStorage.loadCounterSubscription(counterSubscription.getAppId());
    	
    	final Long id = dbCounterSubscription == null?counterStorage.createCounterSubscription(counterSubscription):counterStorage.updateCounterSubscription(counterSubscription,dbCounterSubscription.getId());
        return Response.created(
            ui.getBaseUriBuilder()
            .path(MetricsResource.class)
            .path("{id}")
            .build(id))
            .entity(new HashMap<String, Long>(){{put("id",id);}})
            .build();  
    }
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/subscription/{subscriptionId}")
    public CounterSubscription getSubscription(@PathParam("subscriptionId") Long subscriptionId){
        return counterStorage.loadCounterSubscriptionById(subscriptionId);
    }
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{appId}")
    public List<RolledUpCounter> getRolledUpCounter(@PathParam("appId") final String appId, 
        @QueryParam("fromDate") final String fromDate, 
        @QueryParam("toDate") final String toDate,
        @QueryParam("aggregateByMonth") final String aggregateByMonth,
        @QueryParam("includeDistribution") final String includeDistribution,
        @QueryParam("counterType") final List<String> counterTypes)
    {
        if(Strings.isNullOrEmpty(appId))
        {
            return Lists.newArrayList();
        }
        Set<String> counterTypesSet = counterTypes == null?null:new HashSet<String>(counterTypes);
        
        return rollUpCounterProcessor.loadAggregatedRolledUpCounters(appId, 
            Optional.fromNullable(fromDate), 
            Optional.fromNullable(toDate), 
            Optional.fromNullable(counterTypesSet), 
            (!Strings.isNullOrEmpty(aggregateByMonth) && Objects.equal("y", aggregateByMonth.toLowerCase())),
            (Strings.isNullOrEmpty(includeDistribution) || !Objects.equal("y", includeDistribution.toLowerCase())));
    }
    
    

}
