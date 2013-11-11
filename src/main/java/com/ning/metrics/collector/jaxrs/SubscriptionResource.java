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
package com.ning.metrics.collector.jaxrs;

import com.ning.metrics.collector.processing.db.SubscriptionStorage;
import com.ning.metrics.collector.processing.db.model.Subscription;

import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Subscription API
 * */
@Path("/rest/1.0/subscription")
public class SubscriptionResource
{
    private static final Logger log = LoggerFactory.getLogger(SubscriptionResource.class);
    private final SubscriptionStorage subscriptionStorage;
    
    @Inject
    public SubscriptionResource(final SubscriptionStorage subscriptionStorage)
    {
        this.subscriptionStorage = subscriptionStorage;
    }
    
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createSubscription(final Subscription subscription, @Context UriInfo ui){
        
        Long key = subscriptionStorage.insert(subscription);
        
        return Response.created(
            ui.getBaseUriBuilder()
            .path(SubscriptionResource.class)
            .path("{id}")
            .build(key))
            .build();        
    }
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    public Subscription getSubscription(@PathParam("id") Long id){
        return subscriptionStorage.loadSubscriptionById(id);
    }
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{feed}")
    public Collection<Subscription> getSubscriptionsByFeed(@PathParam("feed") String feed)
    {
        return subscriptionStorage.loadByFeed(feed);
        
    }
    
    @DELETE
    @Path("{id}")
    public void deleteSubscription(@PathParam("id") Long id){
        if(!subscriptionStorage.deleteSubscriptionById(id))
        {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
    }
    
    
}
