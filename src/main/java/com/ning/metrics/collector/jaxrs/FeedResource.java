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

import com.ning.metrics.collector.processing.db.DatabaseFeedStorage;
import com.ning.metrics.collector.processing.db.model.Feed;
import com.ning.metrics.collector.processing.db.model.FeedEvent;

import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Feed API
 * */
@Path("/feed")
public class FeedResource
{
    private static final Logger log = LoggerFactory.getLogger(FeedResource.class);
    private final DatabaseFeedStorage databaseFeedStorage;
    
    @Inject
    public FeedResource(final DatabaseFeedStorage databaseFeedStorage){
        this.databaseFeedStorage = databaseFeedStorage;
    }
    
    @GET
    @Path("/{feedKey}")
    @Produces(MediaType.APPLICATION_JSON)
    public Collection<FeedEvent> getFeedEvents(@PathParam("feedKey") String feedKey) {
        Feed feed = databaseFeedStorage.loadFeedByKey(feedKey);
        return feed == null? null : feed.getFeedEvents();
        
    }
    
    @DELETE
    @Path("/{feedKey}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteFeed(@PathParam("feedKey") final String feedKey) {
        databaseFeedStorage.deleteFeed(feedKey);
        return Response.ok().build();
    }

    @DELETE
    @Path("/{feedKey}/{contentId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteFeedItem(@PathParam("feedKey") final String feedKey,
                                   @PathParam("contentId") final String contentId) {
        
        Feed feed = databaseFeedStorage.loadFeedByKey(feedKey);
        if(feed != null)
        {
            if(feed.deleteFeedEvent(contentId))
            {
                databaseFeedStorage.addOrUpdateFeed(feedKey, feed);
            }
        }
        
        return Response.ok().build();
        
    }

}
