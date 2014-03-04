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

import com.ning.metrics.collector.processing.db.FeedEventStorage;
import com.ning.metrics.collector.processing.db.model.FeedEvent;

import com.google.common.base.Strings;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.Collections;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * Internal Channel event API
 */
@Path("/rest/1.0/channel")
public class ChannelResource
{

    private final FeedEventStorage feedEventStorage;
	
    @Inject
    public ChannelResource(final FeedEventStorage feedEventStorage)
    {
        this.feedEventStorage = feedEventStorage;
    }

    
    @GET
    @Path("/{channelKey}")
    @Produces(MediaType.APPLICATION_JSON)
    public Collection<FeedEvent> showChannels(@PathParam("channelKey") String channelKey, 
            @QueryParam("eventOffsetId") @DefaultValue("0") final long eventOffsetId, 
            @QueryParam("count") final int count) 
    {
        if (Strings.isNullOrEmpty(channelKey)) {
            return Collections.emptyList();
        }

        return feedEventStorage.loadFeedEventsByOffset(channelKey, eventOffsetId, count);
    }

}
