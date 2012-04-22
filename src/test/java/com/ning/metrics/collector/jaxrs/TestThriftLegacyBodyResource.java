/*
 * Copyright 2010-2012 Ning, Inc.
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

import com.ning.metrics.collector.endpoint.extractors.DeserializationType;

import javax.ws.rs.core.Response;

public class TestThriftLegacyBodyResource extends TestResources<BodyResource>
{
    protected TestThriftLegacyBodyResource()
    {
        super(DeserializationType.DEFAULT);
    }

    @Override
    public BodyResource setupResource(final EventDeserializerRequestHandler requestHandler)
    {
        return new BodyResource(requestHandler);
    }

    @Override
    public Response callEndpoint()
    {
        return resource.postThriftLegacy(EVENT_NAME, EVENT_DATETIME.toString(), EVENT_GRANULARITY.toString(), new byte[]{}, httpHeaders, request);
    }
}
