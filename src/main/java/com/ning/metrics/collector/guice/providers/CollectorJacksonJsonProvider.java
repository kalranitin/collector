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
package com.ning.metrics.collector.guice.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Inject;

import javax.inject.Provider;

public class CollectorJacksonJsonProvider implements Provider<JacksonJsonProvider>
{
    private final ObjectMapper mapper;
    
    @Inject
    public CollectorJacksonJsonProvider(final ObjectMapper mapper){
        this.mapper = mapper;
    }

    @Override
    public JacksonJsonProvider get()
    {
       return new JacksonJsonProvider(mapper);
    }

    

}
