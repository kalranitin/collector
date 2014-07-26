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
package com.ning.metrics.collector.processing.db.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CounterEvent
{
    private final String namespace;
    private final List<CounterEventData> counterEvents;

    @JsonCreator
    public CounterEvent(@JsonProperty("namespace") String namespace,
        @JsonProperty("buckets") List<CounterEventData> counterEvents)
    {
        this.namespace = namespace;
        if (null == counterEvents)        {
            this.counterEvents = null;
        }
        else
        {
            this.counterEvents = ImmutableList.copyOf(counterEvents);
        }

    }

    public String getNamespace()
    {
        return namespace;
    }

    @JsonProperty("buckets")
    public List<CounterEventData> getCounterEvents()
    {
        return counterEvents;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
        result = prime * result + ((counterEvents == null) ? 0 : counterEvents.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CounterEvent other = (CounterEvent) obj;
        if (namespace == null) {
            if (other.namespace != null)
                return false;
        }
        else if (!namespace.equals(other.namespace))
            return false;
        if (counterEvents == null) {
            if (other.counterEvents != null)
                return false;
        }
        else if (!counterEvents.equals(other.counterEvents))
            return false;
        return true;
    }
}
