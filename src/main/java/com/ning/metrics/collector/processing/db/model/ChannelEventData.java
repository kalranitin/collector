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
package com.ning.metrics.collector.processing.db.model;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@JsonSerialize(using = ChannelEventData.ChannelEventDataSerializer.class)
public class ChannelEventData
{
    private final Map<String, Object> data = new ConcurrentHashMap<String, Object>();
    private final List<String> topics = new CopyOnWriteArrayList<String>();


    //    @JsonCreator
    public ChannelEventData()
    {

    }

    public ChannelEventData(Map<String, Object> map)
    {
        this.data.putAll(map);
    }

    public ChannelEventData(String topic, Map<String, Object> data)
    {
        this.topics.add(topic);
        this.data.putAll(data);
    }

    @JsonAnySetter
    public void setAttribute(String key, Object value)
    {
        if ("topics".equals(key)) {
            this.topics.addAll((Collection) value);
        }
        else {
            data.put(key, value);
        }
    }

    @JsonView
    public Map<String, Object> getData()
    {
        return data;
    }

    

    public List<String> getTopics()
    {
        return topics;
    }

    public static class ChannelEventDataSerializer extends JsonSerializer<ChannelEventData>
    {
        @Override
        public void serialize(ChannelEventData event, JsonGenerator jgen, SerializerProvider sp) throws IOException, JsonProcessingException
        {
            jgen.writeStartObject();
            jgen.writeFieldName("topics");
            jgen.writeObject(event.getTopics());
            for (Map.Entry<String, Object> entry : event.getData().entrySet()) {
                if (!"topics".equals(entry.getKey())) {
                    jgen.writeFieldName(entry.getKey());
                    jgen.writeObject(entry.getValue());
                }
            }
            jgen.writeEndObject();
        }
    }
}
