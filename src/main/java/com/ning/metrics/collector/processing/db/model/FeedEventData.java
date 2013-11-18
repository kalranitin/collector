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
import com.google.common.base.Objects;
import com.google.common.base.Strings;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@JsonSerialize(using = FeedEventData.FeedEventDataSerializer.class)
public class FeedEventData
{
    private final Map<String, Object> data = new ConcurrentHashMap<String, Object>();
    private final List<String> topics = new CopyOnWriteArrayList<String>();
    private final List<String> removalTargets = new CopyOnWriteArrayList<String>();
    private String feedEventId = "";
    private String eventType = "";
    public final static String FEED_EVENT_ID_KEY = "feedEventId";
    public final static String EVENT_TYPE_KEY = "eventType";
    public final static String CREATED_DATE_KEY = "createdDate";
    public final static String TOPICS_KEY = "topics";
    public final static String REMOVAL_TARGETS = "removalTargets";
    public final static String EVENT_TYPE_SUPPRESS = "eventSuppress";
    
    //    @JsonCreator
    public FeedEventData()
    {

    }

    public FeedEventData(Map<String, Object> map)
    {
        this.data.putAll(map);
    }

    public FeedEventData(String topic, Map<String, Object> data)
    {
        this.topics.add(topic);
        this.data.putAll(data);
    }

    @JsonAnySetter
    public void setAttribute(String key, Object value)
    {
        if (TOPICS_KEY.equals(key)) {
            this.topics.addAll((Collection) value);
        }
        else if(FEED_EVENT_ID_KEY.equals(key)){
            this.feedEventId = (String) value;
        }
        else if(EVENT_TYPE_KEY.equals(key)){
            this.eventType = (String) value;
        }
        else if (REMOVAL_TARGETS.equals(key)) {
            this.removalTargets.addAll((Collection) value);
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

    public String getFeedEventId(){
        if(Strings.isNullOrEmpty(this.feedEventId))
        {
            this.feedEventId = UUID.randomUUID().toString();
        }
        return feedEventId;
    }
    
    public String getEventType(){
        return eventType;
    }
    
    public DateTime getCreatedDate(){
        if(data.get(CREATED_DATE_KEY) != null)
        {
            try {
                return new DateTime(data.get(CREATED_DATE_KEY),DateTimeZone.UTC);
            }
            catch (Exception e) {
                return new DateTime(DateTimeZone.UTC);
            }
        }
        
        return new DateTime(DateTimeZone.UTC);
    }

    public List<String> getTopics()
    {
        return topics;
    }
    
    public List<String> getRemovalTargets()
    {
        return removalTargets;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((feedEventId == null) ? 0 : feedEventId.hashCode());
        result = prime * result + ((eventType == null) ? 0 : eventType.hashCode());
        result = prime * result + ((topics == null) ? 0 : topics.hashCode());
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
        FeedEventData other = (FeedEventData) obj;
        if (feedEventId == null) {
            if (other.feedEventId != null)
                return false;
        }
        else if (!feedEventId.equals(other.feedEventId))
            return false;
        if (eventType == null) {
            if (other.eventType != null)
                return false;
        }
        else if (!eventType.equals(other.eventType))
            return false;
        if (topics == null) {
            if (other.topics != null)
                return false;
        }
        else if (!topics.equals(other.topics))
            return false;
        return true;
    }



    public static class FeedEventDataSerializer extends JsonSerializer<FeedEventData>
    {
        @Override
        public void serialize(FeedEventData event, JsonGenerator jgen, SerializerProvider sp) throws IOException, JsonProcessingException
        {
            jgen.writeStartObject();
            jgen.writeFieldName(TOPICS_KEY);
            jgen.writeObject(event.getTopics());
            jgen.writeFieldName(FEED_EVENT_ID_KEY);
            jgen.writeObject(event.getFeedEventId());
            jgen.writeFieldName(EVENT_TYPE_KEY);
            jgen.writeObject(event.getEventType());
            jgen.writeFieldName(REMOVAL_TARGETS);
            jgen.writeObject(event.getRemovalTargets());
            
            for (Map.Entry<String, Object> entry : event.getData().entrySet()) {
                if (!TOPICS_KEY.equals(entry.getKey()) 
                        && !FEED_EVENT_ID_KEY.equals(entry.getKey()) 
                        && !EVENT_TYPE_KEY.equals(entry.getKey())
                        && !REMOVAL_TARGETS.equals(entry.getKey())) {
                    jgen.writeFieldName(entry.getKey());
                    jgen.writeObject(entry.getValue());
                }
            }
            jgen.writeEndObject();
        }
    }
}
