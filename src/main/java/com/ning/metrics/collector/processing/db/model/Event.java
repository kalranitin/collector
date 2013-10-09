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

@JsonSerialize(using = Event.EventSerializer.class)
public class Event
{
    private final Map<String, Object> data = new ConcurrentHashMap<String, Object>();
    private final List<String> targets = new CopyOnWriteArrayList<String>();


    //    @JsonCreator
    public Event()
    {

    }

    public Event(Map<String, Object> map)
    {
        this.data.putAll(map);
    }

    public Event(String target, Map<String, Object> data)
    {
        this.targets.add(target);
        this.data.putAll(data);
    }

    @JsonAnySetter
    public void setAttribute(String key, Object value)
    {
        if ("targets".equals(key)) {
            this.targets.addAll((Collection) value);
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

    

    public List<String> getTargets()
    {
        return targets;
    }

    public static class EventSerializer extends JsonSerializer<Event>
    {
        @Override
        public void serialize(Event event, JsonGenerator jgen, SerializerProvider sp) throws IOException, JsonProcessingException
        {
            jgen.writeStartObject();
            jgen.writeFieldName("targets");
            jgen.writeObject(event.getTargets());
            for (Map.Entry<String, Object> entry : event.getData().entrySet()) {
                if (!"targets".equals(entry.getKey())) {
                    jgen.writeFieldName(entry.getKey());
                    jgen.writeObject(entry.getValue());
                }
            }
            jgen.writeEndObject();
        }
    }
}
