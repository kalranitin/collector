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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonSerialize(using = RolledUpCounter.RolledUpCounterSerializer.class)
@JsonDeserialize(using = RolledUpCounter.RolledUpCounterDeserializer.class)
public class RolledUpCounter
{
    private final String appId;
    private final DateTime fromDate;
    private final DateTime toDate;
    private final Table<String, String, RolledUpCounterData> counterSummary;

    public final static String COUNTER_SUMMARY_PREFIX = "counterSummary_";
    public final static String APP_ID_KEY = "appId";
    public final static String FROM_DATE_KEY = "fromDate";
    public final static String TO_DATE_KEY = "toDate";
    public final static String UNIQUES_KEY = "uniques";
    public static final DateTimeFormatter ROLLUP_COUNTER_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);


    public RolledUpCounter(final String appId,
        final DateTime fromDate,
        final DateTime toDate,
        final Table<String, String, RolledUpCounterData> counterSummary)
    {
        this.appId = appId;
        this.fromDate = fromDate;
        this.toDate = toDate;

        if(counterSummary == null)
        {
            this.counterSummary = HashBasedTable.create();
        }
        else
        {
            this.counterSummary = HashBasedTable.create(counterSummary);
        }
    }

    public RolledUpCounter(final String appId,
        final DateTime fromDate,
        final DateTime toDate)
    {
        this.appId = appId;
        this.fromDate = fromDate;
        this.toDate = toDate;
        counterSummary = HashBasedTable.create();
    }

    public String getAppId()
    {
        return appId;
    }
    public DateTime getFromDate()
    {
        if(this.fromDate != null)
        {
            try {
                return new DateTime(this.fromDate,DateTimeZone.UTC);
            }
            catch (Exception e) {
                return new DateTime(DateTimeZone.UTC);
            }
        }

        return new DateTime(DateTimeZone.UTC);
    }
    public DateTime getToDate()
    {
        if(this.toDate != null)
        {
            try {
                return new DateTime(this.toDate,DateTimeZone.UTC);
            }
            catch (Exception e) {
                return new DateTime(DateTimeZone.UTC);
            }
        }

        return new DateTime(DateTimeZone.UTC);
    }

    @JsonIgnore
    public String getFormattedDate()
    {
        return ROLLUP_COUNTER_DATE_FORMATTER.print(getFromDate());
    }

    public Table<String, String, RolledUpCounterData> getCounterSummary()
    {
        return counterSummary;
    }

    public void updateRolledUpCounterData(final CounterEventData counterEventData, final List<String> identifierDistribution)
    {
     // This creates a string with e.g. counterRowName_1
        final String counterRowName = COUNTER_SUMMARY_PREFIX + counterEventData.getIdentifierCategory();

        final Map<String,RolledUpCounterData> counterSummaryRow = counterSummary.row(counterRowName);

        for(final Entry<String, Integer> counterEntry : counterEventData.getCounters().entrySet())
        {
            final String counterName = counterEntry.getKey();
            final Integer counter = counterEntry.getValue();

            RolledUpCounterData rolledUpCounterData = counterSummaryRow.get(counterName);
            if(Objects.equal(null, rolledUpCounterData))
            {
                rolledUpCounterData = new RolledUpCounterData(counterName, 0, new ConcurrentHashMap<String, Integer>());
                counterSummaryRow.put(counterName, rolledUpCounterData);
            }

            rolledUpCounterData.incrementCounter(counter);

            if(!Objects.equal(null, identifierDistribution) && identifierDistribution.contains(counterName))
            {
                rolledUpCounterData.incrementDistributionCounter(counterEventData.getUniqueIdentifier(), counter);
            }
        }
    }

    public void evaluateUniques()
    {
        Set<String> uniqueKeySet = Sets.newConcurrentHashSet();

        if(!Objects.equal(null, counterSummary) && !counterSummary.isEmpty())
        {
            for(String rowName : counterSummary.rowKeySet())
            {
                for(RolledUpCounterData rolledUpCounterData : counterSummary.row(rowName).values())
                {
                    if(!Objects.equal(null, rolledUpCounterData.getDistribution()))
                    {
                        uniqueKeySet = Sets.union(uniqueKeySet, rolledUpCounterData.getDistribution().keySet());
                    }
                }

                counterSummary.put(rowName, UNIQUES_KEY, new RolledUpCounterData(UNIQUES_KEY, uniqueKeySet.size(), null));
            }
        }

    }

    public void aggregateCounterDataFor(Set<String> counterNames,
            boolean excludeDistribution, Optional<Integer> distributionLimit,
            Optional<Set<String>> uniqueIds)
    {
        if(!Objects.equal(null, counterNames) && !counterNames.isEmpty())
        {
            if(!Objects.equal(null, counterSummary) && !counterSummary.isEmpty())
            {
                counterNames.add(UNIQUES_KEY);

                Set<String> columnNames = counterSummary.columnKeySet();
                Iterables.retainAll(columnNames, counterNames);

                for(String rowName : counterSummary.rowKeySet())
                {
                    counterSummary.remove(rowName, columnNames);
                }
            }
        }

        for(RolledUpCounterData counterData : counterSummary.values())
        {
            if(!Objects.equal(null, counterData.getDistribution()) && !counterData.getDistribution().isEmpty())
            {
                if(excludeDistribution)
                {
                    counterData.truncateDistribution();
                }
                else if (uniqueIds != null && uniqueIds.isPresent()) {
                    counterData.applyDistributionFilterById(uniqueIds.get());
                }
                else if(distributionLimit != null && distributionLimit.isPresent()){
                    counterData.applyDistributionLimit(distributionLimit.get());
                }
            }
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((appId == null) ? 0 : appId.hashCode());
        result = prime * result + ((fromDate == null) ? 0 : fromDate.hashCode());
        result = prime * result + ((toDate == null) ? 0 : toDate.hashCode());
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
        RolledUpCounter other = (RolledUpCounter) obj;
        if (appId == null) {
            if (other.appId != null)
                return false;
        }
        else if (!appId.equals(other.appId))
            return false;
        if (fromDate == null) {
            if (other.fromDate != null)
                return false;
        }
        else if (!fromDate.equals(other.fromDate))
            return false;
        if (toDate == null) {
            if (other.toDate != null)
                return false;
        }
        else if (!toDate.equals(other.toDate))
            return false;
        return true;
    }



    public static class RolledUpCounterSerializer extends JsonSerializer<RolledUpCounter>{

        @Override
        public void serialize(RolledUpCounter rolledUpCounter, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException
        {
            jgen.writeStartObject();

            jgen.writeFieldName(APP_ID_KEY);
            jgen.writeObject(rolledUpCounter.getAppId());

            jgen.writeFieldName(FROM_DATE_KEY);
            jgen.writeObject(ROLLUP_COUNTER_DATE_FORMATTER.print(rolledUpCounter.getFromDate()));

            jgen.writeFieldName(TO_DATE_KEY);
            jgen.writeObject(ROLLUP_COUNTER_DATE_FORMATTER.print(rolledUpCounter.getToDate()));

            for(String key : rolledUpCounter.getCounterSummary().rowKeySet()){
                jgen.writeFieldName(key);
                jgen.writeObject(rolledUpCounter.getCounterSummary().row(key));
            }


            jgen.writeEndObject();

        }

    }

    public static class RolledUpCounterDeserializer extends JsonDeserializer<RolledUpCounter>{

        @Override
        public RolledUpCounter deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException
        {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JodaModule());
            mapper.registerModule(new GuavaModule());

            Table<String, String, RolledUpCounterData> counterSummary = HashBasedTable.create();

            ObjectCodec codec = jp.getCodec();
            JsonNode node = codec.readTree(jp);

            String appId = node.get(APP_ID_KEY).asText();
            DateTime fromDate = new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis(node.get(FROM_DATE_KEY).asText()),DateTimeZone.UTC);
            DateTime toDate = new DateTime(RolledUpCounter.ROLLUP_COUNTER_DATE_FORMATTER.parseMillis(node.get(TO_DATE_KEY).asText()),DateTimeZone.UTC);

            Iterator<String> fields = node.fieldNames();

            while(fields.hasNext())
            {
                String fieldName = fields.next();
                if(fieldName.startsWith(COUNTER_SUMMARY_PREFIX))
                {
                    Iterator<Entry<String, JsonNode>> nodeIterator = node.get(fieldName).fields();

                    while(nodeIterator.hasNext())
                    {
                        JsonNode childNode = nodeIterator.next().getValue();
                        RolledUpCounterData rolledUpCounterData = mapper.readValue(childNode.toString(), RolledUpCounterData.class);
                        counterSummary.put(fieldName, rolledUpCounterData.getCounterName(), rolledUpCounterData);
                    }

                }
            }


            return new RolledUpCounter(appId,fromDate,toDate,counterSummary);
        }

    }


}
