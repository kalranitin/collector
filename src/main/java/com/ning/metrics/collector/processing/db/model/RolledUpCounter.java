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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Map.Entry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This class represents a report of counter information over a given time range
 * for a given namespace.
 * @author kguthrie
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RolledUpCounter
{
    private final String namespace;
    private final DateTime fromDate;
    private final DateTime toDate;
    private final Map<String, RolledUpCounterData> counterSummary;

    public static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);


    @JsonCreator
    public RolledUpCounter(
            @JsonProperty("namespace") String namespace,
            @JsonProperty("fromDate") DateTime fromDate,
            @JsonProperty("toDate") DateTime toDate,
            @JsonProperty("counterSummary")
                    Map<String, RolledUpCounterData> counterSummary) {
        this.namespace = namespace;
        this.fromDate = fromDate;
        this.toDate = toDate;

        if(counterSummary == null) {
            this.counterSummary = Maps.newHashMap();
        }
        else {
            this.counterSummary = Maps.newHashMap(counterSummary);
        }
    }

    public RolledUpCounter(String namespace,
            DateTime fromDate, DateTime toDate) {
        this.namespace = namespace;
        this.fromDate = fromDate;
        this.toDate = toDate;
        counterSummary = Maps.newHashMap();
    }

    /**
     * get the id for this rolled up counter which is the namespace and
     * start date delimited with a '|'
     * @return
     */
    @JsonIgnore
    public String getId() {
        StringBuilder result = new StringBuilder();
        result.append(namespace);
        result.append('|');
        result.append(getFromDate());

        return result.toString();
    }

    public String getNamespace()
    {
        return namespace;
    }

    @JsonIgnore
    public DateTime getFromDateActual() {
        return fromDate;
    }

    @JsonIgnore
    public DateTime getToDateActual() {
        return toDate;
    }

    public String getFromDate() {
        return DATE_FORMATTER.print(getFromDateActual());
    }

    public String getToDate() {
        return DATE_FORMATTER.print(getToDateActual());
    }

    public Map<String, RolledUpCounterData> getCounterSummary()
    {
        return counterSummary;
    }

    /**
     * Incorporate the the given counter event data into this rolled up
     * counter
     * @param counterEventData
     */
    public void updateRolledUpCounterData(CounterEventData counterEventData) {

        for(Entry<String, Integer> counterEntry
                : counterEventData.getCounters().entrySet()) {
            String counterName = counterEntry.getKey();
            Integer counter = counterEntry.getValue();

            RolledUpCounterData rolledUpCounterData =
                    counterSummary.get(counterName);

            if(null == rolledUpCounterData) {
                rolledUpCounterData = new RolledUpCounterData(counterName);

                counterSummary.put(counterName, rolledUpCounterData);
            }

            rolledUpCounterData.incrementCounter(counter);

            rolledUpCounterData.incrementDistributionCounter(
                    counterEventData.getUniqueIdentifier(), counter);
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
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
        if (namespace == null) {
            if (other.namespace != null)
                return false;
        }
        else if (!namespace.equals(other.namespace))
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


}
