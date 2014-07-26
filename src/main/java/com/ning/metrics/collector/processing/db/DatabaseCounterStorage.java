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
package com.ning.metrics.collector.processing.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.ning.metrics.collector.binder.config.CollectorConfig;
import com.ning.metrics.collector.processing.counter.CounterDistribution;
import com.ning.metrics.collector.processing.db.model.CounterEventData;
import com.ning.metrics.collector.processing.db.model.RolledUpCounter;
import com.ning.metrics.collector.processing.db.model.RolledUpCounterData;
import com.ning.metrics.collector.processing.db.util.MySqlLock;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.skife.config.TimeSpan;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.StringMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an implementation of the counter storage interface that stores
 * counters in a mysql database
 * @author kguthrie
 */
public class DatabaseCounterStorage implements CounterStorage {

    /**
     * jdbi interface that allows for batch operations on daily rolled-up
     * counters
     */
    public static interface DailyRolledUpCounters {

        /**
         * Batch insert of a set of daily counter data that comprise a single
         * rolled-up counter
         * @param namespace common namespace for this batch of inserts
         * @param datestamp common datestamp for this batch of inserts
         * @param counterNames list of names of counters in the roll up
         * @param totalCounts list of total counts for each counter name
         * @param uniqueCounts list of unique counts for each counter name
         * @param distributions list of serialized distributions f.e. counter
         */
        @SqlBatch("INSERT INTO `metrics_daily` (`namespace`, `datestamp`, "
                + "counter_name, total_count, unique_count, distribution) "
                + "VALUES (:namespace, :datestamp, :counterName, :totalCount, "
                + ":uniqueCount, :distribution) "
                + "ON DUPLICATE KEY UPDATE "
                + "`total_count` = :totalCount, "
                + "`unique_count` = :uniqueCount, "
                + "`distribution` = :distribution")
        void insertRolledUpCounter(
                @Bind("namespace") String namespace,
                @Bind("datestamp") String datestamp,
                @Bind("counterName") List<String> counterNames,
                @Bind("totalCount") List<Integer> totalCounts,
                @Bind("uniqueCount") List<Integer> uniqueCounts,
                @Bind("distribution") List<byte[]> distributions);

        /**
         * Select a complete rolled up counter from the database by its date and
         * namespace
         * @param namespace
         * @param counterDate
         * @return
         */
        @SqlQuery("SELECT * FROM metrics_daily WHERE "
                + "`namespace` = :namespace AND `datestamp` = :datestamp")
        @Mapper(value = SingleCompleteRolledUpCounterMapper.class)
        List<RolledUpCounter> getById(
                @Bind("namespace") String namespace,
                @Bind("datestamp") String counterDate);
    }

    private static final Logger log =
            LoggerFactory.getLogger(DatabaseCounterStorage.class);
    public static final DateTimeFormatter DAILY_METRICS_DATE_FORMAT =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(DateTimeZone.UTC);
    private final IDBI dbi;
    private final CollectorConfig config;
    private final Lock dbLock;
    private final ObjectMapper mapper;
    final TimeSpan cacheExpiryTime;

    @Inject
    public DatabaseCounterStorage(final IDBI dbi, final CollectorConfig config, final ObjectMapper mapper)
    {
        this.dbi = dbi;
        this.config = config;
        this.dbLock = new MySqlLock("counter-event-storage", dbi);
        this.cacheExpiryTime = config.getSubscriptionCacheTimeout();
        this.mapper = mapper;
    }

    /**
     * Insert the given counter data into the metrics buffer
     * @param dailyCounters Map of namespace to the set of counter events
     *      observed in that namespace
     */
    @Override
    public void bufferMetrics(
            final Multimap<String, CounterEventData> dailyCounters) {
       dbi.withHandle(new HandleCallback<Void>() {

        @Override
        public Void withHandle(Handle handle) throws Exception {

            PreparedBatch batch = handle.prepareBatch(
                    "insert into metrics_buffer "
                            + "(`namespace`,`metrics`,`timestamp`) values "
                            + "(:namespace, :metrics, :timestamp)");

            for(Entry<String, CounterEventData> entry
                    : dailyCounters.entries()) {

                batch.bind("namespace", entry.getKey())
                        .bind("metrics", mapper.writeValueAsString(entry.getValue()))
                        .bind("timestamp", DAILY_METRICS_DATE_FORMAT.print(
                                entry.getValue().getCreatedTime()))
                        .add();
            }

            batch.execute();

            return null;
        }});
    }

    /**
     * Pagenated method for loading metrics events for a given namespace and a
     * given time range
     * @param namespace namespace of the metrics events to un buffer
     * @param toDateTime upper limit of timestamps of retrieved events
     *
     * @param limit
     * @param offset
     * @return
     */
    @Override
    public List<CounterEventData> loadBufferedMetricsPaged(
            final String namespace, final DateTime toDateTime,
            final Integer limit, final Integer offset) {

        return dbi.withHandle(new HandleCallback<List<CounterEventData>>() {

        @Override
        public List<CounterEventData> withHandle(Handle handle)
                throws Exception {

            // Everything by the namespace in this method is optional.  These
            // wrapper objects make that fact explicit and easier to work with
            // albeit a little redundtant
            final Optional<DateTime> toDateTimeOptional =
                    Optional.fromNullable(toDateTime);
            final Optional<Integer> limitOptional =
                    Optional.fromNullable(limit);
            final Optional<Integer> offsetOptional =
                    Optional.fromNullable(offset);

            final StringBuilder queryStr = new StringBuilder();

            // Build the query based on the optionals

            queryStr.append("select metrics from metrics_buffer "
                    + "where namespace = :namespace");

            queryStr.append(toDateTimeOptional.isPresent()
                    ? " and `timestamp` <= :toDateTime" : "");

            queryStr.append(limitOptional.isPresent()
                    ? " limit :limit" : "");

            queryStr.append(
                    limitOptional.isPresent() && offsetOptional.isPresent()
                            ? " offset :offset" : "");

            Query<Map<String, Object>> query =
                    handle.createQuery(queryStr.toString())
                            .bind("namespace", namespace);

            // Bind present paramters into the query
            if(toDateTimeOptional.isPresent()) {
                query.bind("toDateTime", DAILY_METRICS_DATE_FORMAT.print(
                        toDateTimeOptional.get()));
            }
            if(limitOptional.isPresent()) {
                query.bind("limit", limitOptional.get());

                if(offsetOptional.isPresent()) {
                    query.bind("offset", offsetOptional.get());
                }
            }

            return ImmutableList.copyOf(query.map(
                    new CounterEventDataMapper(mapper)).list());

        }});
    }

    /**
     * method for loading metrics events for a given namespace and a
     * given time range
     *
     * @param namespace
     * @param toDateTime end date for this query
     * @return
     */
    @Override
    public List<CounterEventData> loadBufferedMetrics(
            final String namespace, final DateTime toDateTime) {

        return dbi.withHandle(new HandleCallback<List<CounterEventData>>() {

        @Override
        public List<CounterEventData> withHandle(Handle handle)
                throws Exception {

            StringBuilder queryStr = new StringBuilder();
            queryStr.append("select metrics from metrics_buffer where "
                    + "`namespace` = :namespace");

            queryStr.append(Objects.equal(null, toDateTime)
                    ? "" : " and `timestamp` <= :toDateTime");

            Query<Map<String, Object>> query =
                    handle.createQuery(queryStr.toString())
                            .bind("namespace", namespace);

            if(!Objects.equal(null, toDateTime))
            {
                query.bind("toDateTime",
                        DAILY_METRICS_DATE_FORMAT.print(toDateTime));
            }

            Map<String,CounterEventData> groupMap = Maps.newHashMap();

            ResultIterator<CounterEventData> rs = query.map(
                    new CounterEventDataMapper(mapper)).iterator();

            try {
                while(rs.hasNext()) {

                    CounterEventData counterEventData = rs.next();
                    String counterKey = counterEventData.getUniqueIdentifier()
                            + counterEventData.getFormattedDate();
                    CounterEventData groupedData = groupMap.get(counterKey);

                    if(Objects.equal(null, groupedData)) {
                        groupMap.put(counterKey, counterEventData);
                        continue;
                    }

                    groupedData.mergeCounters(counterEventData.getCounters());
                    groupMap.put(counterKey, groupedData);
                }
            }
            finally{
                rs.close();
            }



            return ImmutableList.copyOf(groupMap.values());

        }});
    }

    /**
     * This method will delete the set of buffered metrics for the given time
     * range and namespace
     * @param namespace
     * @param toDateTime
     * @return
     */
    @Override
    public boolean deleteBufferedMetrics(final String namespace,
            final DateTime toDateTime){
        int deleted = dbi.withHandle(new HandleCallback<Integer>() {

        @Override
        public Integer withHandle(Handle handle) throws Exception {

            StringBuilder queryStr = new StringBuilder();
            queryStr.append("delete from metrics_buffer where "
                    + "`namespace` = :namespace");

            queryStr.append(Objects.equal(null, toDateTime)
                    ? "" : " and `timestamp` <= :toDateTime");

            Update query = handle.createStatement(queryStr.toString())
                            .bind("namespace", namespace);

            if(!Objects.equal(null, toDateTime))
            {
                query.bind("toDateTime",
                        DAILY_METRICS_DATE_FORMAT.print(toDateTime));
            }

            return query.execute();
        }});

        return deleted > 0;
    }

    /**
     * selects the unique namespaces currently found in the buffered metrics
     * @return
     */
    @Override
    public List<String> getNamespacesFromMetricsBuffer(){
        return dbi.withHandle(new HandleCallback<List<String>>() {

        @Override
        public List<String> withHandle(Handle handle) throws Exception
        {
            return ImmutableList.copyOf(handle.createQuery(
                    "select distinct(`namespace`) from metrics_buffer")
                            .map(StringMapper.FIRST).list());

        }});
    }

    /**
     * Insert or update the given rolled up counter into the database. This
     * method will split the rolled up counter into its component Rolled Up
     * Counter datas and distributions insert those individually
     * @param rolledUpCounter
     * @return
     */
    @Override
    public String insertOrUpdateDailyRolledUpCounter(
            final RolledUpCounter rolledUpCounter) {
        return dbi.withHandle(new HandleCallback<String>() {

            @Override
            public String withHandle(Handle handle) throws Exception
            {
                List<String> counterNames = Lists.newArrayList();
                List<Integer> totalCounts = Lists.newArrayList();
                List<Integer> uniqueCounts = Lists.newArrayList();
                List<byte[]> distributions = Lists.newArrayList();

                String namespace = rolledUpCounter.getNamespace();
                String counterDate = rolledUpCounter.getFormattedDate();

                for (Map.Entry<String, RolledUpCounterData> e
                        : rolledUpCounter.getCounterSummary().entrySet()) {
                    counterNames.add(e.getKey());
                    totalCounts.add(e.getValue().getTotalCount());
                    uniqueCounts.add(e.getValue().getUniqueCount());
                    distributions.add(serializeDistribution(e.getValue()));
                }

                // Some jdbi magic happens here:
                DailyRolledUpCounters operator =
                        handle.attach(DailyRolledUpCounters.class);

                operator.insertRolledUpCounter(namespace, counterDate,
                        counterNames, totalCounts, uniqueCounts, distributions);

                return rolledUpCounter.getId();
            }});
    }

    /**
     * serialize the given rolled-up counter data's distribution to a byte
     * array for storage in a blob
     * @param counter
     * @return
     * @throws java.io.IOException technically, but unlikely because all ops are
     *          in memory
     */
    public static byte[] serializeDistribution(RolledUpCounterData counter)
            throws IOException {

        ByteArrayOutputStream result = new ByteArrayOutputStream();
        GZIPOutputStream zipStream = new GZIPOutputStream(result);
        PrintStream printer = new PrintStream(zipStream, true, "UTF-8");

        int index = 0;

        // iterate through all the entries in the distribution and generate a
        // serialization of the form:
        //
        // uniqueId1|count1\n
        // uniqueId2|count2\n
        // ...
        //
        // and then gzip the result into a byte array
        for (Map.Entry<String, Integer> entry
                : counter.getDistribution().entrySet()) {
            String id = entry.getKey();
            int value = entry.getValue() == null ? 0 : entry.getValue();

            // Don't write unique ids that have a zero count
            if (value == 0) {
                continue;
            }

            if (index++ > 0) {
                printer.println();
            }

            printer.print(id);
            printer.print('|');
            printer.print(Integer.toString(value));
        }

        zipStream.finish();
        printer.close();

        return result.toByteArray();
    }

    /**
     * Read the serialized distribution from the given byte buffer and return
     * the inflated and deserialized version as a map
     * @param serialDist serialized version of the distribution
     * @param uniqueIds
     * @param distributionLimit
     * @return
     * @throws java.io.IOException
     */
    public static CounterDistribution deserializeDistribution(
            byte[] serialDist, Optional<Set<String>> uniqueIds,
                    Optional<Integer> distributionLimit) throws IOException {

        CounterDistribution result = new CounterDistribution();
        ByteArrayInputStream bis = new ByteArrayInputStream(serialDist);
        GZIPInputStream zip = new GZIPInputStream(bis);
        BufferedReader reader = new BufferedReader(new InputStreamReader(zip));

        String line;


        while ((line = reader.readLine()) != null) {

            line = line.trim();

            if (line.isEmpty()) {
                continue;
            }

            int split = line.indexOf('|');
            String uniqueId = line.substring(0, split);

            if (uniqueIds.isPresent()) {
                if (!uniqueIds.get().contains(uniqueId)) {
                    continue;
                }
            }

            int count = Integer.parseInt(line.substring(split + 1));

            result.putPresortedEntry(uniqueId, count);

            if (distributionLimit.isPresent()) {
                if (result.size() >= distributionLimit.get()) {
                    break;
                }
            }
        }

        return result;
    }

    /**
     * Load the daily rolled-up counter for the given id.  This is really a
     * composite object of all the daily rolled-up counter datas stored in the
     * database with ids that start with "namespace|counterDate"
     * @param namespace
     * @param counterDate
     * @return
     */
    @Override
    public RolledUpCounter loadDailyRolledUpCounter(final String namespace,
            final DateTime counterDate)
    {
        return dbi.withHandle(new HandleCallback<RolledUpCounter>()
        {
            @Override
            public RolledUpCounter withHandle(Handle handle) throws Exception {
                List<RolledUpCounter> rolledUpCounterList
                        = handle.attach(DailyRolledUpCounters.class).getById(
                                namespace, RolledUpCounter.DATE_FORMATTER.print(
                                        counterDate));
                return rolledUpCounterList.isEmpty()
                        ? null
                        : rolledUpCounterList.get(0);
            }
        });
    }

    /**
     * queries the daily rolled up counters based on counter names, date range,
     * and distribution facts
     * @param namespace
     * @param fromDate
     * @param toDate
     * @param fetchCounterNames
     * @param excludeDistribution
     * @param distributionLimit
     * @param unqiueIds
     * @return
     */
    @Override
    public List<RolledUpCounter> queryDailyRolledUpCounters(
            final String namespace,
            final DateTime fromDate, final DateTime toDate,
            final Optional<Set<String>> fetchCounterNames,
            final boolean excludeDistribution,
            final Optional<Integer> distributionLimit,
            final Optional<Set<String>> unqiueIds)
    {
        return dbi.withHandle(
                new HandleCallback<List<RolledUpCounter>>() {

        @Override
        public List<RolledUpCounter> withHandle(Handle handle) throws Exception {

            StringBuilder queryStr = new StringBuilder();

            queryStr.append("select `namespace`, `datestamp`, counter_name, "
                    + "total_count, unique_count");

            // Add the distribution to the set of returned columns if not
            // excluded
            if (!excludeDistribution) {
                queryStr.append(", distribution");
            }

            queryStr.append(" from metrics_daily "
                    + "where namespace = :namespace");

            // Add the optional query parameters
            if (fromDate != null) {
                queryStr.append(" and datestamp >= :fromDate");
            }
            if (toDate != null) {
                queryStr.append(" and datestamp <= :toDate");
            }
            if (fetchCounterNames != null
                    && fetchCounterNames.isPresent()
                    && !fetchCounterNames.get().isEmpty()) {
                queryStr.append(" and counter_name in (");

                for (int i = 0; i < fetchCounterNames.get().size(); i++) {

                    if (i > 0) {
                        queryStr.append(", ");
                    }

                    queryStr.append(":counterName_");
                    queryStr.append(i);
                }

                queryStr.append(")");
            }

            Query<Map<String, Object>> query =  handle.createQuery(
                    queryStr.toString()).bind("namespace", namespace);

            if(null != fromDate) {
                query.bind("fromDate",
                        RolledUpCounter.DATE_FORMATTER.print(fromDate));
            }
            if(null != toDate) {
                query.bind("toDate",
                        RolledUpCounter.DATE_FORMATTER.print(toDate));
            }
            if (fetchCounterNames != null
                    && fetchCounterNames.isPresent()
                    && !fetchCounterNames.get().isEmpty()) {

                int index = 0;

                for (String counterName : fetchCounterNames.get()) {
                    query.bind("counterName_" + (index++), counterName);
                }
            }

            List<List<RolledUpCounter>> resultList = query.map(
                    new QueriedRolledUpCounterMapper(
                            excludeDistribution,
                            distributionLimit,
                            unqiueIds)).list();

            List<RolledUpCounter> result;

            if (resultList == null || resultList.isEmpty()) {
                result = ImmutableList.of();
            }
            else {
                result = resultList.get(0);
            }

            return result;
        }});
    }

    @Override
    public int cleanExpiredDailyRolledUpCounters(final DateTime toDateTime)
    {
        int deleted = dbi.withHandle(new HandleCallback<Integer>() {

            @Override
            public Integer withHandle(Handle handle) throws Exception
            {
                String queryStr = "delete from metrics_daily "
                        + "where `datestamp` <= :toDateTime";

                Update query =  handle.createStatement(queryStr).bind(
                        "toDateTime",
                        RolledUpCounter.DATE_FORMATTER.print(toDateTime));

                return query.execute();
            }});

        return deleted;
    }

    public static class CounterEventDataMapper implements ResultSetMapper<CounterEventData>
    {
        private final ObjectMapper mapper;

        public CounterEventDataMapper(final ObjectMapper mapper){
            this.mapper = mapper;
        }

        @Override
        public CounterEventData map(int index, ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                return mapper.readValue(r.getString("metrics"), CounterEventData.class);
            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Error handling not implemented!", e);
            }
        }

    }


    public static class SingleCompleteRolledUpCounterMapper
            implements ResultSetMapper<RolledUpCounter> {

        /**
         * This is a one to many mapping, so the given result set will be read
         * completely in the first call
         * @param index
         * @param r result set of the query
         * @param ctx
         * @return
         * @throws SQLException
         */
        @Override
        public RolledUpCounter map(int index,
                ResultSet r, StatementContext ctx) throws SQLException
        {
            try {
                // This assumes that all the results in the set will be from the
                // same rolled counter
                String namespace = r.getString("namespace");
                DateTime date = new DateTime(r.getDate("datestamp"));

                Map<String, RolledUpCounterData> counterSummary
                        = Maps.newHashMap();

                do {
                    RolledUpCounterData single = mapSingle(r);
                    counterSummary.put(single.getCounterName(), single);

                } while(r.next());

                return new RolledUpCounter(
                        namespace, date, date, counterSummary);
            }
            catch (IOException e) {
                throw new SQLException("Error handling not implemented!", e);
            }

        }

        /**
         * Map a single row in the result set to a
         * @param r
         * @return
         */
        private RolledUpCounterData mapSingle(ResultSet r)
                throws SQLException, IOException {
            String counterName = r.getString("counter_name");
            int totalCount = r.getInt("total_count");
            CounterDistribution distriution =
                    deserializeDistribution(r.getBytes("distribution"),
                            Optional.<Set<String>>absent(),
                            Optional.<Integer>absent());

            RolledUpCounterData result = new RolledUpCounterData(
                    counterName, totalCount, distriution);

            return result;
        }


    }

    /**
     * Jdbi mapper for turning a result set of rolled up counter datas into a
     * rolled up counter
     */
    public static class QueriedRolledUpCounterMapper
            implements ResultSetMapper<List<RolledUpCounter>> {

        private final boolean excludeDistribution;
        private final Optional<Integer> distributionLimit;
        private final Optional<Set<String>> uniqueIds;

        public QueriedRolledUpCounterMapper(
                boolean excludeDistribution,
                Optional<Integer> distributionLimit,
                Optional<Set<String>> uniqueIds) {
            this.excludeDistribution = excludeDistribution;
            this.distributionLimit
                    = distributionLimit == null
                    ? Optional.<Integer>absent()
                    : distributionLimit;
            this.uniqueIds
                    = uniqueIds == null
                    ? Optional.<Set<String>>absent()
                    : uniqueIds;
        }

        /**
         * This mapper handles the one-to-many relationship between what is
         * stored in the database as metrics_daily rows and the rolled-up
         * counter.  The resultsets this mapper is meant to handle do not have
         * a single common date, so the return is not a single rolled up
         * counter, but multiple sorted out into a map by datestamp
         * @param index
         * @param r resultset ... the only thing we actually use here :P
         * @param ctx
         * @return
         * @throws SQLException
         */
        @Override
        public List<RolledUpCounter> map(int index, ResultSet r,
                StatementContext ctx) throws SQLException {
            try {

                // This assumes all rolled up counters will have the same
                // namespaces
                String namespace = r.getString("namespace");

                Map<DateTime, Map<String, RolledUpCounterData>>
                        counterSummariesByDate = Maps.newHashMap();

                Map<String, RolledUpCounterData> currSummary = null;
                DateTime lastDate = null;

                // Iterate through the result and collect all the rows by their
                // datestamp.
                do {
                    DateTime date = new DateTime(r.getDate("datestamp"));

                    if (currSummary == null || !Objects.equal(date, lastDate)) {
                        currSummary = counterSummariesByDate.get(date);

                        if (currSummary == null) {
                            currSummary = Maps.newHashMap();
                            counterSummariesByDate.put(date, currSummary);
                        }
                    }

                    lastDate = date;

                    RolledUpCounterData single = mapSingle(r);
                    currSummary.put(single.getCounterName(), single);

                } while(r.next());

                Map<DateTime, RolledUpCounter> resultMap = Maps.newTreeMap();

                for (Map.Entry<DateTime, Map<String, RolledUpCounterData>> e
                        : counterSummariesByDate.entrySet()) {

                    resultMap.put(e.getKey(), new RolledUpCounter(
                            namespace, e.getKey(), e.getKey(), e.getValue()));
                }

                return ImmutableList.copyOf(resultMap.values());
            }
            catch (IOException ie) {
                throw new SQLException("IO Exception in result set mapping "
                        + "for queried, rolled-up counters", ie);
            }
        }

        /**
         * Map a single row in the result set to and take into account the
         * member query parameters like distribution limit and exclusion
         * @param r
         * @return
         */
        private RolledUpCounterData mapSingle(ResultSet r)
                throws SQLException, IOException {
            String counterName = r.getString("counter_name");
            int totalCount = r.getInt("total_count");
            CounterDistribution distribution =
                    excludeDistribution
                    ? null
                    : deserializeDistribution(
                            r.getBytes("distribution"),
                            uniqueIds,
                            distributionLimit);

            RolledUpCounterData result = new RolledUpCounterData(
                    counterName, totalCount, distribution);

            return result;
        }
    }
}
