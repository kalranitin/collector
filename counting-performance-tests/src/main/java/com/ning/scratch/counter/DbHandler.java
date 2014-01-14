/*
 * Copyright 2010-2011 Ning, Inc.
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
package com.ning.scratch.counter;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author kguthrie
 */
public class DbHandler {

    public static final String TABLE_NAME_BASE = "counters_";
    public static AtomicInteger insertCount = new AtomicInteger(0);
    public static AtomicInteger updateCount = new AtomicInteger(0);

    private final CounterPartitioner partitioner;
    private final Connection con;
    private final PreparedStatement upsert;
    private final PreparedStatement updateCounterDates;
    private final PreparedStatement dropPartition;
    private final PreparedStatement createPartition;
    private final PreparedStatement selectCount;
    private final PreparedStatement selectCountsNotEqualTo;
    private final PreparedStatement selectSingleCount;
    private final PreparedStatement setMaxHeapTableSize;

    private PreparedStatement createEventCountersTable;

    private final Date today;

    public DbHandler(String url) throws Exception {
        con = DriverManager.getConnection(url);

        partitioner = CounterPartitioner.get();

        setMaxHeapTableSize = con.prepareStatement(
                "set @@max_heap_table_size = 1073741824;");

        dropPartition = con.prepareStatement("drop table if exists "
                + TABLE_NAME_BASE + "?");
        createPartition = con.prepareStatement(
                "CREATE TABLE " + TABLE_NAME_BASE + "? (\n"
                + "  `id` int(11) unsigned NOT NULL,\n"
                + "  `count` int(11) DEFAULT NULL,\n"
                + "  `count_start` date NOT NULL,\n"
                + "  PRIMARY KEY (`id`,`count_start`)\n"
                // + ",  KEY `id` (`id`)\n"
                //                + ",  KEY `count` (`count`)\n"
                //                + ",  KEY `count_start` (`count_start`)\n"
                + ") ENGINE=memory DEFAULT CHARSET=utf8;");
        upsert = con.prepareStatement(
                "insert into " + TABLE_NAME_BASE
                + "? (id, `count`, count_start) "
                + "values (?, ?, ?) "
                + "on duplicate key update count = count + ?");

        selectCount = con.prepareStatement(
                "select count(*) from " + TABLE_NAME_BASE + "? "
                + "where count_start = ?");

        selectCountsNotEqualTo = con.prepareStatement(
                "select count(*) from " + TABLE_NAME_BASE + "? "
                + "where count_start = ? and count != ?");

        updateCounterDates = con.prepareStatement("update "
                + TABLE_NAME_BASE + "? "
                + "set count_start = ? + interval ? day "
                + "where count_start = ?");

        selectSingleCount = con.prepareStatement("select `count` from "
                + TABLE_NAME_BASE + "? where id = ? and count_start = ?");

        try (Statement st = con.createStatement();
                ResultSet rs = st.executeQuery(
                        "select curdate() as count_start")) {
            rs.next();
            today = rs.getDate(1);
        }

        setUpEventCountersTable();
    }

    /**
     * set up the event counters table
     */
    private void setUpEventCountersTable() throws Exception {
        StringBuilder createStatement = new StringBuilder();

        createStatement.append("CREATE TABLE `event_counters` (\n"
                + "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\n"
                + "  `event_group` varchar(20) NOT NULL DEFAULT '',\n"
                + "  `event_name` varchar(20) NOT NULL DEFAULT '',\n"
                + "  `event_group_time_zone` smallint(2) NOT NULL,\n"
        );

        for (int i = 0; i < Counter.DAYS_OF_HISTORY; i++) {
            createStatement.append("  `daily_count_");
            createStatement.append(i);
            createStatement.append("` int(11) NOT NULL DEFAULT '0',\n");
        }

        createStatement.append("  PRIMARY KEY (`id`),\n"
                + "  KEY `subscription_id` (`event_group`,`event_name`),\n"
                + "  KEY `event_group_time_zone` (`event_group_time_zone`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

        createEventCountersTable = con.prepareStatement(
                createStatement.toString());
    }

    public Connection getConnection() {
        return con;
    }

    public Date getToday() {
        return today;
    }

    /**
     * Drops and recreates the counter table partition with the given name (if
     * it exists) and recreates it
     *
     * @throws Exception
     */
    public void dropAndRecreatePartitions() throws Exception {

        setMaxHeapTableSize.executeUpdate();

        for (int i = 0; i < CounterPartitioner.PARTITION_COUNT; i++) {
            dropPartition.setInt(1, i);
            createPartition.setInt(1, i);
            dropPartition.executeUpdate();
            createPartition.executeUpdate();
        }
    }

    public int increment(String counterGroupId, String counterId,
            String timezone, int incrmentBy) {
        int result = -1;

        return result;
    }


    public int getNumberOfCountersForDate(Date date, int rem, int mod)
            throws Exception {

        int result = 0;
        selectCount.setDate(2, date);

        for (int i = rem; i < CounterPartitioner.PARTITION_COUNT; i += mod) {
            selectCount.setInt(1, i);
            try (ResultSet rs = selectCount.executeQuery()) {
                rs.next();
                result += rs.getInt(1);
            }
            catch (Exception ex) {
                throw ex;
            }
        }

        return result;
    }

    public int getNumberOfCountersForDateNotEqualTo(Date date, int count,
            int rem, int mod) throws Exception {
        int result = 0;

        selectCountsNotEqualTo.setDate(2, date);
        selectCountsNotEqualTo.setInt(3, count);

        for (int i = rem; i < CounterPartitioner.PARTITION_COUNT; i += mod) {
            selectCountsNotEqualTo.setInt(1, i);
            try (ResultSet rs = selectCountsNotEqualTo.executeQuery()) {
                rs.next();
                result += rs.getInt(1);
            }
            catch (Exception ex) {
                throw ex;
            }
        }

        return result;
    }

    public void incrementCounterDatesBy(int days, int rem, int mod)
            throws Exception {
        updateCounterDates.setDate(2, today);
        updateCounterDates.setInt(3, days);
        updateCounterDates.setDate(4, today);

        for (int i = rem; i < CounterPartitioner.PARTITION_COUNT; i += mod) {
            updateCounterDates.setInt(1, i);
            updateCounterDates.executeUpdate();
        }

    }

    public int getCountForCounter(int id, Date date) throws Exception {
        selectSingleCount.setInt(1, partitioner.getPartition(id));
        selectSingleCount.setInt(2, id);
        selectSingleCount.setDate(3, date);

        try (ResultSet rs = selectSingleCount.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }

        throw new Exception("Counter " + id + " for date " + date
                + " not found");
    }
}
