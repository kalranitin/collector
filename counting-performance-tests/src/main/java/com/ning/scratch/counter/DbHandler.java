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


    private final PreparedStatement setMaxHeapTableSize;
    //private final PreparedStatement createEventGroupsTable;
    private final PreparedStatement createEventCountersTable;
    //private final PreparedStatement createTimeZoneTable;

    private final Date today;

    public DbHandler(String url) throws Exception {
        con = DriverManager.getConnection(url);

        partitioner = CounterPartitioner.get();

        setMaxHeapTableSize = con.prepareStatement(
                "set @@max_heap_table_size = 1073741824;");

        createEventCountersTable = createEventCountersTableStatment();
        //createEventGroupsTable = con.prepareStatement();


        try (Statement st = con.createStatement();
                ResultSet rs = st.executeQuery(
                        "select curdate() as count_start")) {
            rs.next();
            today = rs.getDate(1);
        }

    }

    /**
     * set up the event counters table
     */
    private PreparedStatement createEventCountersTableStatment() throws Exception {
        StringBuilder createStatement = new StringBuilder();

        createStatement.append("CREATE TABLE `event_counter` (\n"
                + "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\n"
                + "  `event_group_id` int(11) unsigned NOT NULL,\n"
                + "  `event_name` varchar(20) NOT NULL,\n"
        );

        for (int i = 0; i < CounterServiceImpl.DAYS_OF_HISTORY; i++) {
            createStatement.append("  `daily_count_");
            createStatement.append(i);
            createStatement.append("` int(11) NOT NULL DEFAULT '0',\n");
        }

        createStatement.append("  PRIMARY KEY (`id`),\n"
                + "  KEY `subscription_id` (`event_group`,`event_name`),\n"
                + "  KEY `event_group_time_zone` (`event_group_time_zone`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

        return con.prepareStatement(createStatement.toString());
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


        return result;
    }

    public int getNumberOfCountersForDateNotEqualTo(Date date, int count,
            int rem, int mod) throws Exception {
        int result = 0;


        return result;
    }

    public void incrementCounterDatesBy(int days, int rem, int mod)
            throws Exception {


    }

    public int getCountForCounter(int id, Date date) throws Exception {
        return 0;
    }
}
