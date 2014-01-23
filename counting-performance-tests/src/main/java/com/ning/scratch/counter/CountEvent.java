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

import java.util.List;

/**
 * Stores the information about the a single incrementation of a given counter
 *
 * @author kguthrie
 */
public class CountEvent {

    public static final String MEMBER_VISIT = "memberVisit";
    public static final String NON_MEMBER_VISIT = "nonMemberVisit";

    public static final String MEMBER_PHONE_VISIT = "memberPhoneVisit";
    public static final String MEMBER_TABLET_VISIT = "memberTabletVisit";
    public static final String MEMBER_DESKTOP_VISIT = "memberDesktopVisit";
    public static final String NON_MEMBER_PHONE_VISIT = "nonMemberPhoneVisit";
    public static final String NON_MEMBER_TABLET_VISIT = "nonMemberTabletVisit";
    public static final String NON_MEMBER_DESKTOP_VISIT
            = "nonMemberDesktopVisit";
    public static final String NON_MEMBER_ROBOT_VISIT = "nonMemberRobotVisit";

    public static final String MEMBER_CONTRIBUTE = "memberContribute";

    private final CounterCallback callback;
    private final String counterGroup;
    private final List<String> counterNames;
    private final int incrementBy;
    private final String timeZone;

    public CountEvent(CounterCallback callback, String counterGroup,
            List<String> counterNames, int incrementBy, String timeZone) {
        this.callback = callback;
        this.counterGroup = counterGroup;
        this.counterNames = counterNames;
        this.incrementBy = incrementBy;
        this.timeZone = timeZone;
    }

    /**
     * @return the callback
     */
    public CounterCallback getCallback() {
        return callback;
    }

    /**
     * @return the counterGroup
     */
    public String getCounterGroup() {
        return counterGroup;
    }

    /**
     * @return the counterNames
     */
    public List<String> getCounterNames() {
        return counterNames;
    }

    /**
     * @return the incrementBy
     */
    public int getIncrementBy() {
        return incrementBy;
    }

    /**
     * @return the timeZone
     */
    public String getTimeZone() {
        return timeZone;
    }

}
