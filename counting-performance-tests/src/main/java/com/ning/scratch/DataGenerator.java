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
package com.ning.scratch;

import com.ning.scratch.counter.CountEvent;
import com.ning.scratch.counter.CounterCallback;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Generates counter data as fast as possible that is shaped to match the
 * distribution of users in ning networks
 *
 * @author kguthrie
 */
public class DataGenerator implements Runnable {

    private final BlockingQueue<CountEvent> outputQueue;
    private final Random rand;
    private final CounterCallback callback;
    private final long numberOfIncrements;

    private int[] networkSize;
    private String[] networkName;
    private String[] networkTimeZone;
    private int networkCount;
    private int userCount;

    public DataGenerator(long numberOfIncrements,
            BlockingQueue<CountEvent> outputQueue,
            CounterCallback callback) throws Exception {
        this.outputQueue = outputQueue;
        this.callback = callback;
        this.numberOfIncrements = numberOfIncrements;

        rand = new Random(System.currentTimeMillis());

        loadNetworkInfo();

    }

    private void loadNetworkInfo() throws IOException {

        String[] timeZoneIds = TimeZone.getAvailableIDs();


        try (
                FileReader fr = new FileReader("networks.csv");
                BufferedReader br = new BufferedReader(fr);) {

            String sLine;
            String[] sParts;

            networkCount = Integer.parseInt(br.readLine().replace(",", ""));

            networkSize = new int[networkCount];
            networkName = new String[networkCount];
            networkTimeZone = new String[networkCount];

            userCount = 0;
            int i = 0;

            while ((sLine = br.readLine()) != null) {
                sParts = sLine.split("[,\t ]+");
                String name = sParts[0];
                int size = Integer.parseInt(sParts[1]);
                userCount += size;
                networkSize[i] = size;
                networkName[i] = name;
                networkTimeZone[i] = timeZoneIds[rand.nextInt(
                        timeZoneIds.length)];

                i++;
            }

        }
    }

    @Override
    public void run() {

        long increments = 0;

        long start = System.currentTimeMillis();
        long stop;

        long reportInterval = numberOfIncrements / 500;

        while (increments < numberOfIncrements) {
            int networkNum = getNextNetwork();
            String network = networkName[networkNum];
            String[] counters = getNextCounters();
            String timeZone = networkTimeZone[networkNum];

            try {
                for (String counter : counters) {

                    if (counter == null) {
                        continue;
                    }

                    CountEvent next = new CountEvent(
                            callback, network, counter, 1, timeZone);

                    while (!outputQueue.offer(next, 1000, TimeUnit.SECONDS)) {

                    }

//                    if (++increments % reportInterval == 0) {
//                        stop = System.currentTimeMillis();
//                        double rate = 1000.0 * reportInterval / (stop - start);
//                        System.out.println(String.format(
//                                "Generator: %.4f inc/s", rate));
//                        System.out.flush();
//                        start = System.currentTimeMillis();
//                    }
                }
            }
            catch (InterruptedException ie) {
                break;
            }
        }

        System.out.println("Generator: done");
    }

    /**
     * Gets the next network based on a roulette of all the existing users
     *
     * @return
     */
    private int getNextNetwork() {
        int nextUser = rand.nextInt(userCount);

        int result = 0;
        int sum = 0;

        while ((sum += networkSize[result]) < nextUser) {
            result++;
        }

        return result;
    }

    /**
     * Get the set of counters to be incremented by the current event
     *
     * @return
     */
    private String[] getNextCounters() {

        String[] result = new String[3];
        result[2] = null;

        boolean isMember = rand.nextBoolean();

        if (isMember) {
            result[0] = CountEvent.MEMBER_VISIT;
            switch (rand.nextInt(4)) {
                case 0:
                case 1: {
                    result[1] = CountEvent.MEMBER_DESKTOP_VISIT;
                    break;
                }
                case 2: {
                    result[1] = CountEvent.MEMBER_PHONE_VISIT;
                    break;
                }
                case 3: {
                    result[1] = CountEvent.MEMBER_TABLET_VISIT;
                    break;
                }
            }

            if (rand.nextBoolean()) {
                result[2] = CountEvent.MEMBER_CONTRIBUTE;
            }
        }
        else {
            result[0] = CountEvent.NON_MEMBER_VISIT;
            switch (rand.nextInt(5)) {
                case 0:
                case 1: {
                    result[1] = CountEvent.NON_MEMBER_DESKTOP_VISIT;
                    break;
                }
                case 2: {
                    result[1] = CountEvent.NON_MEMBER_PHONE_VISIT;
                    break;
                }
                case 3: {
                    result[1] = CountEvent.NON_MEMBER_TABLET_VISIT;
                    break;
                }
                case 4: {
                    result[1] = CountEvent.NON_MEMBER_ROBOT_VISIT;
                    break;
                }
            }

        }

        return result;
    }

}
