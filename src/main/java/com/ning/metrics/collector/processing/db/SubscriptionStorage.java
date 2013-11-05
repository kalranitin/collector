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
package com.ning.metrics.collector.processing.db;

import com.ning.metrics.collector.processing.db.model.Subscription;

import java.util.Set;

public interface SubscriptionStorage
{
    Long insert(Subscription subscription);

    Set<Subscription> load(final String topic);
    
    public Set<Subscription> loadByStartsWith(final String topic);

    Subscription loadSubscriptionById(final Long id);

    boolean deleteSubscriptionById(final Long id);
    
    public void cleanUp();
}
