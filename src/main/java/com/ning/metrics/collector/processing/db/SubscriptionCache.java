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

import com.google.common.base.Optional;

import java.util.Map;
import java.util.Set;

public interface SubscriptionCache
{
    public Map<String, Optional<Subscription>> loadTopicSubscriptions(final Set<String> topics);
    public void addTopicSubscriptions(final Map<String, Optional<Subscription>> subscriptions);
    public void addTopicSubscriptions(final String topic, final Optional<Subscription> subscriptions);
    public void addEmptyTopicSubscriptions(final Set<String> topics);
    public void removeTopicSubscriptions(final String topic);
    public Set<Subscription> loadFeedSubscriptions(final String feed);
    public void addFeedSubscriptions(final String feed, final Set<Subscription> subscriptions);
    public void removeFeedSubscriptions(final String feed);
    public void cleanUp();
}
