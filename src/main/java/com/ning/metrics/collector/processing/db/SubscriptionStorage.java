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
