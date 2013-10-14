package com.ning.metrics.collector.processing.db;

import com.ning.metrics.collector.processing.db.model.Subscription;

import java.util.Set;

public interface SubscriptionStorage
{
    Long insert(Subscription subscription);

    Set<Subscription> load(String target);

    Subscription loadSubscriptionById(Long id);

    boolean deleteSubscriptionById(Long id);
    
    public void cleanUp();
}
