// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

import java.util.Collection;
import java.util.List;

/**
 * Created by hiral on 10/21/14.
 */
public interface EventBatch<T> {

    List<T> getEvents();

    interface EventBatchBuilder<T> {
        EventBatchBuilder<T> add(T t);
        EventBatchBuilder<T> addAll(Collection<T> collection);
        int size();
        EventBatch<T> build();
    }
}
