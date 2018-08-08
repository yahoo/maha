// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

import java.io.Serializable;

/**
 * Created by hiral on 10/21/14.
 */
public interface EventBatcher<T> extends Serializable {
    EventBatch.EventBatchBuilder<T> newBuilder(int initSize);
}
