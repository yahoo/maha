// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

/**
 * Created by hiral on 10/21/14.
 */

public class StringEventBatcher implements EventBatcher<String> {
    @Override
    public EventBatch.EventBatchBuilder<String> newBuilder(int initSize) {
        return new StringEventBatch.Builder(initSize);
    }
}
