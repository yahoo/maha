// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.data;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by hiral on 10/21/14.
 * NOTE: not thread safe!
 */
public class StringEventBatch implements EventBatch<String> {
    private final ImmutableList<String> events;

    private StringEventBatch(ImmutableList<String> events) {
        this.events = events;
    }

    public List<String> getEvents() {
        return events;
    }

    public static class Builder implements EventBatchBuilder<String> {
        private final ArrayList<String> builder;

        public Builder(int initSize) {
            builder = new ArrayList<>(initSize);
        }

        public EventBatchBuilder<String> add(String t) {
            builder.add(t);
            return this;
        }

        public EventBatchBuilder<String> addAll(Collection<String> collection) {
            builder.addAll(collection);
            return this;
        }

        public int size() {
            return builder.size();
        }

        public EventBatch<String> build() {
            return new StringEventBatch(ImmutableList.copyOf(builder));
        }
    }
}

