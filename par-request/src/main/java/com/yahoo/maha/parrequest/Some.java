// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

import com.google.common.base.Preconditions;

/**
 * Created by hiral on 5/21/15.
 */
public class Some<T> extends Option<T> {

    private final T t;

    protected Some(T t) {
        Preconditions.checkState(t != null, "Cannot set null value with Some");
        this.t = t;
    }

    @Override
    public boolean isDefined() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public T get() {
        return t;
    }
    
    @Override
    public String toString() {
        return String.format("Some(%s)", t);
    }
}
