// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

import java.util.NoSuchElementException;

/**
 * Created by hiral on 5/21/15.
 */
public class None<T> extends Option<T> {

    protected None() {
    }

    @Override
    public boolean isDefined() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public T get() {
        throw new NoSuchElementException("Cannot call get() on None");
    }

    @Override
    public String toString() {
        return "None";
    }
}
