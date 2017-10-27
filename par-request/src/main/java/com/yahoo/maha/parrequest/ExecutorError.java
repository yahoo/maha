// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

import java.util.function.Function;

/**
 * Created by hiral on 3/26/14.
 */
abstract public class ExecutorError<T> implements Function<Throwable, T> {

    public final String stage;
    public final String message;

    public ExecutorError(String stage, String message) {
        this.stage = stage;
        this.message = message;
    }

    @Override
    public boolean equals(Object object) {
        //don't care, DON'T USE IT
        return false;
    }
}
