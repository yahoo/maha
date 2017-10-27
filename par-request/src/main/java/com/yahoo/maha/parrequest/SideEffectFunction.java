// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

import java.util.function.Function;

/**
 * Used to represent side effecting functions which do not return anything, e.g. logging
 */
abstract public class SideEffectFunction<T> implements Function<T, Nothing> {

    abstract void doSideEffect(T input);

    @Override
    final public Nothing apply(T input) {
        doSideEffect(input);
        return Nothing.get();
    }
}
