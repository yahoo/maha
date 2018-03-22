// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.util.concurrent.ListenableFuture;
import scala.util.Either;
import com.yahoo.maha.parrequest2.GeneralError;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class represents a no-op request, in essence a request which wraps an input future and only allows a get() on
 * the result.
 */
public class NoopRequest<T> {

    private final ParallelServiceExecutor executor;
    private final ListenableFuture<T> future;

    NoopRequest(ParallelServiceExecutor executor, ListenableFuture<T> future) {
        checkNotNull(future, "Future is null");
        this.executor = executor;

        this.future = future;
    }

    public Either<GeneralError, T> get() {
        return executor.getSafely("Noop", future);
    }

    ListenableFuture<T> asFuture() {
        return future;
    }
}
