// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest.future;

import java.util.function.Function;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.yahoo.maha.parrequest.Either;
import com.yahoo.maha.parrequest.GeneralError;

import java.util.concurrent.CancellationException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class takes a future and a function to apply to the future on completion and returns a future which represents
 * the result of this operation.
 */
class ComposableFuture<T, O> extends AbstractFuture<Either<GeneralError, O>> implements Runnable {

    private final ListenableFuture<Either<GeneralError, T>> future;
    private final Function<T, Either<GeneralError, O>> fn;

    public ComposableFuture(ParallelServiceExecutor executor, final ListenableFuture<Either<GeneralError, T>> future,
                            Function<T, Either<GeneralError, O>> fn) {
        checkNotNull(executor, "executor cannot be null");
        checkNotNull(future, "future cannot be null");
        checkNotNull(fn, "function cannot be null");
        this.future = future;
        this.fn = fn;

        executor.addListener(this, new Runnable() {
            @Override
            public void run() {
                if (isCancelled()) {
                    if (future != null) {
                        future.cancel(wasInterrupted());
                    }
                }
            }
        });
        executor.addListener(future, this);
    }

    @Override
    public void run() {
        try {
            checkState(future.isDone(),
                       "Tried to set value from future which is not done");
            Either<GeneralError, T> returnValue = Uninterruptibles.getUninterruptibly(future);
            set(returnValue.flatMap(fn));
        } catch (CancellationException e) {
            cancel(false);
        } catch (Throwable t) {
            super.setException(t);
        }
    }
}
