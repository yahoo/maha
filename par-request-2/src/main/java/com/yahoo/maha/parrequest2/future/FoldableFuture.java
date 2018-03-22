// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import java.util.function.Function;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.yahoo.maha.parrequest2.EitherUtils;
import scala.util.Either;
import com.yahoo.maha.parrequest2.GeneralError;

import java.util.concurrent.CancellationException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class takes a future and two functions, an error function and success function which are applied to result of
 * the input future, and returns a future which represents this operation.
 */
public class FoldableFuture<T, O> extends AbstractFuture<O> implements Runnable {

    private final ListenableFuture<Either<GeneralError, T>> future;
    private final Function<GeneralError, O> errFn;
    private final Function<T, O> fn;

    public FoldableFuture(ParallelServiceExecutor executor, final ListenableFuture<Either<GeneralError, T>> future,
                          Function<T, O> fn, Function<GeneralError, O> errFn) {
        checkNotNull(executor, "executor cannot be null");
        checkNotNull(future, "future cannot be null");
        checkNotNull(fn, "function cannot be null");
        checkNotNull(errFn, "error function cannot be null");
        this.future = future;
        this.fn = fn;
        this.errFn = errFn;

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
            set(EitherUtils.fold(errFn, fn, returnValue));
        } catch (CancellationException e) {
            cancel(false);
        } catch (Throwable t) {
            Either<GeneralError, T> either = GeneralError.either("run", "exception from upstream", t);
            set(EitherUtils.fold(errFn, fn, either));
        }
    }
}
