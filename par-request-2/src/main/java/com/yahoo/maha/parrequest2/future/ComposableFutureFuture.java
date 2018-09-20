// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.yahoo.maha.parrequest2.EitherUtils;
import com.yahoo.maha.parrequest2.GeneralError;
import scala.util.Either;

import java.util.concurrent.CancellationException;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class takes a future and a function, which produces a future, to apply to the future on completion and returns a
 * future which represents the result of future returned by the function.
 */
class ComposableFutureFuture<T, O> extends AbstractFuture<Either<GeneralError, O>> implements Runnable {

    class ResultListener implements Runnable {

        private final CombinableRequest<O> resultFuture;

        public ResultListener(CombinableRequest<O> resultFuture) {
            this.resultFuture = resultFuture;
        }

        @Override
        public void run() {
            try {
                checkState(this.resultFuture.asFuture().isDone(),
                           "Tried to set value from result future which is not done");
                Either<GeneralError, O> returnValue = Uninterruptibles.getUninterruptibly(this.resultFuture.asFuture());
                set(returnValue);
            } catch (CancellationException e) {
                cancel(false);
            } catch (Throwable t) {
                setException(t);
            }
        }

    }

    private final ListenableFuture<Either<GeneralError, T>> future;
    private final Function<T, CombinableRequest<O>> fn;
    private final ParallelServiceExecutor executor;

    public ComposableFutureFuture(ParallelServiceExecutor executor,
                                  final ListenableFuture<Either<GeneralError, T>> future,
                                  Function<T, CombinableRequest<O>> fn) {
        checkNotNull(executor, "executor cannot be null");
        checkNotNull(future, "future cannot be null");
        checkNotNull(fn, "function cannot be null");
        this.executor = executor;
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

    public void addResultListener(final CombinableRequest<O> resultFuture) {
        executor.addListener(this, new Runnable() {
            @Override
            public void run() {
                if (isCancelled()) {
                    if (resultFuture.asFuture() != null) {
                        resultFuture.asFuture().cancel(wasInterrupted());
                    }
                }
            }
        });
        executor.addListener(resultFuture.asFuture(), new ResultListener(resultFuture));
    }

    @Override
    public void run() {
        try {
            checkState(future.isDone(),
                       "Tried to set value from future which is not done");
            Either<GeneralError, T> returnValue = Uninterruptibles.getUninterruptibly(future);
            if (returnValue.isLeft()) {
                set(EitherUtils.<GeneralError, T, O>castLeft(returnValue));
            } else {
                T rightValue = returnValue.right().get();
                addResultListener(fn.apply(rightValue));
            }
        } catch (CancellationException e) {
            cancel(false);
        } catch (Throwable t) {
            setException(t);
        }
    }
}
