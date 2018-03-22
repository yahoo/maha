// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.yahoo.maha.parrequest2.EitherUtils;
import scala.util.Either;
import com.yahoo.maha.parrequest2.GeneralError;
import scala.Option;
import scala.util.Right;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Tuple2;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class takes two futures and on completion of both, sets a Tuple2<Option<T,Option<U>> as a result of this future.
 * If allMustSucceed is true, the result will not be an error if both futures provide a result. If allMustSucceed is
 * false, the result will optionally provide both results depending on if they completed successfully
 */
class CombinedFuture2Option<T, U> extends AbstractFuture<Either<GeneralError, Tuple2<Option<T>, Option<U>>>> {

    private final ListenableFuture<Either<GeneralError, T>> firstFuture;
    private final ListenableFuture<Either<GeneralError, U>> secondFuture;
    private final AtomicInteger remaining = new AtomicInteger(2);
    private final AtomicBoolean shortCircuit = new AtomicBoolean(false);
    private volatile Option<T> firstValue = Option.empty();
    private volatile Option<U> secondValue = Option.empty();
    private final boolean allMustSucceed;

    public CombinedFuture2Option(ParallelServiceExecutor executor,
                                 final ListenableFuture<Either<GeneralError, T>> firstFuture,
                                 final ListenableFuture<Either<GeneralError, U>> secondFuture,
                                 boolean allMustSucceed) {
        checkNotNull(executor, "executor is null");
        checkNotNull(firstFuture, "First future is null");
        checkNotNull(secondFuture, "Second future is null");
        this.firstFuture = firstFuture;
        this.secondFuture = secondFuture;
        this.allMustSucceed = allMustSucceed;

        // First, schedule cleanup to execute when the Future is done.
        executor.addListener(this, new Runnable() {
            @Override
            public void run() {
                // Cancel all the component futures.
                if (isCancelled()) {
                    if (firstFuture != null && !firstFuture.isDone()) {
                        firstFuture.cancel(wasInterrupted());
                    }
                    if (secondFuture != null && !secondFuture.isDone()) {
                        secondFuture.cancel(wasInterrupted());
                    }
                }
            }
        });

        executor.addListener(firstFuture, new Runnable() {
            @Override
            public void run() {
                setFirstValue();
            }
        });
        executor.addListener(secondFuture, new Runnable() {
            @Override
            public void run() {
                setSecondValue();
            }
        });
    }

    private void setFirstValue() {
        try {
            checkState(firstFuture.isDone(),
                       "Tried to set value from future which is not done");
            Either<GeneralError, T> returnValue = Uninterruptibles.getUninterruptibly(firstFuture);
            if (returnValue.isLeft()) {
                if (allMustSucceed && shortCircuit.compareAndSet(false, true)) {
                    set(EitherUtils.<GeneralError, T, Tuple2<Option<T>, Option<U>>>castLeft(returnValue));
                }
            } else {
                firstValue = Option.apply(returnValue.right().get());
            }
        } catch (CancellationException e) {
            if (allMustSucceed) {
                cancel(false);
            }
        } catch (Throwable t) {
            if (allMustSucceed) {
                super.setException(t);
            }
        } finally {
            checkAndSet();
        }
    }

    private void setSecondValue() {
        try {
            checkState(secondFuture.isDone(),
                       "Tried to set value from future which is not done");
            Either<GeneralError, U> returnValue = Uninterruptibles.getUninterruptibly(secondFuture);
            if (returnValue.isLeft()) {
                if (allMustSucceed && shortCircuit.compareAndSet(false, true)) {
                    set(EitherUtils.<GeneralError, U, Tuple2<Option<T>, Option<U>>>castLeft(returnValue));
                }
            } else {
                secondValue = Option.apply(returnValue.right().get());
            }
        } catch (CancellationException e) {
            if (allMustSucceed) {
                cancel(false);
            }
        } catch (Throwable t) {
            if (allMustSucceed) {
                super.setException(t);
            }
        } finally {
            checkAndSet();
        }
    }

    private void checkAndSet() {
        int newRemaining = remaining.decrementAndGet();
        checkState(newRemaining >= 0, "Less than 0 remaining futures");
        if (newRemaining == 0) {
            if (!shortCircuit.get() && !isDone()) {
                checkNotNull(firstValue, "First value is null on completion");
                checkNotNull(secondValue, "Second value is null on completion");
                set(new Right<GeneralError, Tuple2<Option<T>, Option<U>>>(new Tuple2<>(firstValue, secondValue)));
            }
        }
    }

    public static <T, U> CombinedFuture2Option<T, U> from(ParallelServiceExecutor executor,
                                                          ListenableFuture<Either<GeneralError, T>> firstFuture,
                                                          ListenableFuture<Either<GeneralError, U>> secondFuture,
                                                          boolean allMustSucceed
    ) {
        return new CombinedFuture2Option<>(executor, firstFuture, secondFuture, allMustSucceed);
    }
}
