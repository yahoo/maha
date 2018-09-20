// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.yahoo.maha.parrequest2.EitherUtils;
import com.yahoo.maha.parrequest2.GeneralError;
import scala.Tuple2;
import scala.util.Either;
import scala.util.Right;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class takes two futures and on completion of both, sets a Tuple2<T,U> as a result of this future.
 */
class CombinedFuture2<T, U> extends AbstractFuture<Either<GeneralError, Tuple2<T, U>>> {

    private final ListenableFuture<Either<GeneralError, T>> firstFuture;
    private final ListenableFuture<Either<GeneralError, U>> secondFuture;
    private final AtomicInteger remaining = new AtomicInteger(2);
    private final AtomicBoolean shortCircuit = new AtomicBoolean(false);
    private volatile T firstValue = null;
    private volatile U secondValue = null;

    public CombinedFuture2(ParallelServiceExecutor executor,
                           final ListenableFuture<Either<GeneralError, T>> firstFuture,
                           final ListenableFuture<Either<GeneralError, U>> secondFuture) {
        checkNotNull(executor, "executor is null");
        checkNotNull(firstFuture, "First future is null");
        checkNotNull(secondFuture, "Second future is null");
        this.firstFuture = firstFuture;
        this.secondFuture = secondFuture;

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
                if (shortCircuit.compareAndSet(false, true)) {
                    set(EitherUtils.<GeneralError, T, Tuple2<T, U>>castLeft(returnValue));
                }
            } else {
                firstValue = returnValue.right().get();
            }
        } catch (CancellationException e) {
            cancel(false);
        } catch (Throwable t) {
            super.setException(t);
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
                if (shortCircuit.compareAndSet(false, true)) {
                    set(EitherUtils.<GeneralError, U, Tuple2<T, U>>castLeft(returnValue));
                }
            } else {
                secondValue = returnValue.right().get();
            }
        } catch (CancellationException e) {
            cancel(false);
        } catch (Throwable t) {
            super.setException(t);
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
                set(new Right<GeneralError, Tuple2<T, U>>(new Tuple2<>(firstValue, secondValue)));
            }
        }
    }

    public static <T, U> CombinedFuture2<T, U> from(ParallelServiceExecutor executor,
                                                    ListenableFuture<Either<GeneralError, T>> firstFuture,
                                                    ListenableFuture<Either<GeneralError, U>> secondFuture
    ) {
        return new CombinedFuture2<>(executor, firstFuture, secondFuture);
    }
}
