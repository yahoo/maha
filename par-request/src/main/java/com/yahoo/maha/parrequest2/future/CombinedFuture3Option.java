// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest.future;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.yahoo.maha.parrequest.Either;
import com.yahoo.maha.parrequest.GeneralError;
import com.yahoo.maha.parrequest.Option;
import com.yahoo.maha.parrequest.Right;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Tuple3;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class takes three futures and on completion of both, sets a Tuple3<T,U,V> as a result of this future. If
 * allMustSucceed is true, the result will not be an error if all futures provide a result. If allMustSucceed is false,
 * the result will optionally provide all results depending on if they completed successfully
 */
class CombinedFuture3Option<T, U, V>
    extends AbstractFuture<Either<GeneralError, Tuple3<Option<T>, Option<U>, Option<V>>>> {

    private final ListenableFuture<Either<GeneralError, T>> firstFuture;
    private final ListenableFuture<Either<GeneralError, U>> secondFuture;
    private final ListenableFuture<Either<GeneralError, V>> thirdFuture;
    private final AtomicInteger remaining = new AtomicInteger(3);
    private final AtomicBoolean shortCircuit = new AtomicBoolean(false);
    private volatile Option<T> firstValue = Option.none();
    private volatile Option<U> secondValue = Option.none();
    private volatile Option<V> thirdValue = Option.none();
    private boolean allMustSucceed;

    public CombinedFuture3Option(ParallelServiceExecutor executor,
                                 final ListenableFuture<Either<GeneralError, T>> firstFuture,
                                 final ListenableFuture<Either<GeneralError, U>> secondFuture,
                                 final ListenableFuture<Either<GeneralError, V>> thirdFuture,
                                 boolean allMustSucceed) {
        checkNotNull(executor, "executor is null");
        checkNotNull(firstFuture, "First future is null");
        checkNotNull(secondFuture, "Second future is null");
        checkNotNull(thirdFuture, "Third future is null");
        this.firstFuture = firstFuture;
        this.secondFuture = secondFuture;
        this.thirdFuture = thirdFuture;
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
                    if (thirdFuture != null && !thirdFuture.isDone()) {
                        thirdFuture.cancel(wasInterrupted());
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
        executor.addListener(thirdFuture, new Runnable() {
            @Override
            public void run() {
                setThirdValue();
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
                    set(Either.<GeneralError, T, Tuple3<Option<T>, Option<U>, Option<V>>>castLeft(returnValue));
                }
            } else {
                firstValue = Option.apply(Either.extractRight(returnValue));
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
                    set(Either.<GeneralError, U, Tuple3<Option<T>, Option<U>, Option<V>>>castLeft(returnValue));
                }
            } else {
                secondValue = Option.apply(Either.extractRight(returnValue));
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

    private void setThirdValue() {
        try {
            checkState(thirdFuture.isDone(),
                       "Tried to set value from future which is not done");
            Either<GeneralError, V> returnValue = Uninterruptibles.getUninterruptibly(thirdFuture);
            if (returnValue.isLeft()) {
                if (allMustSucceed && shortCircuit.compareAndSet(false, true)) {
                    set(Either.<GeneralError, V, Tuple3<Option<T>, Option<U>, Option<V>>>castLeft(returnValue));
                }
            } else {
                thirdValue = Option.apply(Either.extractRight(returnValue));
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
                checkNotNull(thirdValue, "Third value is null on completion");
                set(new Right<GeneralError, Tuple3<Option<T>, Option<U>, Option<V>>>(
                    new Tuple3<>(firstValue, secondValue, thirdValue)));
            }
        }
    }

    public static <T, U, V> CombinedFuture3Option<T, U, V> from(ParallelServiceExecutor executor,
                                                                ListenableFuture<Either<GeneralError, T>> firstFuture,
                                                                ListenableFuture<Either<GeneralError, U>> secondFuture,
                                                                ListenableFuture<Either<GeneralError, V>> thirdFuture,
                                                                boolean allMustSucceed
    ) {
        return new CombinedFuture3Option<>(executor, firstFuture, secondFuture, thirdFuture, allMustSucceed);
    }
}
