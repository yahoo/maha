// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.yahoo.maha.parrequest2.EitherUtils;
import scala.util.Either;
import scala.util.Right;
import com.yahoo.maha.parrequest2.GeneralError;
import scala.Tuple4;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class takes four futures and on completion of both, sets a Tuple4<T,U,V, W> as a result of this future.
 */
class CombinedFuture4<T, U, V, W> extends AbstractFuture<Either<GeneralError, Tuple4<T, U, V, W>>> {

    private final ListenableFuture<Either<GeneralError, T>> firstFuture;
    private final ListenableFuture<Either<GeneralError, U>> secondFuture;
    private final ListenableFuture<Either<GeneralError, V>> thirdFuture;
    private final ListenableFuture<Either<GeneralError, W>> fourthFuture;
    private final AtomicInteger remaining = new AtomicInteger(4);
    private final AtomicBoolean shortCircuit = new AtomicBoolean(false);
    private volatile T firstValue = null;
    private volatile U secondValue = null;
    private volatile V thirdValue = null;
    private volatile W fourthValue = null;

    public CombinedFuture4(ParallelServiceExecutor executor,
                           final ListenableFuture<Either<GeneralError, T>> firstFuture,
                           final ListenableFuture<Either<GeneralError, U>> secondFuture,
                           final ListenableFuture<Either<GeneralError, V>> thirdFuture,
                           final ListenableFuture<Either<GeneralError, W>> fourthFuture
    ) {
        checkNotNull(executor, "executor is null");
        checkNotNull(firstFuture, "First future is null");
        checkNotNull(secondFuture, "Second future is null");
        checkNotNull(thirdFuture, "Third future is null");
        checkNotNull(fourthFuture, "Fourth future is null");
        this.firstFuture = firstFuture;
        this.secondFuture = secondFuture;
        this.thirdFuture = thirdFuture;
        this.fourthFuture = fourthFuture;

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
                    if (fourthFuture != null && !fourthFuture.isDone()) {
                        fourthFuture.cancel(wasInterrupted());
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
        executor.addListener(fourthFuture, new Runnable() {
            @Override
            public void run() {
                setFourthValue();
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
                    set(EitherUtils.<GeneralError, T, Tuple4<T, U, V, W>>castLeft(returnValue));
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
                    set(EitherUtils.<GeneralError, U, Tuple4<T, U, V, W>>castLeft(returnValue));
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

    private void setThirdValue() {
        try {
            checkState(thirdFuture.isDone(),
                    "Tried to set value from future which is not done");
            Either<GeneralError, V> returnValue = Uninterruptibles.getUninterruptibly(thirdFuture);
            if (returnValue.isLeft()) {
                if (shortCircuit.compareAndSet(false, true)) {
                    set(EitherUtils.<GeneralError, V, Tuple4<T, U, V, W>>castLeft(returnValue));
                }
            } else {
                thirdValue = returnValue.right().get();
            }
        } catch (CancellationException e) {
            cancel(false);
        } catch (Throwable t) {
            super.setException(t);
        } finally {
            checkAndSet();
        }
    }

    private void setFourthValue() {
        try {
            checkState(fourthFuture.isDone(),
                    "Tried to set value from future which is not done");
            Either<GeneralError, W> returnValue = Uninterruptibles.getUninterruptibly(fourthFuture);
            if (returnValue.isLeft()) {
                if (shortCircuit.compareAndSet(false, true)) {
                    set(EitherUtils.<GeneralError, W, Tuple4<T, U, V, W>>castLeft(returnValue));
                }
            } else {
                fourthValue = returnValue.right().get();
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
                checkNotNull(thirdValue, "Third value is null on completion");
                checkNotNull(fourthFuture, "Fourth value is null on completion");
                set(new Right<GeneralError, Tuple4<T, U, V, W>>(new Tuple4<>(firstValue, secondValue, thirdValue, fourthValue)));
            }
        }
    }

    public static <T, U, V, W> CombinedFuture4<T, U, V, W> from(ParallelServiceExecutor executor,
                                                                ListenableFuture<Either<GeneralError, T>> firstFuture,
                                                                ListenableFuture<Either<GeneralError, U>> secondFuture,
                                                                ListenableFuture<Either<GeneralError, V>> thirdFuture,
                                                                ListenableFuture<Either<GeneralError, W>> fourthFuture
    ) {
        return new CombinedFuture4<>(executor, firstFuture, secondFuture, thirdFuture, fourthFuture);
    }
}
