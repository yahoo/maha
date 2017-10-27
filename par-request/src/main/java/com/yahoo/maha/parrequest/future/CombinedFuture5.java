// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest.future;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.yahoo.maha.parrequest.Either;
import com.yahoo.maha.parrequest.Right;
import com.yahoo.maha.parrequest.GeneralError;
import scala.Tuple5;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class takes five futures and on completion of both, sets a Tuple5<T,U,V,W,X> as a result of this future.
 */
class CombinedFuture5<T, U, V, W, X> extends AbstractFuture<Either<GeneralError, Tuple5<T, U, V, W, X>>> {

    private final ListenableFuture<Either<GeneralError, T>> firstFuture;
    private final ListenableFuture<Either<GeneralError, U>> secondFuture;
    private final ListenableFuture<Either<GeneralError, V>> thirdFuture;
    private final ListenableFuture<Either<GeneralError, W>> fourthFuture;
    private final ListenableFuture<Either<GeneralError, X>> fifthFuture;
    private final AtomicInteger remaining = new AtomicInteger(5);
    private final AtomicBoolean shortCircuit = new AtomicBoolean(false);
    private volatile T firstValue = null;
    private volatile U secondValue = null;
    private volatile V thirdValue = null;
    private volatile W fourthValue = null;
    private volatile X fifthValue = null;

    public CombinedFuture5(ParallelServiceExecutor executor,
                           final ListenableFuture<Either<GeneralError, T>> firstFuture,
                           final ListenableFuture<Either<GeneralError, U>> secondFuture,
                           final ListenableFuture<Either<GeneralError, V>> thirdFuture,
                           final ListenableFuture<Either<GeneralError, W>> fourthFuture,
                           final ListenableFuture<Either<GeneralError, X>> fifthFuture
    ) {
        checkNotNull(executor, "executor is null");
        checkNotNull(firstFuture, "First future is null");
        checkNotNull(secondFuture, "Second future is null");
        checkNotNull(thirdFuture, "Third future is null");
        checkNotNull(fourthFuture, "Fourth future is null");
        checkNotNull(fifthFuture, "Fifth future is null");
        this.firstFuture = firstFuture;
        this.secondFuture = secondFuture;
        this.thirdFuture = thirdFuture;
        this.fourthFuture = fourthFuture;
        this.fifthFuture = fifthFuture;

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
                    if (fifthFuture != null && !fifthFuture.isDone()) {
                        fifthFuture.cancel(wasInterrupted());
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
        executor.addListener(fifthFuture, new Runnable() {
            @Override
            public void run() {
                setFifthValue();
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
                    set(Either.<GeneralError, T, Tuple5<T, U, V, W, X>>castLeft(returnValue));
                }
            } else {
                firstValue = Either.extractRight(returnValue);
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
                    set(Either.<GeneralError, U, Tuple5<T, U, V, W, X>>castLeft(returnValue));
                }
            } else {
                secondValue = Either.extractRight(returnValue);
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
                    set(Either.<GeneralError, V, Tuple5<T, U, V, W, X>>castLeft(returnValue));
                }
            } else {
                thirdValue = Either.extractRight(returnValue);
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
                    set(Either.<GeneralError, W, Tuple5<T, U, V, W, X>>castLeft(returnValue));
                }
            } else {
                fourthValue = Either.extractRight(returnValue);
            }
        } catch (CancellationException e) {
            cancel(false);
        } catch (Throwable t) {
            super.setException(t);
        } finally {
            checkAndSet();
        }
    }

    private void setFifthValue() {
        try {
            checkState(fifthFuture.isDone(),
                    "Tried to set value from future which is not done");
            Either<GeneralError, X> returnValue = Uninterruptibles.getUninterruptibly(fifthFuture);
            if (returnValue.isLeft()) {
                if (shortCircuit.compareAndSet(false, true)) {
                    set(Either.<GeneralError, X, Tuple5<T, U, V, W, X>>castLeft(returnValue));
                }
            } else {
                fifthValue = Either.extractRight(returnValue);
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
                checkNotNull(fifthFuture, "Fifth value is null on completion");
                set(new Right<GeneralError, Tuple5<T, U, V, W, X>>(new Tuple5<>(firstValue, secondValue, thirdValue, fourthValue, fifthValue)));
            }
        }
    }

    public static <T, U, V, W, X> CombinedFuture5<T, U, V, W, X> from(ParallelServiceExecutor executor,
                                                                      ListenableFuture<Either<GeneralError, T>> firstFuture,
                                                                      ListenableFuture<Either<GeneralError, U>> secondFuture,
                                                                      ListenableFuture<Either<GeneralError, V>> thirdFuture,
                                                                      ListenableFuture<Either<GeneralError, W>> fourthFuture,
                                                                      ListenableFuture<Either<GeneralError, X>> fifthFuture
    ) {
        return new CombinedFuture5<>(executor, firstFuture, secondFuture, thirdFuture, fourthFuture, fifthFuture);
    }
}
