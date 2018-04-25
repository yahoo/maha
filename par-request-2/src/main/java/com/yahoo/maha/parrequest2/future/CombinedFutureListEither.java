// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.maha.parrequest2.GeneralError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;

/**
 * Created by hiral on 4/11/18.
 */
class CombinedFutureListEither<T> extends AbstractFuture<Either<GeneralError, List<Either<GeneralError, T>>>> {

    private static final Either<GeneralError, ?> DEFAULT_VALUE = new Left<>(GeneralError.from("init", "default error, either was not set"));
    private static final Logger LOGGER = LoggerFactory.getLogger(CombinedFutureListEither.class);

    private final ImmutableCollection<ListenableFuture<Either<GeneralError, T>>> futures;
    private final AtomicInteger remaining;
    private final AtomicBoolean shortCircuit = new AtomicBoolean(false);
    private final List<Either<GeneralError, T>> values;
    private final boolean allMustSucceed;

    CombinedFutureListEither(
            ParallelServiceExecutor executor,
            final ImmutableCollection<ListenableFuture<Either<GeneralError, T>>> futures,
            boolean allMustSucceed
    ) {
        checkNotNull(executor, "executor is null");
        checkNotNull(futures, "futures list is null");
        this.allMustSucceed = allMustSucceed;
        this.remaining = new AtomicInteger(futures.size());
        this.values = Collections.synchronizedList(Lists.<Either<GeneralError, T>>newArrayListWithCapacity(futures.size()));
        this.futures = futures;

        executor.addListener(this, new Runnable() {
            @Override
            public void run() {
                if (isCancelled()) {
                    for (ListenableFuture<Either<GeneralError, T>> listenableFuture: CombinedFutureListEither.this.futures) {
                        if(!listenableFuture.isCancelled()) {
                            listenableFuture.cancel(wasInterrupted());
                        }
                    }
                }
            }
        });

        if (futures.isEmpty()) {
            set(new Right<GeneralError, List<Either<GeneralError, T>>>(CombinedFutureListEither.this.values));
            return;
        }

        for (int i = 0; i < futures.size(); ++i) {
            values.add(getDefaultError());
        }

        int i = 0;
        for (final ListenableFuture<Either<GeneralError, T>> listenable : CombinedFutureListEither.this.futures) {
            final int index = i++;
            executor.addListener(listenable
                    , new Runnable() {
                        @Override
                        public void run() {
                            try {
                                setOneValue(index, listenable);
                            } catch (Exception e) {
                                LOGGER.error("Failed to set index", e);
                            }
                        }
                    });
        }
    }

    private Either<GeneralError, T> getDefaultError() {
        return (Either<GeneralError, T>) DEFAULT_VALUE;
    }

    private void setOneValue(int index, ListenableFuture<Either<GeneralError, T>> listenable) {
        List<Either<GeneralError, T>> localValues = values;
        if (isDone()) {
            checkState(allMustSucceed || isCancelled(), "Future was done before all dependencies completed");
        }

        checkState(!isCancelled(), "Cannot set value after cancelled");

        try {
            checkState(listenable.isDone(), "Tried to set value from future which is not done");
            Either<GeneralError, T> returnValue = getUninterruptibly(listenable);
            if (returnValue.isLeft()) {
                if (allMustSucceed && shortCircuit.compareAndSet(false, true)) {
                    for (ListenableFuture<Either<GeneralError, T>> listenableFuture: CombinedFutureListEither.this.futures) {
                        if (listenable != listenableFuture) {
                            try {
                                if(!listenableFuture.isCancelled())
                                    listenableFuture.cancel(true);
                            } catch (Throwable t) {
                                //do nothing
                            }
                        }
                    }
                    set(new Left<GeneralError, List<Either<GeneralError, T>>>(returnValue.left().get()));
                } else {
                    localValues.set(index, new Left<>(returnValue.left().get()));
                }
            } else {
                localValues.set(index, new Right<>(returnValue.right().get()));
            }
        } catch (CancellationException e) {
            if (allMustSucceed && !this.isCancelled()) {
                cancel(false);
            }
        } catch (Throwable t) {
            if (allMustSucceed) {
                super.setException(t);
            }
        } finally {
            int newRemaining = remaining.decrementAndGet();
            checkState(newRemaining >= 0, "Less than 0 remaining futures");
            if (newRemaining == 0) {
                if (!shortCircuit.get() && !isDone()) {
                    set(new Right<GeneralError, List<Either<GeneralError, T>>>(localValues));
                }
            }
        }
    }

    public static <T> CombinedFutureListEither from(ParallelServiceExecutor executor,
                                                       ImmutableCollection<ListenableFuture<Either<GeneralError, T>>> futureCollection,
                                                       boolean allMustSucceed
    ) {
        return new CombinedFutureListEither<>(executor, futureCollection, allMustSucceed);
    }
}
