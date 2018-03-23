// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;
import com.yahoo.maha.parrequest2.GeneralError;
import scala.Option;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;

/**
 * Created by jians on 3/31/15.
 */
class CombinedFutureListOption<T> extends AbstractFuture<Either<GeneralError, List<Option<T>>>> {

    private final ImmutableCollection<ListenableFuture<Either<GeneralError, T>>> futures;
    private final AtomicInteger remaining;
    private final AtomicBoolean shortCircuit = new AtomicBoolean(false);
    private final List<Option<T>> values;
    private final boolean allMustSucceed;

    CombinedFutureListOption(
        ParallelServiceExecutor executor,
        final ImmutableCollection<ListenableFuture<Either<GeneralError, T>>> futures,
        boolean allMustSucceed
    ) {
        checkNotNull(executor, "executor is null");
        checkNotNull(futures, "futures list is null");
        this.allMustSucceed = allMustSucceed;
        this.remaining = new AtomicInteger(futures.size());
        this.values = Collections.synchronizedList(Lists.<Option<T>>newArrayListWithCapacity(futures.size()));
        this.futures = futures;

        executor.addListener(this, new Runnable() {
            @Override
            public void run() {
                if (isCancelled()) {
                    for (ListenableFuture<Either<GeneralError, T>> listenableFuture: CombinedFutureListOption.this.futures) {
                        listenableFuture.cancel(wasInterrupted());
                    }
                }
            }
        });

        if (futures.isEmpty()) {
            set(new Right<GeneralError, List<Option<T>>>(CombinedFutureListOption.this.values));
            return;
        }

        for (int i = 0; i < futures.size(); ++i) {
            values.add(Option.<T>empty());
        }

        int i = 0;
        for (final ListenableFuture<Either<GeneralError, T>> listenable : CombinedFutureListOption.this.futures) {
            final int index = i++;
            executor.addListener(listenable
                , new Runnable() {
                @Override
                public void run() {
                    setOneValue(index, listenable);
                }
            });
        }
    }

    private void setOneValue(int index, ListenableFuture<Either<GeneralError, T>> listenable) {
        List<Option<T>> localValues = values;
        if (isDone()) {
            checkState(allMustSucceed || isCancelled(), "Future was done before all dependencies completed");
        }

        try {
            checkState(listenable.isDone(), "Tried to set value from future which is not done");
            Either<GeneralError, T> returnValue = getUninterruptibly(listenable);
            if (returnValue.isLeft()) {
                if (allMustSucceed && shortCircuit.compareAndSet(false, true)) {
                    for (ListenableFuture<Either<GeneralError, T>> listenableFuture: CombinedFutureListOption.this.futures) {
                        if (listenable != listenableFuture) {
                            try {
                                if(!listenableFuture.isCancelled())
                                    listenableFuture.cancel(true);
                            } catch (Throwable t) {
                                //do nothing
                            }
                        }
                    }
                    set(new Left<GeneralError, List<Option<T>>>(returnValue.left().get()));
                }
            } else {
                localValues.set(index, Option.apply(returnValue.right().get()));
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
                    set(new Right<GeneralError, List<Option<T>>>(localValues));
                }
            }
        }
    }

    public static <T> CombinedFutureListOption<T> from(ParallelServiceExecutor executor,
                                                       ImmutableCollection<ListenableFuture<Either<GeneralError, T>>> futureCollection,
                                                       boolean allMustSucceed
    ) {
        return new CombinedFutureListOption<>(executor, futureCollection, allMustSucceed);
    }
}
