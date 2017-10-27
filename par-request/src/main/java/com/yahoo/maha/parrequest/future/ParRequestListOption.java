// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest.future;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.maha.parrequest.Either;
import com.yahoo.maha.parrequest.Option;
import com.yahoo.maha.parrequest.ParCallable;
import com.yahoo.maha.parrequest.GeneralError;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Created by jians on 3/31/15.
 */
public class ParRequestListOption<T> extends CombinableRequest<List<Option<T>>> {

    private final ParallelServiceExecutor executor;
    private final CombinedFutureListOption<T> combinedFutureList;

    ParRequestListOption(String label
        , ParallelServiceExecutor executor
        , List<ParCallable<Either<GeneralError, T>>> requestList
        , boolean allMustSucceed
    ) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(requestList, "Request is null");
        this.label = label;
        this.executor = executor;

        //fire requests
        ImmutableList.Builder<ListenableFuture<Either<GeneralError, T>>> futuresBuilder = ImmutableList.builder();
        for (ParCallable<Either<GeneralError, T>> request : requestList) {
            futuresBuilder.add(executor.submitParCallable(request));
        }

        final ImmutableList<ListenableFuture<Either<GeneralError, T>>> futures = futuresBuilder.build();
        try {
            combinedFutureList = CombinedFutureListOption.from(executor, futures, allMustSucceed);
        } catch (Exception e) {
            for (ListenableFuture<Either<GeneralError, T>> future : futures) {
                future.cancel(false);
            }
            throw e;
        }
    }

    ParRequestListOption(String label
            , ParallelServiceExecutor executor
            , ArrayList<CombinableRequest<T>> requestList
            , boolean allMustSucceed) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(requestList, "Request List is null");
        this.label = label;
        this.executor = executor;

        ImmutableList.Builder<ListenableFuture<Either<GeneralError, T>>> futuresBuilder = ImmutableList.builder();
        for (CombinableRequest<T> req : requestList) {
            futuresBuilder.add(req.asFuture());
        }
        combinedFutureList = CombinedFutureListOption.from(executor, futuresBuilder.build(), allMustSucceed);
    }

    @Override
    ListenableFuture<Either<GeneralError, List<Option<T>>>> asFuture() {
        return combinedFutureList;
    }

    public <O> Either<GeneralError, O> resultMap(ParFunction<List<Option<T>>, O> fn) {
        Either<GeneralError, List<Option<T>>> result = executor.getEitherSafely(label, combinedFutureList);
        return result.map(fn);
    }

    public <O> NoopRequest<O> fold(ParFunction<GeneralError, O> errFn, ParFunction<List<Option<T>>, O> fn) {
        return new NoopRequest<O>(executor, new FoldableFuture<>(executor, combinedFutureList, fn, errFn));
    }

    public <O> ParRequest<O> map(String label, ParFunction<List<Option<T>>, Either<GeneralError, O>> fn) {
        return new ParRequest<>(label, executor, new ComposableFuture<>(executor, combinedFutureList, fn));
    }

    public <O> ParRequest<O> flatMap(String label, ParFunction<List<Option<T>>, CombinableRequest<O>> fn) {
        return new ParRequest<>(label, executor, new ComposableFutureFuture<>(executor, combinedFutureList, fn));
    }

    public Either<GeneralError, List<Option<T>>> get() {
        return executor.getEitherSafely(label, combinedFutureList);
    }

    public static class Builder<T> {

        private final ParallelServiceExecutor executor;
        private final ImmutableList.Builder<ParCallable<Either<GeneralError, T>>>
            parCallablesBuilder =
            ImmutableList.builder();
        private boolean built = false;
        private boolean allMustSucceed = false;
        private String label = "changethis";

        public Builder<T> setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder(ParallelServiceExecutor executor) {
            this.executor = executor;
        }

        public Builder<T> addParCallable(ParCallable<Either<GeneralError, T>> parCallable) {
            checkNotNull(parCallable, "parCallable is null");
            parCallablesBuilder.add(parCallable);
            return this;
        }

        public Builder<T> allMustSucceed(boolean allMustSucceed) {
            this.allMustSucceed = allMustSucceed;
            return this;
        }

        public ParRequestListOption<T> build() {
            checkState(!built, "Cannot build a request twice!");
            ImmutableList<ParCallable<Either<GeneralError, T>>> parCallableList = parCallablesBuilder.build();
            try {
                return new ParRequestListOption<>(label, executor, parCallableList, allMustSucceed);
            } finally {
                built = true;
            }
        }
    }
}
