// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.maha.parrequest2.EitherUtils;
import com.yahoo.maha.parrequest2.ParCallable;
import scala.util.Either;
import com.yahoo.maha.parrequest2.GeneralError;
import scala.Option;

import scala.Tuple3;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class represents three parallel requests which can be composed synchronously with resultMap, or composed
 * asynchronously with map and fold
 */
public class ParRequest3<T, U, V> extends CombinableRequest<Tuple3<T, U, V>> {

    private final ParallelServiceExecutor executor;
    private final CombinedFuture3<T, U, V> combinedFuture3;

    ParRequest3(String label, ParallelServiceExecutor executor,
                ParCallable<Either<GeneralError, T>> firstRequest,
                ParCallable<Either<GeneralError, U>> secondRequest,
                ParCallable<Either<GeneralError, V>> thirdRequest) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(firstRequest, "First request is null");
        checkNotNull(secondRequest, "Second request is null");
        checkNotNull(thirdRequest, "Third request is null");
        this.label = label;
        this.executor = executor;

        //fire requests
        final ListenableFuture<Either<GeneralError, T>> firstFuture = executor.submitParCallable(firstRequest);
        final ListenableFuture<Either<GeneralError, U>> secondFuture = executor.submitParCallable(secondRequest);
        final ListenableFuture<Either<GeneralError, V>> thirdFuture = executor.submitParCallable(thirdRequest);

        try {
            combinedFuture3 = CombinedFuture3.from(executor, firstFuture, secondFuture, thirdFuture);
        } catch (Exception e) {
            //failed to create combiner, cancel futures and re-throw exception
            firstFuture.cancel(false);
            secondFuture.cancel(false);
            thirdFuture.cancel(false);
            throw e;
        }
    }

    ParRequest3(String label, ParallelServiceExecutor executor,
                CombinableRequest<T> firstRequest,
                CombinableRequest<U> secondRequest,
                CombinableRequest<V> thirdRequest) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(firstRequest, "First request is null");
        checkNotNull(secondRequest, "Second request is null");
        checkNotNull(thirdRequest, "Third request is null");
        this.label = label;
        this.executor = executor;
        combinedFuture3 = CombinedFuture3.from(
            executor, firstRequest.asFuture(), secondRequest.asFuture(), thirdRequest.asFuture());
    }

    public <O> Either<GeneralError, O> resultMap(ParFunction<Tuple3<T, U, V>, O> fn) {
        Either<GeneralError, Tuple3<T, U, V>> result = executor.getEitherSafely(label, combinedFuture3);
        return EitherUtils.map(fn, result);
    }

    public <O> NoopRequest<O> fold(ParFunction<GeneralError, O> errFn, ParFunction<Tuple3<T, U, V>, O> fn) {
        return new NoopRequest<O>(executor, new FoldableFuture<>(executor, combinedFuture3, fn, errFn));
    }

    public <O> ParRequest<O> map(String label, ParFunction<Tuple3<T, U, V>, Either<GeneralError, O>> fn) {
        return new ParRequest<>(label, executor, new ComposableFuture<>(executor, combinedFuture3, fn));
    }

    public <O> ParRequest<O> flatMap(String label, ParFunction<Tuple3<T, U, V>, CombinableRequest<O>> fn) {
        return new ParRequest<>(label, executor, new ComposableFutureFuture<>(executor, combinedFuture3, fn));
    }

    public Either<GeneralError, Tuple3<T, U, V>> get() {
        return executor.getEitherSafely(label, combinedFuture3);
    }

    ListenableFuture<Either<GeneralError, Tuple3<T, U, V>>> asFuture() {
        return combinedFuture3;
    }

    public static class Builder<A, B, C> {

        private final ParallelServiceExecutor executor;
        private Option<ParCallable<Either<GeneralError, A>>> firstParCallable = Option.empty();
        private Option<ParCallable<Either<GeneralError, B>>> secondParCallable = Option.empty();
        private Option<ParCallable<Either<GeneralError, C>>> thirdParCallable = Option.empty();
        private boolean built = false;
        private String label = "changethis";

        public Builder<A, B, C> setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder(ParallelServiceExecutor executor) {
            this.executor = executor;
        }

        public Builder<A, B, C> setFirstParCallable(ParCallable<Either<GeneralError, A>> parCallable) {
            checkState(firstParCallable.isEmpty(), "Cannot set the first parCallable twice!");
            firstParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C> setSecondParCallable(ParCallable<Either<GeneralError, B>> parCallable) {
            checkState(secondParCallable.isEmpty(), "Cannot set the second parCallable twice!");
            secondParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C> setThirdParCallable(ParCallable<Either<GeneralError, C>> parCallable) {
            checkState(thirdParCallable.isEmpty(), "Cannot set the third parCallable twice!");
            thirdParCallable = Option.apply(parCallable);
            return this;
        }

        public ParRequest3<A, B, C> build() {
            checkState(!built, "Cannot build a request twice!");
            checkState(firstParCallable.isDefined(), "First parCallable not defined!");
            checkState(secondParCallable.isDefined(), "Second parCallable not defined!");
            checkState(thirdParCallable.isDefined(), "Third parCallable not defined!");
            try {
                ParCallable<Either<GeneralError, A>> first = ParCallable.from(firstParCallable.get());
                ParCallable<Either<GeneralError, B>> second = ParCallable.from(secondParCallable.get());
                ParCallable<Either<GeneralError, C>> third = ParCallable.from(thirdParCallable.get());
                return new ParRequest3<>(label, executor, first, second, third);
            } finally {
                built = true;
            }
        }
    }
}
