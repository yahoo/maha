// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.maha.parrequest2.EitherUtils;
import scala.util.Either;
import com.yahoo.maha.parrequest2.GeneralError;
import scala.Option;
import com.yahoo.maha.parrequest2.ParCallable;
import scala.Tuple4;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class represents four parallel requests which can be composed synchronously with resultMap, or composed
 * asynchronously with map and fold
 */
public class ParRequest4<T, U, V, W> extends CombinableRequest<Tuple4<T, U, V, W>> {

    private final ParallelServiceExecutor executor;
    private final CombinedFuture4<T, U, V, W> combinedFuture4;

    ParRequest4(String label, ParallelServiceExecutor executor,
                ParCallable<Either<GeneralError, T>> firstRequest,
                ParCallable<Either<GeneralError, U>> secondRequest,
                ParCallable<Either<GeneralError, V>> thirdRequest,
                ParCallable<Either<GeneralError, W>> fourthRequest) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(firstRequest, "First request is null");
        checkNotNull(secondRequest, "Second request is null");
        checkNotNull(thirdRequest, "Third request is null");
        checkNotNull(fourthRequest, "Fourth request is null");
        this.label = label;
        this.executor = executor;

        //fire requests
        final ListenableFuture<Either<GeneralError, T>> firstFuture = executor.submitParCallable(firstRequest);
        final ListenableFuture<Either<GeneralError, U>> secondFuture = executor.submitParCallable(secondRequest);
        final ListenableFuture<Either<GeneralError, V>> thirdFuture = executor.submitParCallable(thirdRequest);
        final ListenableFuture<Either<GeneralError, W>> fourthFuture = executor.submitParCallable(fourthRequest);

        try {
            combinedFuture4 = CombinedFuture4.from(executor, firstFuture, secondFuture, thirdFuture, fourthFuture);
        } catch (Exception e) {
            //failed to create combiner, cancel futures and re-throw exception
            firstFuture.cancel(false);
            secondFuture.cancel(false);
            thirdFuture.cancel(false);
            fourthFuture.cancel(false);
            throw e;
        }
    }

    ParRequest4(String label, ParallelServiceExecutor executor,
                CombinableRequest<T> firstRequest,
                CombinableRequest<U> secondRequest,
                CombinableRequest<V> thirdRequest,
                CombinableRequest<W> fourthRequest) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(firstRequest, "First request is null");
        checkNotNull(secondRequest, "Second request is null");
        checkNotNull(thirdRequest, "Third request is null");
        checkNotNull(fourthRequest, "Fourth request is null");
        this.label = label;
        this.executor = executor;
        combinedFuture4 = CombinedFuture4.from(
                executor, firstRequest.asFuture(), secondRequest.asFuture(), thirdRequest.asFuture(), fourthRequest.asFuture());
    }

    public <O> Either<GeneralError, O> resultMap(ParFunction<Tuple4<T, U, V, W>, O> fn) {
        Either<GeneralError, Tuple4<T, U, V, W>> result = executor.getEitherSafely(label, combinedFuture4);
        return EitherUtils.map(fn, result);
    }

    public <O> NoopRequest<O> fold(ParFunction<GeneralError, O> errFn, ParFunction<Tuple4<T, U, V, W>, O> fn) {
        return new NoopRequest<O>(executor, new FoldableFuture<>(executor, combinedFuture4, fn, errFn));
    }

    public <O> ParRequest<O> map(String label, ParFunction<Tuple4<T, U, V, W>, Either<GeneralError, O>> fn) {
        return new ParRequest<>(label, executor, new ComposableFuture<>(executor, combinedFuture4, fn));
    }

    public <O> ParRequest<O> flatMap(String label, ParFunction<Tuple4<T, U, V, W>, CombinableRequest<O>> fn) {
        return new ParRequest<>(label, executor, new ComposableFutureFuture<>(executor, combinedFuture4, fn));
    }

    public Either<GeneralError, Tuple4<T, U, V, W>> get() {
        return executor.getEitherSafely(label, combinedFuture4);
    }

    ListenableFuture<Either<GeneralError, Tuple4<T, U, V, W>>> asFuture() {
        return combinedFuture4;
    }

    public static class Builder<A, B, C, D> {

        private final ParallelServiceExecutor executor;
        private Option<ParCallable<Either<GeneralError, A>>> firstParCallable = Option.empty();
        private Option<ParCallable<Either<GeneralError, B>>> secondParCallable = Option.empty();
        private Option<ParCallable<Either<GeneralError, C>>> thirdParCallable = Option.empty();
        private Option<ParCallable<Either<GeneralError, D>>> fourthParCallable = Option.empty();
        private boolean built = false;
        private String label = "changethis";

        public Builder<A, B, C, D> setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder(ParallelServiceExecutor executor) {
            this.executor = executor;
        }

        public Builder<A, B, C, D> setFirstParCallable(ParCallable<Either<GeneralError, A>> parCallable) {
            checkState(firstParCallable.isEmpty(), "Cannot set the first parCallable twice!");
            firstParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D> setSecondParCallable(ParCallable<Either<GeneralError, B>> parCallable) {
            checkState(secondParCallable.isEmpty(), "Cannot set the second parCallable twice!");
            secondParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D> setThirdParCallable(ParCallable<Either<GeneralError, C>> parCallable) {
            checkState(thirdParCallable.isEmpty(), "Cannot set the third parCallable twice!");
            thirdParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D> setFourthParCallable(ParCallable<Either<GeneralError, D>> parCallable) {
            checkState(fourthParCallable.isEmpty(), "Cannot set the fourth parCallable twice!");
            fourthParCallable = Option.apply(parCallable);
            return this;
        }

        public ParRequest4<A, B, C, D> build() {
            checkState(!built, "Cannot build a request twice!");
            checkState(firstParCallable.isDefined(), "First parCallable not defined!");
            checkState(secondParCallable.isDefined(), "Second parCallable not defined!");
            checkState(thirdParCallable.isDefined(), "Third parCallable not defined!");
            checkState(fourthParCallable.isDefined(), "Fourth parCallable not defined!");
            try {
                ParCallable<Either<GeneralError, A>> first = ParCallable.from(firstParCallable.get());
                ParCallable<Either<GeneralError, B>> second = ParCallable.from(secondParCallable.get());
                ParCallable<Either<GeneralError, C>> third = ParCallable.from(thirdParCallable.get());
                ParCallable<Either<GeneralError, D>> fourth = ParCallable.from(fourthParCallable.get());
                return new ParRequest4<>(label, executor, first, second, third, fourth);
            } finally {
                built = true;
            }
        }
    }
}

