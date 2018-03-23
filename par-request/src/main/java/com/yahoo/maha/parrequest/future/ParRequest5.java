// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest.future;

import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.maha.parrequest.Either;
import com.yahoo.maha.parrequest.GeneralError;
import com.yahoo.maha.parrequest.Option;
import com.yahoo.maha.parrequest.ParCallable;
import scala.Tuple5;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class represents five parallel requests which can be composed synchronously with resultMap, or composed
 * asynchronously with map and fold
 */
public class ParRequest5<T, U, V, W, X> extends CombinableRequest<Tuple5<T, U, V, W, X>> {

    private final ParallelServiceExecutor executor;
    private final CombinedFuture5<T, U, V, W, X> combinedFuture5;

    ParRequest5(String label, ParallelServiceExecutor executor,
                ParCallable<Either<GeneralError, T>> firstRequest,
                ParCallable<Either<GeneralError, U>> secondRequest,
                ParCallable<Either<GeneralError, V>> thirdRequest,
                ParCallable<Either<GeneralError, W>> fourthRequest,
                ParCallable<Either<GeneralError, X>> fifthRequest) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(firstRequest, "First request is null");
        checkNotNull(secondRequest, "Second request is null");
        checkNotNull(thirdRequest, "Third request is null");
        checkNotNull(fourthRequest, "Fourth request is null");
        checkNotNull(fifthRequest, "Fifth request is null");
        this.label = label;
        this.executor = executor;

        //fire requests
        final ListenableFuture<Either<GeneralError, T>> firstFuture = executor.submitParCallable(firstRequest);
        final ListenableFuture<Either<GeneralError, U>> secondFuture = executor.submitParCallable(secondRequest);
        final ListenableFuture<Either<GeneralError, V>> thirdFuture = executor.submitParCallable(thirdRequest);
        final ListenableFuture<Either<GeneralError, W>> fourthFuture = executor.submitParCallable(fourthRequest);
        final ListenableFuture<Either<GeneralError, X>> fifthFuture = executor.submitParCallable(fifthRequest);

        try {
            combinedFuture5 = CombinedFuture5.from(executor, firstFuture, secondFuture, thirdFuture, fourthFuture, fifthFuture);
        } catch (Exception e) {
            //failed to create combiner, cancel futures and re-throw exception
            firstFuture.cancel(false);
            secondFuture.cancel(false);
            thirdFuture.cancel(false);
            fourthFuture.cancel(false);
            fifthFuture.cancel(false);
            throw e;
        }
    }

    ParRequest5(String label, ParallelServiceExecutor executor,
                CombinableRequest<T> firstRequest,
                CombinableRequest<U> secondRequest,
                CombinableRequest<V> thirdRequest,
                CombinableRequest<W> fourthRequest,
                CombinableRequest<X> fifthRequest) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(firstRequest, "First request is null");
        checkNotNull(secondRequest, "Second request is null");
        checkNotNull(thirdRequest, "Third request is null");
        checkNotNull(fourthRequest, "Fourth request is null");
        checkNotNull(fifthRequest, "Fifth request is null");
        this.label = label;
        this.executor = executor;
        combinedFuture5 = CombinedFuture5.from(
                executor, firstRequest.asFuture(), secondRequest.asFuture(), thirdRequest.asFuture(), fourthRequest.asFuture(), fifthRequest.asFuture());
    }

    public <O> Either<GeneralError, O> resultMap(ParFunction<Tuple5<T, U, V, W, X>, O> fn) {
        Either<GeneralError, Tuple5<T, U, V, W, X>> result = executor.getEitherSafely(label, combinedFuture5);
        return result.map(fn);
    }

    public <O> NoopRequest<O> fold(ParFunction<GeneralError, O> errFn, ParFunction<Tuple5<T, U, V, W, X>, O> fn) {
        return new NoopRequest<O>(executor, new FoldableFuture<>(executor, combinedFuture5, fn, errFn));
    }

    public <O> ParRequest<O> map(String label, ParFunction<Tuple5<T, U, V, W, X>, Either<GeneralError, O>> fn) {
        return new ParRequest<>(label, executor, new ComposableFuture<>(executor, combinedFuture5, fn));
    }

    public <O> ParRequest<O> flatMap(String label, ParFunction<Tuple5<T, U, V, W, X>, CombinableRequest<O>> fn) {
        return new ParRequest<>(label, executor, new ComposableFutureFuture<>(executor, combinedFuture5, fn));
    }

    public Either<GeneralError, Tuple5<T, U, V, W, X>> get() {
        return executor.getEitherSafely(label, combinedFuture5);
    }

    ListenableFuture<Either<GeneralError, Tuple5<T, U, V, W, X>>> asFuture() {
        return combinedFuture5;
    }

    public static class Builder<A, B, C, D, E> {

        private final ParallelServiceExecutor executor;
        private Option<ParCallable<Either<GeneralError, A>>> firstParCallable = Option.none();
        private Option<ParCallable<Either<GeneralError, B>>> secondParCallable = Option.none();
        private Option<ParCallable<Either<GeneralError, C>>> thirdParCallable = Option.none();
        private Option<ParCallable<Either<GeneralError, D>>> fourthParCallable = Option.none();
        private Option<ParCallable<Either<GeneralError, E>>> fifthParCallable = Option.none();
        private boolean built = false;
        private String label = "changethis";

        public Builder<A, B, C, D, E> setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder(ParallelServiceExecutor executor) {
            this.executor = executor;
        }

        public Builder<A, B, C, D, E> setFirstParCallable(ParCallable<Either<GeneralError, A>> parCallable) {
            checkState(firstParCallable.isEmpty(), "Cannot set the first parCallable twice!");
            firstParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D, E> setSecondParCallable(ParCallable<Either<GeneralError, B>> parCallable) {
            checkState(secondParCallable.isEmpty(), "Cannot set the second parCallable twice!");
            secondParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D, E> setThirdParCallable(ParCallable<Either<GeneralError, C>> parCallable) {
            checkState(thirdParCallable.isEmpty(), "Cannot set the third parCallable twice!");
            thirdParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D, E> setFourthParCallable(ParCallable<Either<GeneralError, D>> parCallable) {
            checkState(fourthParCallable.isEmpty(), "Cannot set the fourth parCallable twice!");
            fourthParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D, E> setFifthParCallable(ParCallable<Either<GeneralError, E>> parCallable) {
            checkState(fifthParCallable.isEmpty(), "Cannot set the fifth parCallable twice!");
            fifthParCallable = Option.apply(parCallable);
            return this;
        }

        public ParRequest5<A, B, C, D, E> build() {
            checkState(!built, "Cannot build a request twice!");
            checkState(firstParCallable.isDefined(), "First parCallable not defined!");
            checkState(secondParCallable.isDefined(), "Second parCallable not defined!");
            checkState(thirdParCallable.isDefined(), "Third parCallable not defined!");
            checkState(fourthParCallable.isDefined(), "Fourth parCallable not defined!");
            checkState(fifthParCallable.isDefined(), "Fifth parCallable not defined!");
            try {
                ParCallable<Either<GeneralError, A>> first = ParCallable.from(firstParCallable.get());
                ParCallable<Either<GeneralError, B>> second = ParCallable.from(secondParCallable.get());
                ParCallable<Either<GeneralError, C>> third = ParCallable.from(thirdParCallable.get());
                ParCallable<Either<GeneralError, D>> fourth = ParCallable.from(fourthParCallable.get());
                ParCallable<Either<GeneralError, E>> fifth = ParCallable.from(fifthParCallable.get());
                return new ParRequest5<>(label, executor, first, second, third, fourth, fifth);
            } finally {
                built = true;
            }
        }
    }
}
