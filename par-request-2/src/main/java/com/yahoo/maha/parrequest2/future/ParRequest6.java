// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.maha.parrequest2.EitherUtils;
import com.yahoo.maha.parrequest2.ParCallable;
import scala.util.Either;
import com.yahoo.maha.parrequest2.GeneralError;
import scala.Option;
import scala.Tuple6;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class represents six parallel requests which can be composed synchronously with resultMap, or composed
 * asynchronously with map and fold
 */
public class ParRequest6<T, U, V, W, X, Y> extends CombinableRequest<Tuple6<T, U, V, W, X, Y>> {

    private final ParallelServiceExecutor executor;
    private final CombinedFuture6<T, U, V, W, X, Y> combinedFuture6;

    ParRequest6(String label, ParallelServiceExecutor executor,
                ParCallable<Either<GeneralError, T>> firstRequest,
                ParCallable<Either<GeneralError, U>> secondRequest,
                ParCallable<Either<GeneralError, V>> thirdRequest,
                ParCallable<Either<GeneralError, W>> fourthRequest,
                ParCallable<Either<GeneralError, X>> fifthRequest,
                ParCallable<Either<GeneralError, Y>> sixthRequest) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(firstRequest, "First request is null");
        checkNotNull(secondRequest, "Second request is null");
        checkNotNull(thirdRequest, "Third request is null");
        checkNotNull(fourthRequest, "Fourth request is null");
        checkNotNull(fifthRequest, "Fifth request is null");
        checkNotNull(sixthRequest, "Sixth request is null");
        this.label = label;
        this.executor = executor;

        //fire requests
        final ListenableFuture<Either<GeneralError, T>> firstFuture = executor.submitParCallable(firstRequest);
        final ListenableFuture<Either<GeneralError, U>> secondFuture = executor.submitParCallable(secondRequest);
        final ListenableFuture<Either<GeneralError, V>> thirdFuture = executor.submitParCallable(thirdRequest);
        final ListenableFuture<Either<GeneralError, W>> fourthFuture = executor.submitParCallable(fourthRequest);
        final ListenableFuture<Either<GeneralError, X>> fifthFuture = executor.submitParCallable(fifthRequest);
        final ListenableFuture<Either<GeneralError, Y>> sixthFuture = executor.submitParCallable(sixthRequest);

        try {
            combinedFuture6 = CombinedFuture6.from(executor, firstFuture, secondFuture, thirdFuture, fourthFuture, fifthFuture, sixthFuture);
        } catch (Exception e) {
            //failed to create combiner, cancel futures and re-throw exception
            firstFuture.cancel(false);
            secondFuture.cancel(false);
            thirdFuture.cancel(false);
            fourthFuture.cancel(false);
            fifthFuture.cancel(false);
            sixthFuture.cancel(false);
            throw e;
        }
    }

    ParRequest6(String label, ParallelServiceExecutor executor,
                CombinableRequest<T> firstRequest,
                CombinableRequest<U> secondRequest,
                CombinableRequest<V> thirdRequest,
                CombinableRequest<W> fourthRequest,
                CombinableRequest<X> fifthRequest,
                CombinableRequest<Y> sixthRequest) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(firstRequest, "First request is null");
        checkNotNull(secondRequest, "Second request is null");
        checkNotNull(thirdRequest, "Third request is null");
        checkNotNull(fourthRequest, "Fourth request is null");
        checkNotNull(fifthRequest, "Fifth request is null");
        checkNotNull(sixthRequest, "Sixth request is null");
        this.label = label;
        this.executor = executor;
        combinedFuture6 = CombinedFuture6.from(
                executor, firstRequest.asFuture(), secondRequest.asFuture(), thirdRequest.asFuture(), fourthRequest.asFuture(), fifthRequest.asFuture(), sixthRequest.asFuture());
    }

    public <O> Either<GeneralError, O> resultMap(ParFunction<Tuple6<T, U, V, W, X, Y>, O> fn) {
        Either<GeneralError, Tuple6<T, U, V, W, X, Y>> result = executor.getEitherSafely(label, combinedFuture6);
        return EitherUtils.map(fn, result);
    }

    public <O> NoopRequest<O> fold(ParFunction<GeneralError, O> errFn, ParFunction<Tuple6<T, U, V, W, X, Y>, O> fn) {
        return new NoopRequest<O>(executor, new FoldableFuture<>(executor, combinedFuture6, fn, errFn));
    }

    public <O> ParRequest<O> map(String label, ParFunction<Tuple6<T, U, V, W, X, Y>, Either<GeneralError, O>> fn) {
        return new ParRequest<>(label, executor, new ComposableFuture<>(executor, combinedFuture6, fn));
    }

    public <O> ParRequest<O> flatMap(String label, ParFunction<Tuple6<T, U, V, W, X, Y>, CombinableRequest<O>> fn) {
        return new ParRequest<>(label, executor, new ComposableFutureFuture<>(executor, combinedFuture6, fn));
    }

    public Either<GeneralError, Tuple6<T, U, V, W, X, Y>> get() {
        return executor.getEitherSafely(label, combinedFuture6);
    }

    ListenableFuture<Either<GeneralError, Tuple6<T, U, V, W, X, Y>>> asFuture() {
        return combinedFuture6;
    }

    public static class Builder<A, B, C, D, E, F> {

        private final ParallelServiceExecutor executor;
        private Option<ParCallable<Either<GeneralError, A>>> firstParCallable = Option.empty();
        private Option<ParCallable<Either<GeneralError, B>>> secondParCallable = Option.empty();
        private Option<ParCallable<Either<GeneralError, C>>> thirdParCallable = Option.empty();
        private Option<ParCallable<Either<GeneralError, D>>> fourthParCallable = Option.empty();
        private Option<ParCallable<Either<GeneralError, E>>> fifthParCallable = Option.empty();
        private Option<ParCallable<Either<GeneralError, F>>> sixthParCallable = Option.empty();
        private boolean built = false;
        private String label = "changethis";

        public Builder<A, B, C, D, E, F> setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder(ParallelServiceExecutor executor) {
            this.executor = executor;
        }

        public Builder<A, B, C, D, E, F> setFirstParCallable(ParCallable<Either<GeneralError, A>> parCallable) {
            checkState(firstParCallable.isEmpty(), "Cannot set the first parCallable twice!");
            firstParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D, E, F> setSecondParCallable(ParCallable<Either<GeneralError, B>> parCallable) {
            checkState(secondParCallable.isEmpty(), "Cannot set the second parCallable twice!");
            secondParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D, E, F> setThirdParCallable(ParCallable<Either<GeneralError, C>> parCallable) {
            checkState(thirdParCallable.isEmpty(), "Cannot set the third parCallable twice!");
            thirdParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D, E, F> setFourthParCallable(ParCallable<Either<GeneralError, D>> parCallable) {
            checkState(fourthParCallable.isEmpty(), "Cannot set the fourth parCallable twice!");
            fourthParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D, E, F> setFifthParCallable(ParCallable<Either<GeneralError, E>> parCallable) {
            checkState(fifthParCallable.isEmpty(), "Cannot set the fifth parCallable twice!");
            fifthParCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A, B, C, D, E, F> setSixthParCallable(ParCallable<Either<GeneralError, F>> parCallable) {
            checkState(sixthParCallable.isEmpty(), "Cannot set the sixth parCallable twice!");
            sixthParCallable = Option.apply(parCallable);
            return this;
        }

        public ParRequest6<A, B, C, D, E, F> build() {
            checkState(!built, "Cannot build a request twice!");
            checkState(firstParCallable.isDefined(), "First parCallable not defined!");
            checkState(secondParCallable.isDefined(), "Second parCallable not defined!");
            checkState(thirdParCallable.isDefined(), "Third parCallable not defined!");
            checkState(fourthParCallable.isDefined(), "Fourth parCallable not defined!");
            checkState(fifthParCallable.isDefined(), "Fifth parCallable not defined!");
            checkState(sixthParCallable.isDefined(), "Sixth parCallable not defined!");
            try {
                ParCallable<Either<GeneralError, A>> first = ParCallable.from(firstParCallable.get());
                ParCallable<Either<GeneralError, B>> second = ParCallable.from(secondParCallable.get());
                ParCallable<Either<GeneralError, C>> third = ParCallable.from(thirdParCallable.get());
                ParCallable<Either<GeneralError, D>> fourth = ParCallable.from(fourthParCallable.get());
                ParCallable<Either<GeneralError, E>> fifth = ParCallable.from(fifthParCallable.get());
                ParCallable<Either<GeneralError, F>> sixth = ParCallable.from(sixthParCallable.get());
                return new ParRequest6<>(label, executor, first, second, third, fourth, fifth, sixth);
            } finally {
                built = true;
            }
        }
    }
}
