// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.maha.parrequest2.EitherUtils;
import scala.util.Either;
import com.yahoo.maha.parrequest2.GeneralError;
import scala.Option;
import com.yahoo.maha.parrequest2.ParCallable;
import scala.Function1;
import scala.Unit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class represents a parallel request which can be composed synchronously with resultMap, or composed
 * asynchronously with map and fold
 */
public class ParRequest<T> extends CombinableRequest<T> {

    private final ParallelServiceExecutor executor;
    private final ListenableFuture<Either<GeneralError, T>> future;

    ParRequest(String label, ParallelServiceExecutor executor, ParCallable<Either<GeneralError, T>> request) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(request, "Request is null");
        this.label = label;
        this.executor = executor;

        //fire request
        this.future = executor.submitParCallable(request);
    }

    ParRequest(String label, ParallelServiceExecutor executor, ListenableFuture<Either<GeneralError, T>> future) {
        checkNotNull(executor, "Executor is null");
        checkNotNull(future, "Future is null");
        this.label = label;
        this.executor = executor;

        this.future = future;
    }

    public <O> Either<GeneralError, O> resultMap(ParFunction<T, O> fn) {
        Either<GeneralError, T> result = executor.getEitherSafely(label, future);
        return EitherUtils.map(fn, result);
    }

    public <O> NoopRequest<O> fold(ParFunction<GeneralError, O> errFn, ParFunction<T, O> fn) {
        return new NoopRequest<>(executor, new FoldableFuture<>(executor, future, fn, errFn));
    }

    /**
     * The reason we need a label here is because we are creating a new ParRequest.
     */
    public <O> ParRequest<O> map(String label, ParFunction<T, Either<GeneralError, O>> fn) {
        return new ParRequest<O>(label, executor, new ComposableFuture<>(executor, future, fn));
    }

    /**
     * *
     */
    public <O> ParRequest<O> flatMap(String label, ParFunction<T, CombinableRequest<O>> fn) {
        return new ParRequest<O>(label, executor, new ComposableFutureFuture<>(executor, future, fn));
    }

    public Either<GeneralError, T> get(long timeoutMillis) {
        return executor.getEitherSafely(label, future, timeoutMillis);
    }

    public Either<GeneralError, T> get() {
        return executor.getEitherSafely(label, future);
    }

    ListenableFuture<Either<GeneralError, T>> asFuture() {
        return future;
    }

    public static class Builder<A> {

        private final ParallelServiceExecutor executor;
        private Option<ParCallable<Either<GeneralError, A>>> parCallable = Option.empty();
        private boolean built = false;
        private String label = "changethis";

        public Builder(ParallelServiceExecutor executor) {
            this.executor = executor;
        }

        public Builder<A> setParCallable(ParCallable<Either<GeneralError, A>> parCallable) {
            checkState(this.parCallable.isEmpty(), "Cannot set the parCallable twice!");
            this.parCallable = Option.apply(parCallable);
            return this;
        }

        public Builder<A> with(Function1<Unit, Either<GeneralError, A>> fn1) {
            checkState(this.parCallable.isEmpty(), "Cannot set the parCallable twice!");
            this.parCallable = Option.apply(ParCallable.fromScala(fn1));
            return this;
        }


        public Builder<A> setLabel(String label) {
            this.label = label;
            return this;
        }

        public ParRequest<A> build() {
            checkState(!built, "Cannot build a request twice!");
            checkState(parCallable.isDefined(), "ParCallable not defined!");
            try {
                ParCallable<Either<GeneralError, A>> first = ParCallable.from(parCallable.get());
                return new ParRequest<>(label, executor, first);
            } finally {
                built = true;
            }
        }
    }

    public static <T> ParRequest<T> immediateResult(String label, ParallelServiceExecutor executor,
                                                    Either<GeneralError, T> t) {
        checkNotNull(t, "result is null");
        checkNotNull(executor, "Executor is null");
        return new ParRequest<T>(label, executor, Futures.immediateFuture(t));
    }
}
