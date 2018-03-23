// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

import scala.Function1;

import java.util.function.Function;

abstract public class Option<T> {

    private static final Option<?> NONE = new None<>();

    abstract public T get();

    abstract public boolean isDefined();

    abstract public boolean isEmpty();

    public boolean nonEmpty() {
        return !isEmpty();
    }

    public T getOrElse(T defaultValue) {
        if (isDefined()) {
            return get();
        } else {
            return defaultValue;
        }
    }

    /**
     * If option is defined, applies doWithOption function and return option if result is true, else returns
     * Option.none()
     */
    final public Option<T> filter(Function<T, Boolean> doWithOption) {
        if (isDefined() && Boolean.TRUE.equals(doWithOption.apply(get()))) {
            return this;
        } else {
            return none();
        }
    }

    /**
     * If option is defined, applies doWithOption function and return option if result is true, else returns
     * Option.none()
     */
    final public Option<T> filter(Function1<T, Boolean> doWithOption) {
        if (isDefined() && Boolean.TRUE.equals(doWithOption.apply(get()))) {
            return this;
        } else {
            return none();
        }
    }

    /**
     * If option is defined, applies doWithOption function and return option if result is false, else returns
     * Option.none()
     */
    final public Option<T> filterNot(Function<T, Boolean> doWithOption) {
        if (isDefined() && Boolean.FALSE.equals(doWithOption.apply(get()))) {
            return this;
        } else {
            return none();
        }
    }

    /**
     * If option is defined, applies doWithOption function and return option if result is false, else returns
     * Option.none()
     */
    final public Option<T> filterNot(Function1<T, Boolean> doWithOption) {
        if (isDefined() && Boolean.FALSE.equals(doWithOption.apply(get()))) {
            return this;
        } else {
            return none();
        }
    }

    /**
     * If option is defined, applies doWithOption function, else returns Option.none()
     */
    final public <U> Option<U> map(Function<T, U> doWithOption) {
        if (isDefined()) {
            T t = get();
            return Option.apply(doWithOption.apply(t));
        } else {
            return none();
        }
    }

    /**
     * If option is defined, applies doWithOption function, else returns Option.none()
     */
    final public <U> Option<U> map(Function1<T, U> doWithOption) {
        if (isDefined()) {
            T t = get();
            return Option.apply(doWithOption.apply(t));
        } else {
            return none();
        }
    }

    /**
     * If option is defined, applies doWithOption function, else returns Option.none()
     */
    final public <U> Option<U> flatMap(Function<T, Option<U>> doWithOption) {
        if (isDefined()) {
            return doWithOption.apply(get());
        } else {
            return none();
        }
    }

    /**
     * If option is defined, applies doWithOption function, else returns Option.none()
     */
    final public <U> Option<U> flatMap(Function1<T, Option<U>> doWithOption) {
        if (isDefined()) {
            return doWithOption.apply(get());
        } else {
            return none();
        }
    }

    /**
     * If option is defined, applies doWithOption function, else applies doWithNothing function
     */
    final public <U> U fold(Function<T, U> doWithOption,
                            Function<Nothing, U> doWithNothing) {
        if (isDefined()) {
            return doWithOption.apply(get());
        } else {
            return doWithNothing.apply(Nothing.get());
        }
    }

    /**
     * If option is defined, applies doWithOption function, else applies doWithNothing function
     */
    final public <U> U fold(Function1<T, U> doWithOption,
                            Function1<Nothing, U> doWithNothing) {
        if (isDefined()) {
            return doWithOption.apply(get());
        } else {
            return doWithNothing.apply(Nothing.get());
        }
    }

    @Override
    public int hashCode() {
        if (isDefined()) {
            return get().hashCode();
        } else {
            return super.hashCode();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Option) {
            Option<?> other = (Option<?>) obj;
            if (isDefined() && other.isDefined()) {
                return get().equals(other.get());
            }
        }
        return false;
    }

    /**
     * If option is defined, returns Right, else result of applying nothingToError
     */
    public static <E, A> Either<E, A> toEither(Option<? extends A> input, Function<Nothing, E> nothingToError) {
        if (input.isDefined()) {
            return new Right<>((A) input.get());
        } else {
            return new Left<>(nothingToError.apply(Nothing.get()));
        }
    }

    /**
     * If option is defined, returns Right, else result of applying nothingToError
     */
    public static <E, A> Either<E, A> toEither(Option<? extends A> input, Function1<Nothing, E> nothingToError) {
        if (input.isDefined()) {
            return new Right<>((A) input.get());
        } else {
            return new Left<>(nothingToError.apply(Nothing.get()));
        }
    }

    public static <T> Option<T> none() {
        return ((Option<T>) NONE);
    }

    public static <T> Option<T> apply(T t) {
        if (t == null) {
            return none();
        } else {
            return new Some<>(t);
        }

    }
}
