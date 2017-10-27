// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

import com.google.common.base.Preconditions;

import java.util.concurrent.Callable;
import java.util.function.Function;

import scala.*;

/**
 * Created by hiral on 5/21/15.
 */
abstract public class Either<E, A> {

    private static final Either<?, Nothing> RIGHT_NOTHING = new Right<>(Nothing.get());

    abstract public boolean isRight();

    abstract public boolean isLeft();

    @Override
    public int hashCode() {
        if (isRight()) {
            return extractRight(this).hashCode();
        } else {
            return extractLeft(this).hashCode();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Either) {
            Either<?, ?> other = (Either<?, ?>) obj;
            if (isRight() && other.isRight()) {
                return extractRight(this).equals(extractRight(other));
            }
            if (isLeft() && other.isLeft()) {
                return extractLeft(this).equals(extractLeft(other));
            }
        }
        return false;
    }

    /**
     * Unsafe cast, do not use
     * @return
     */
    @Deprecated
    public Right<E, A> right() {
        return (Right<E, A>) this;
    }

    /**
     * Unsafe cast, do not use
     * @return
     */
    @Deprecated
    public Left<E, A> left() {
        return (Left<E, A>) this;
    }

    /**
     * Converts an either to an Option<B> if the Either isRight(), else returns Option.none()
     */
    final public Option<A> toOption() {
        if (isRight()) {
            return Option.apply(((Right<E, A>) this).a);
        } else {
            return Option.none();
        }
    }

    /**
     * Converts an either to an Option<B> if the Either isRight(), else returns Option.none() after applying the
     * leftSideEffect function on A
     */
    public Option<A> toOption(SideEffectFunction<E> leftSideEffect) {
        if (isRight()) {
            return Option.apply(((Right<E, A>) this).a);
        } else {
            leftSideEffect.apply(extractLeft(this));
            return Option.none();
        }
    }

    /**
     * Converts an either to an Option<B> if the Either isRight(), else returns Option.none() after applying the
     * leftSideEffect function on A
     */
    public Option<A> toOption(SideEffectFunction1<E> leftSideEffect) {
        if (isRight()) {
            return Option.apply(((Right<E, A>) this).a);
        } else {
            leftSideEffect.apply(extractLeft(this));
            return Option.none();
        }
    }

    /**
     * Maps either by applying function doWithRight if the Either isRight(), otherwise, it passes the Either through
     * with cast to <E,U>
     */
    public <U> Either<E, U> map(Function<A, U> doWithRight) {
        if (isRight()) {
            return new Right<>(doWithRight.apply(extractRight(this)));
        } else {
            return ((Either<E, U>) this);
        }
    }

    /**
     * Maps either by applying function doWithRight if the Either isRight(), otherwise, it passes the Either through
     * with cast to <E,U>
     */
    public <U> Either<E, U> map(Function1<A, U> doWithRight) {
        if (isRight()) {
            return new Right<>(doWithRight.apply(extractRight(this)));
        } else {
            return ((Either<E, U>) this);
        }
    }

    /**
     *
     * @param doWithLeft
     * @param <E2>
     * @return
     */
    public <E2> Either<E2, A> mapLeft(Function<E, E2> doWithLeft) {
        if (isLeft()) {
            return new Left<>(doWithLeft.apply(extractLeft(this)));
        } else {
            return ((Either<E2, A>) this);
        }
    }

    /**
     *
     * @param doWithLeft
     * @param <E2>
     * @return
     */
    public <E2> Either<E2, A> mapLeft(Function1<E, E2> doWithLeft) {
        if (isLeft()) {
            return new Left<>(doWithLeft.apply(extractLeft(this)));
        } else {
            return ((Either<E2, A>) this);
        }
    }

    /**
     * Maps either by applying function doWithRight if the Either isRight(), otherwise, it applies the leftSideEffect
     * function and then returns the input either with cast to <A,U>
     */
    public <U> Either<E, U> map(Function<A, U> doWithRight,
                                SideEffectFunction<E> leftSideEffect) {
        if (isRight()) {
            return new Right<>(doWithRight.apply(extractRight(this)));
        } else {
            E e = extractLeft(this);
            leftSideEffect.apply(e);
            return ((Either<E, U>) this);
        }
    }

    /**
     * Maps either by applying function doWithRight if the Either isRight(), otherwise, it applies the leftSideEffect
     * function and then returns the input either with cast to <A,U>
     */
    public <U> Either<E, U> map(Function1<A, U> doWithRight,
                                SideEffectFunction1<E> leftSideEffect) {
        if (isRight()) {
            return new Right<>(doWithRight.apply(extractRight(this)));
        } else {
            E e = extractLeft(this);
            leftSideEffect.apply(e);
            return ((Either<E, U>) this);
        }
    }

    /**
     * Converts Either<E,A> to T by applying the right function or left function depending on if it isRight or isLeft
     */
    public <T> T fold(Function<E, T> doWithLeft, Function<A, T> doWithRight) {
        if (isLeft()) {
            return doWithLeft.apply(extractLeft(this));
        } else {
            return doWithRight.apply(extractRight(this));
        }
    }

    /**
     * Converts Either<E,A> to T by applying the right function or left function depending on if it isRight or isLeft
     */
    public <T> T fold(Function1<E, T> doWithLeft, Function1<A, T> doWithRight) {
        if (isLeft()) {
            return doWithLeft.apply(extractLeft(this));
        } else {
            return doWithRight.apply(extractRight(this));
        }
    }

    /**
     * Maps either by applying function doWithRight if the Either isRight(), otherwise returns the input either with
     * cast to <A,U>
     */
    public <U> Either<E, U> flatMap(Function<A, Either<E, U>> doWithRight) {
        if (isRight()) {
            return doWithRight.apply(extractRight(this));
        } else {
            return ((Either<E, U>) this);
        }
    }

    /**
     * Maps either by applying function doWithRight if the Either isRight(), otherwise returns the input either with
     * cast to <A,U>
     */
    public <U> Either<E, U> flatMap(Function1<A, Either<E, U>> doWithRight) {
        if (isRight()) {
            return doWithRight.apply(extractRight(this));
        } else {
            return ((Either<E, U>) this);
        }
    }

    /**
     * If either isRight(), apply the exists function on it, else return false
     */
    public boolean rightExists(Function<A, java.lang.Boolean> exists) {
        if (isRight()) {
            return exists.apply(extractRight(this));
        }
        return false;
    }
    /**
     * If either isRight(), apply the exists function on it, else return false
     */
    public boolean rightExists(Function1<A, java.lang.Boolean> exists) {
        if (isRight()) {
            return exists.apply(extractRight(this));
        }
        return false;
    }

    /**
     * If either isLeft(), apply the exists function on it, else return false
     */
    public boolean leftExists(Function<E, java.lang.Boolean> exists) {
        if (isLeft()) {
            return exists.apply(extractLeft(this));
        }
        return false;
    }

    /**
     * If either isLeft(), apply the exists function on it, else return false
     */
    public boolean leftExists(Function1<E, java.lang.Boolean> exists) {
        if (isLeft()) {
            return exists.apply(extractLeft(this));
        }
        return false;
    }

    /**
     * If either isRight(), apply the filter function on it, else return Option.none()
     */
    public Option<Either<E, A>> rightFilter(Function<A, java.lang.Boolean> filter) {
        if (isRight()) {
            A right = extractRight(this);
            if (filter.apply(right)) {
                return Option.apply(this);
            }
        }
        return Option.none();
    }

    /**
     * If either isRight(), apply the filter function on it, else return Option.none()
     */
    public Option<Either<E, A>> rightFilter(Function1<A, java.lang.Boolean> filter) {
        if (isRight()) {
            A right = extractRight(this);
            if (filter.apply(right)) {
                return Option.apply(this);
            }
        }
        return Option.none();
    }

    /**
     * If either isLeft(), apply the filter function on it, else return Option.none()
     */
    public Option<Either<E, A>> leftFilter(Function<E, java.lang.Boolean> filter) {
        if (isLeft()) {
            E left = extractLeft(this);
            if (filter.apply(left)) {
                return Option.apply(this);
            }
        }
        return Option.none();
    }

    /**
     * If either isLeft(), apply the filter function on it, else return Option.none()
     */
    public Option<Either<E, A>> leftFilter(Function1<E, java.lang.Boolean> filter) {
        if (isLeft()) {
            E left = extractLeft(this);
            if (filter.apply(left)) {
                return Option.apply(this);
            }
        }
        return Option.none();
    }

    /**
     * Casts the either as a Right, assumes the caller has verified the Either isRight()
     */
    public static <E, A> Right<E, A> asRight(Either<E, A> either) {
        return (Right<E, A>) either;
    }

    /**
     * Casts the either as a Left, assumes the caller has verified the Either isLeft()
     */
    public static <E, A> Left<E, A> asLeft(Either<E, A> either) {
        return (Left<E, A>) either;
    }

    /**
     * Extracts right and returns it, assumes the caller has verified the Either isRight()
     */
    public static <E, A> A extractRight(Either<E, A> either) {
        return ((Right<E, A>) either).a;
    }

    /**
     * Extracts left and returns it, assumes the caller has verified the Either isLeft()
     */
    public static <E, A> E extractLeft(Either<E, A> either) {
        return ((Left<E, A>) either).e;
    }

    /**
     * Casts the leftEither from <E,A> to <E,B>, meaning it is of type E already and not A or B
     */
    public static <E, A, B> Either<E, B> castLeft(Either<E, A> leftEither) {
        Preconditions.checkArgument(leftEither.isLeft());
        return (Either<E, B>) leftEither;
    }

    /**
     * Casts the rightEither from <E1,A> to <E2, A>, meaning it is of type A already and not E1 or E2
     */
    public static <E1, E2, A> Either<E2, A> castRight(Either<E1, A> rightEither) {
        Preconditions.checkArgument(rightEither.isRight());
        return (Either<E2, A>) rightEither;
    }

    /**
     * Provides a right which is of type Nothing
     */
    public static <E> Either<E, Nothing> rightNothing() {
        return (Either<E, Nothing>) RIGHT_NOTHING;
    }

    /**
     * Given a function fn of type Callable, this function will return an Either with a Right of A and Left of Throwable
     * if fn.call() generated an exception
     */
    public static <A> Either<Throwable, A> tryCatch(Callable<A> fn) {
        try {
            return new Right<>(fn.call());
        } catch (Throwable t) {
            return new Left<>(t);
        }
    }

    /**
     * Given 2 eithers, returns the result of calling fn with the right values of the 2 eithers as Tuple2<A,B>,
     * otherwise returns the first left found
     */
    public static <E, A, B, O> Either<E, O> for2yield(Either<E, A> a, Either<E, B> b,
                                                      Function<Tuple2<A, B>, Either<E, O>> fn) {
        if (a.isLeft()) {
            return castLeft(a);
        }
        if (b.isLeft()) {
            return castLeft(b);
        }
        A valA = extractRight(a);
        B valB = extractRight(b);
        return fn.apply(new Tuple2<>(valA, valB));
    }

    /**
     * Given 3 eithers, returns the result of calling fn with the right values of the 3 eithers as Tuple3<A,B,C>,
     * otherwise returns the first left found
     */
    public static <E, A, B, C, O> Either<E, O> for3yield(Either<E, A> a, Either<E, B> b, Either<E, C> c,
                                                         Function<Tuple3<A, B, C>, Either<E, O>> fn) {
        if (a.isLeft()) {
            return castLeft(a);
        }
        if (b.isLeft()) {
            return castLeft(b);
        }
        if (c.isLeft()) {
            return castLeft(c);
        }
        A valA = extractRight(a);
        B valB = extractRight(b);
        C valC = extractRight(c);
        return fn.apply(new Tuple3<A, B, C>(valA, valB, valC));
    }

    /**
     * Given 4 eithers, returns the result of calling fn with the right values of the 4 eithers as Tuple4<A,B,C,D>,
     * otherwise returns the first left found
     */
    public static <E, A, B, C, D, O> Either<E, O> for4yield(Either<E, A> a, Either<E, B> b, Either<E, C> c,
                                                            Either<E, D> d,
                                                            Function<Tuple4<A, B, C, D>, Either<E, O>> fn) {
        if (a.isLeft()) {
            return castLeft(a);
        }
        if (b.isLeft()) {
            return castLeft(b);
        }
        if (c.isLeft()) {
            return castLeft(c);
        }
        if (d.isLeft()) {
            return castLeft(d);
        }
        A valA = extractRight(a);
        B valB = extractRight(b);
        C valC = extractRight(c);
        D valD = extractRight(d);
        return fn.apply(new Tuple4<A, B, C, D>(valA, valB, valC, valD));
    }

    /**
     * Given 5 eithers, returns the result of calling fn with the right values of the 5 eithers as Tuple5<A,B,C,D,F>,
     * otherwise returns the first left found
     */
    public static <E, A, B, C, D, F, O> Either<E, O> for5yield(Either<E, A> a, Either<E, B> b, Either<E, C> c,
                                                               Either<E, D> d,
                                                               Either<E, F> f,
                                                               Function<Tuple5<A, B, C, D, F>, Either<E, O>> fn) {
        if (a.isLeft()) {
            return castLeft(a);
        }
        if (b.isLeft()) {
            return castLeft(b);
        }
        if (c.isLeft()) {
            return castLeft(c);
        }
        if (d.isLeft()) {
            return castLeft(d);
        }
        if (f.isLeft()) {
            return castLeft(f);
        }
        A valA = extractRight(a);
        B valB = extractRight(b);
        C valC = extractRight(c);
        D valD = extractRight(d);
        F valF = extractRight(f);
        return fn.apply(new Tuple5<A, B, C, D, F>(valA, valB, valC, valD, valF));
    }
}
