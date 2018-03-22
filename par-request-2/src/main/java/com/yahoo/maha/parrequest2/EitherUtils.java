package com.yahoo.maha.parrequest2;

import com.google.common.base.Preconditions;
import scala.Function1;
import scala.Option;
import scala.util.Either;
import scala.util.Right;

import java.util.function.Function;

public class EitherUtils {


    /**
     * Casts the leftEither from <E,A> to <E,B>, meaning it is of type E already and not A or B
     */
    public static <E, A, B> Either<E, B> castLeft(Either<E, A> leftEither) {
        Preconditions.checkArgument(leftEither.isLeft());
        return (Either<E, B>) leftEither;
    }

    /**
     * Maps either by applying function doWithRight if the Either isRight(), otherwise returns the input either with
     * cast to <A,U>
     */
    public static <E, A, U> Either<E, U> flatMap(Function<A, Either<E, U>> doWithRight, Either<E, A> either) {
        if (either.isRight()) {
            return doWithRight.apply(either.right().get());
        } else {
            return ((Either<E, U>) either);
        }
    }

    /**
     * Converts Either<E,A> to T by applying the right function or left function depending on if it isRight or isLeft
     */
    public static <T, E, A> T fold(Function<E, T> doWithLeft, Function<A, T> doWithRight, Either<E,A> either) {
        if (either.isLeft()) {
            return doWithLeft.apply(either.left().get());
        } else {
            return doWithRight.apply(either.right().get());
        }
    }

    /**
     * Converts Either<E,A> to T by applying the right function or left function depending on if it isRight or isLeft
     */
    public static <T, E, A> T fold(Function1<E, T> doWithLeft, Function<A, T> doWithRight, Either<E,A> either) {
        if (either.isLeft()) {
            return doWithLeft.apply(either.left().get());
        } else {
            return doWithRight.apply(either.right().get());
        }
    }

    /**
     * Maps either by applying function doWithRight if the Either isRight(), otherwise, it passes the Either through
     * with cast to <E,U>
     */
    public static <U, E, A> Either<E, U> map(Function<A, U> doWithRight, Either<E,A> either) {
        if (either.isRight()) {
            return new Right<>(doWithRight.apply(either.right().get()));
        } else {
            return ((Either<E, U>) either);
        }
    }

    /**
     * If option is defined, applies doWithOption function, else returns Option.none()
     */
    final public static <U, T> Option<U> map(Function<T, U> doWithOption, Option<T> option) {
        if (option.isDefined()) {
            T t = option.get();
            return Option.apply(doWithOption.apply(t));
        } else {
            return Option.empty();
        }
    }
}
