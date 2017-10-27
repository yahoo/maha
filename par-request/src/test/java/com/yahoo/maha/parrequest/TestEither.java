// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

import java.util.function.Function;
import com.google.common.base.Joiner;

import org.testng.annotations.Test;

import java.util.concurrent.Callable;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.Function1;
import scala.runtime.AbstractFunction1;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by hiral on 3/31/14.
 */
public class TestEither {


    <T,U> Function1<T, U> fn1(Function<T, U> fn) {
        return new AbstractFunction1<T, U>() {
            @Override
            public U apply(T t) {
                return fn.apply(t);
            }
        };
    }

    Either<Integer, String> left1 = new Left<>(400);
    Either<Integer, String> left1_negative = new Left<>(404);
    Either<Integer, String> left2 = new Left<>(402);
    Either<Integer, String> left3 = new Left<>(403);
    Either<Integer, String> left4 = new Left<>(404);
    Either<Integer, String> left5 = new Left<>(405);

    Either<Integer, String> right1 = new Right<>("right1");
    Either<Integer, String> right1_negative = new Right<>("right1_negative");
    Either<Integer, String> right2 = new Right<>("right2");
    Either<Integer, String> right3 = new Right<>("right3");
    Either<Integer, String> right4 = new Right<>("right4");
    Either<Integer, String> right5 = new Right<>("right5");

    Function<Integer, Integer> mapLeftF1 = new Function<Integer, Integer>() {
        @Override
        public Integer apply(Integer input) {
            return input + 1;
        }
    };

    Function<String, String> mapF1 = new Function<String, String>() {
        @Override
        public String apply(String input) {
            return input + "-f1";
        }
    };

    Function<String, Either<Integer, String>> flatMapF1 = new Function<String, Either<Integer, String>>() {
        @Override
        public Either<Integer, String> apply(String input) {
            return new Right<>(input + "-flat-f1");
        }
    };

    Joiner dashJoiner = Joiner.on("-");

    Function<Tuple2<String, String>, Either<Integer, String>>
        for2yield =
        new Function<Tuple2<String, String>, Either<Integer, String>>() {
            @Override
            public Either<Integer, String> apply(Tuple2<String, String> input) {
                return new Right<>(dashJoiner.join(input._1(), input._2()));
            }
        };

    Function<Tuple3<String, String, String>, Either<Integer, String>>
        for3yield =
        new Function<Tuple3<String, String, String>, Either<Integer, String>>() {
            @Override
            public Either<Integer, String> apply(Tuple3<String, String, String> input) {
                return new Right<>(dashJoiner.join(input._1(), input._2(), input._3()));
            }
        };

    Function<Tuple4<String, String, String, String>, Either<Integer, String>>
        for4yield =
        new Function<Tuple4<String, String, String, String>, Either<Integer, String>>() {
            @Override
            public Either<Integer, String> apply(Tuple4<String, String, String, String> input) {
                return new Right<>(dashJoiner.join(input._1(), input._2(), input._3(), input._4()));
            }
        };

    Function<Tuple5<String, String, String, String, String>, Either<Integer, String>>
        for5yield =
        new Function<Tuple5<String, String, String, String, String>, Either<Integer, String>>() {
            @Override
            public Either<Integer, String> apply(Tuple5<String, String, String, String, String> input) {
                return new Right<>(dashJoiner.join(input._1(), input._2(), input._3(), input._4(), input._5()));
            }
        };

    int count = 0;
    int count2 = 0;

    SideEffectFunction<Integer> sideEffect = new SideEffectFunction<Integer>() {
        @Override
        void doSideEffect(Integer error) {
            count++;
        }
    };

    SideEffectFunction1<Integer> sideEffectFn1 = new SideEffectFunction1<Integer>() {
        @Override
        void doSideEffect(Integer error) {
            count++;
        }
    };

    SideEffectFunction<Integer> sideEffect2 = new SideEffectFunction<Integer>() {
        @Override
        void doSideEffect(Integer error) {
            count2++;
        }
    };

    SideEffectFunction1<Integer> sideEffect2Fn1 = new SideEffectFunction1<Integer>() {
        @Override
        void doSideEffect(Integer error) {
            count2++;
        }
    };

    Function<Integer, Either<Integer, String>> doWithLeft = new Function<Integer, Either<Integer, String>>() {
        @Override
        public Either<Integer, String> apply(Integer input) {
            return new Left<>(input + 1);
        }
    };

    Function<String, Either<Integer, String>> doWithRight = new Function<String, Either<Integer, String>>() {
        @Override
        public Either<Integer, String> apply(String input) {
            return new Right<>(input + "-fold");
        }
    };

    Function<Integer, Boolean> leftExists = new Function<Integer, Boolean>() {
        @Override
        public Boolean apply(Integer input) {
            return input == 400;
        }
    };

    Function<String, Boolean> rightExists = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String input) {
            return input.equals("right1");
        }
    };

    Function<Integer, Boolean> leftFilter = new Function<Integer, Boolean>() {
        @Override
        public Boolean apply(Integer input) {
            return input == 400;
        }
    };

    Function<String, Boolean> rightFilter = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String input) {
            return input.equals("right1");
        }
    };

    @Test
    public void testAsRight() {
        assertTrue(Either.asRight(right1) == ((Right<Integer, String>) right1));
    }

    @Test
    public void testAsLeft() {
        assertTrue(Either.asLeft(left1) == ((Left<Integer, String>) left1));
    }

    @Test
    public void testExtractRight() {
        assertTrue(Either.extractRight(right1).equals(right1.right().get()));
    }

    @Test
    public void testExtractLeft() {
        assertTrue(Either.extractLeft(left1).equals(left1.left().get()));
    }

    @Test
    public void testMapLeftWithLeft() {
        Either<Integer, String> result = left1.mapLeft(mapLeftF1);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 401);
    }

    @Test
    public void testMapLeftWithLeftFn1() {
        Either<Integer, String> result = left1.mapLeft(fn1(mapLeftF1));
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 401);
    }

    @Test
    public void testMapLeftWithRight() {
        Either<Integer, String> result = right1.mapLeft(mapLeftF1);
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1"));
    }

    @Test
    public void testMapLeftWithRightFn1() {
        Either<Integer, String> result = right1.mapLeft(fn1(mapLeftF1));
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1"));
    }

    @Test
    public void testMapWithLeft() {
        Either<Integer, String> result = left1.map(mapF1);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 400);
    }


    @Test
    public void testMapWithLeftFn1() {
        Either<Integer, String> result = left1.map(fn1(mapF1));
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 400);
    }

    @Test
    public void testMapWithLeftSideEffect() {
        Either<Integer, String> result = left1.map(mapF1, sideEffect);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 400);
        assertTrue(count == 1);
    }

    @Test
    public void testMapWithLeftSideEffectFn1() {
        Either<Integer, String> result = left1.map(fn1(mapF1), sideEffectFn1);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 400);
        assertTrue(count == 2);
    }

    @Test
    public void testMapWithRight() {
        Either<Integer, String> result = right1.map(mapF1);
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-f1"));

        result = right1.map(mapF1, sideEffect);
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-f1"));
    }

    @Test
    public void testMapWithRightFn1() {
        Either<Integer, String> result = right1.map(fn1(mapF1));
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-f1"));

        result = right1.map(fn1(mapF1), sideEffectFn1);
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-f1"));
    }

    @Test
    public void testFlatMapWithLeft() {
        Either<Integer, String> result = left1.flatMap(flatMapF1);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 400);
    }

    @Test
    public void testFlatMapWithLeftFn1() {
        Either<Integer, String> result = left1.flatMap(fn1(flatMapF1));
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 400);
    }

    @Test
    public void testFlatMapWithRight() {
        Either<Integer, String> result = right1.flatMap(flatMapF1);
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-flat-f1"));
    }

    @Test
    public void testFlatMapWithRightFn1() {
        Either<Integer, String> result = right1.flatMap(fn1(flatMapF1));
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-flat-f1"));
    }

    @Test
    public void testToOptionWithLeft() {
        Option<String> result = left1.toOption();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testToOptionWithRight() {
        Option<String> result = right1.toOption();
        assertTrue(result.isDefined());
        assertTrue(result.get().equals("right1"));
    }

    @Test
    public void testToOptionWithLeftSideEffect() {
        Option<String> result = left1.toOption(sideEffect2);
        assertTrue(result.isEmpty());
        assertTrue(count2 == 1);
    }

    @Test
    public void testToOptionWithLeftSideEffectFn1() {
        Option<String> result = left1.toOption(sideEffect2Fn1);
        assertTrue(result.isEmpty());
        assertTrue(count2 == 2);
    }

    @Test
    public void testToOptionWithRightSideEffect() {
        Option<String> result = right1.toOption(sideEffect);
        assertTrue(result.isDefined());
        assertTrue(result.get().equals("right1"));
    }

    @Test
    public void testToOptionWithRightSideEffectFn1() {
        Option<String> result = right1.toOption(sideEffectFn1);
        assertTrue(result.isDefined());
        assertTrue(result.get().equals("right1"));
    }

    @Test
    public void testFoldWithLeft() {
        Either<Integer, String> result = left1.fold(doWithLeft, doWithRight);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 401);
    }

    @Test
    public void testFoldWithLeftFn1() {
        Either<Integer, String> result = left1.fold(fn1(doWithLeft), fn1(doWithRight));
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 401);
    }

    @Test
    public void testFoldWithRight() {
        Either<Integer, String> result = right1.fold(doWithLeft, doWithRight);
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-fold"));
    }

    @Test
    public void testFoldWithRightFn1() {
        Either<Integer, String> result = right1.fold(fn1(doWithLeft), fn1(doWithRight));
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-fold"));
    }

    @Test
    public void testLeftExistsWithLeft() {
        assertTrue(left1.leftExists(leftExists));
    }

    @Test
    public void testLeftExistsWithLeftFn1() {
        assertTrue(left1.leftExists(fn1(leftExists)));
    }

    @Test
    public void testLeftExistsWithRight() {
        assertFalse(right1.leftExists(leftExists));
    }

    @Test
    public void testLeftExistsWithRightFn1() {
        assertFalse(right1.leftExists(fn1(leftExists)));
    }

    @Test
    public void testRightExistsWithLeft() {
        assertFalse(left1.rightExists(rightExists));
    }

    @Test
    public void testRightExistsWithLeftFn1() {
        assertFalse(left1.rightExists(fn1(rightExists)));
    }

    @Test
    public void testRightExistsWithRight() {
        assertTrue(right1.rightExists(rightExists));
    }

    @Test
    public void testRightExistsWithRightFn1() {
        assertTrue(right1.rightExists(fn1(rightExists)));
    }

    @Test
    public void testLeftFilterWithLeft() {
        Option<Either<Integer, String>> result = left1.leftFilter(leftFilter);
        assertTrue(result.isDefined());
        assertTrue(result.get().isLeft());
        assertTrue(result.get().left().get() == 400);
    }

    @Test
    public void testLeftFilterWithLeftFn1() {
        Option<Either<Integer, String>> result = left1.leftFilter(fn1(leftFilter));
        assertTrue(result.isDefined());
        assertTrue(result.get().isLeft());
        assertTrue(result.get().left().get() == 400);
    }

    @Test
    public void testLeftFilterWithLeftNegative() {
        Option<Either<Integer, String>> result = left1_negative.leftFilter(leftFilter);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testLeftFilterWithLeftNegativeFn1() {
        Option<Either<Integer, String>> result = left1_negative.leftFilter(fn1(leftFilter));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testLeftFilterWithRight() {
        Option<Either<Integer, String>> result = right1.leftFilter(leftFilter);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testLeftFilterWithRightFn1() {
        Option<Either<Integer, String>> result = right1.leftFilter(fn1(leftFilter));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testRightFilterWithLeft() {
        Option<Either<Integer, String>> result = left1.rightFilter(rightFilter);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testRightFilterWithLeftFn1() {
        Option<Either<Integer, String>> result = left1.rightFilter(fn1(rightFilter));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testRightFilterWithRight() {
        Option<Either<Integer, String>> result = right1.rightFilter(rightFilter);
        assertTrue(result.isDefined());
        assertTrue(result.get().isRight());
        assertTrue(result.get().right().get().equals("right1"));
    }

    @Test
    public void testRightFilterWithRightFn1() {
        Option<Either<Integer, String>> result = right1.rightFilter(fn1(rightFilter));
        assertTrue(result.isDefined());
        assertTrue(result.get().isRight());
        assertTrue(result.get().right().get().equals("right1"));
    }

    @Test
    public void testRightFilterWithRightNegative() {
        Option<Either<Integer, String>> result = right1_negative.rightFilter(rightFilter);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testRightFilterWithRightNegativeFn1() {
        Option<Either<Integer, String>> result = right1_negative.rightFilter(fn1(rightFilter));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testCastLeft() {
        Either<String, String> left = new Left<>("left");
        Either<String, Integer> newLeft = Either.castLeft(left);
        assertTrue(newLeft.isLeft());
        assertEquals(newLeft.left().get(), "left");
    }

    @Test
    public void testCastRight() {
        Either<String, String> right = new Right<>("right");
        Either<Integer, String> newRight = Either.castRight(right);
        assertTrue(newRight.isRight());
        assertEquals(newRight.right().get(), "right");
    }

    @Test
    public void testTryCatch() {
        Either<Throwable, String> result = Either.tryCatch(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return "yay";
            }
        });

        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("yay"));
    }

    @Test
    public void testTryCatchWithException() {
        Either<Throwable, String> result = Either.tryCatch(new Callable<String>() {
            @Override
            public String call() throws Exception {
                throw new IllegalArgumentException("no");
            }
        });

        assertTrue(result.isLeft());
        assertTrue((result.left().get()) instanceof IllegalArgumentException);
    }

    @Test
    public void testFor2YieldWithLeft() {
        Either<Integer, String> result = Either.for2yield(right1, left2, for2yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 402);

        result = Either.for2yield(left1, right2, for2yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 400);
    }

    @Test
    public void testFor2YieldWithRight() {
        Either<Integer, String> result = Either.for2yield(right1, right2, for2yield);
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-right2"));
    }

    @Test
    public void testFor3YieldWithLeft() {
        Either<Integer, String> result = Either.for3yield(right1, right2, left3, for3yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 403);

        result = Either.for3yield(right1, left2, right3, for3yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 402);

        result = Either.for3yield(left1, right2, right3, for3yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 400);
    }

    @Test
    public void testFor4YieldWithLeft() {
        Either<Integer, String> result = Either.for4yield(right1, right2, right3, left4, for4yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 404);

        result = Either.for4yield(right1, right2, left3, right4, for4yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 403);

        result = Either.for4yield(right1, left2, right3, right4, for4yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 402);

        result = Either.for4yield(left1, right2, right3, right4, for4yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 400);
    }

    @Test
    public void testFor5YieldWithLeft() {
        Either<Integer, String> result = Either.for5yield(right1, right2, right3, right4, left5, for5yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 405);

        result = Either.for5yield(right1, right2, right3, left4, right5, for5yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 404);

        result = Either.for5yield(right1, right2, left3, right4, right5, for5yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 403);

        result = Either.for5yield(right1, left2, right3, right4, right5, for5yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 402);

        result = Either.for5yield(left1, right2, right3, right4, right5, for5yield);
        assertTrue(result.isLeft());
        assertTrue(result.left().get() == 400);
    }

    @Test
    public void testFor3YieldWithRight() {
        Either<Integer, String> result = Either.for3yield(right1, right2, right3, for3yield);
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-right2-right3"));
    }

    @Test
    public void testFor4YieldWithRight() {
        Either<Integer, String> result = Either.for4yield(right1, right2, right3, right4, for4yield);
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-right2-right3-right4"));
    }

    @Test
    public void testFor5YieldWithRight() {
        Either<Integer, String> result = Either.for5yield(right1, right2, right3, right4, right5, for5yield);
        assertTrue(result.isRight());
        assertTrue(result.right().get().equals("right1-right2-right3-right4-right5"));
    }
}

