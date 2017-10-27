// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

import java.util.function.Function;

import org.testng.annotations.Test;
import scala.Function1;
import scala.runtime.AbstractFunction1;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by hiral on 5/14/14.
 */
public class TestOption {

    private Option<String> someString = Option.apply("string");
    private Option<String> nullString = Option.apply(null);

    <T,U> Function1<T, U> fn1(Function<T, U> fn) {
        return new AbstractFunction1<T, U>() {
            @Override
            public U apply(T t) {
                return fn.apply(t);
            }
        };
    }

    Function<String, String> mapString = new Function<String, String>() {
        @Override
        public String apply(String input) {
            return input + "-mapped";
        }
    };

    Function<String, Option<String>> flatMapString = new Function<String, Option<String>>() {
        @Override
        public Option<String> apply(String input) {
            return Option.apply(input + "-flat-mapped");
        }
    };

    Function<String, Option<String>> flatMapStringReturnNone = new Function<String, Option<String>>() {
        @Override
        public Option<String> apply(String input) {
            return Option.none();
        }
    };

    Function<String, String> foldString = new Function<String, String>() {
        @Override
        public String apply(String input) {
            return input + "-folded";
        }
    };

    Function<Nothing, String> foldNothing = new Function<Nothing, String>() {
        @Override
        public String apply(Nothing input) {
            return "folded";
        }
    };

    Function<Nothing, String> noneToLeftEither = new Function<Nothing, String>() {
        @Override
        public String apply(Nothing input) {
            return "left";
        }
    };

    Function<String, Boolean> filterPositive = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
            return true;
        }
    };

    Function<String, Boolean> filterNegative = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
            return false;
        }
    };

    @Test
    public void testNonEmpty() {
        assertTrue(someString.nonEmpty());
        assertFalse(nullString.nonEmpty());
    }

    @Test
    public void testGetOrElse() {
        assertEquals(someString.getOrElse("test"),"string");
        assertEquals(nullString.getOrElse("test"),"test");
    }

    @Test
    public void testFilterWithSome() {
        Option<String> resultPositive = someString.filter(filterPositive);
        assertTrue(resultPositive.isDefined());
        Option<String> resultNegative = someString.filter(filterNegative);
        assertTrue(resultNegative.isEmpty());
    }

    @Test
    public void testFilterWithSomeFn1() {
        Option<String> resultPositive = someString.filter(fn1(filterPositive));
        assertTrue(resultPositive.isDefined());
        Option<String> resultNegative = someString.filter(fn1(filterNegative));
        assertTrue(resultNegative.isEmpty());
    }

    @Test
    public void testFilterWithNone() {
        Option<String> resultPositive = nullString.filter(filterPositive);
        assertTrue(resultPositive.isEmpty());
        Option<String> resultNegative = nullString.filter(filterNegative);
        assertTrue(resultNegative.isEmpty());
    }

    @Test
    public void testFilterWithNoneFn1() {
        Option<String> resultPositive = nullString.filter(fn1(filterPositive));
        assertTrue(resultPositive.isEmpty());
        Option<String> resultNegative = nullString.filter(fn1(filterNegative));
        assertTrue(resultNegative.isEmpty());
    }

    @Test
    public void testFilterNotWithSome() {
        Option<String> resultPositive = someString.filterNot(filterPositive);
        assertTrue(resultPositive.isEmpty());
        Option<String> resultNegative = someString.filterNot(filterNegative);
        assertTrue(resultNegative.isDefined());
    }

    @Test
    public void testFilterNotWithSomeFn1() {
        Option<String> resultPositive = someString.filterNot(fn1(filterPositive));
        assertTrue(resultPositive.isEmpty());
        Option<String> resultNegative = someString.filterNot(fn1(filterNegative));
        assertTrue(resultNegative.isDefined());
    }

    @Test
    public void testFilterNotWithNone() {
        Option<String> resultPositive = nullString.filterNot(filterPositive);
        assertTrue(resultPositive.isEmpty());
        Option<String> resultNegative = nullString.filterNot(filterNegative);
        assertTrue(resultNegative.isEmpty());
    }

    @Test
    public void testFilterNotWithNoneFn1() {
        Option<String> resultPositive = nullString.filterNot(fn1(filterPositive));
        assertTrue(resultPositive.isEmpty());
        Option<String> resultNegative = nullString.filterNot(fn1(filterNegative));
        assertTrue(resultNegative.isEmpty());
    }

    @Test
    public void testMapWithSome() {
        Option mapped = someString.map(mapString);
        assertTrue(mapped.isDefined());
        assertTrue(mapped.get().equals("string-mapped"));
    }

    @Test
    public void testMapWithSomeFn1() {
        Option mapped = someString.map(fn1(mapString));
        assertTrue(mapped.isDefined());
        assertTrue(mapped.get().equals("string-mapped"));
    }

    @Test
    public void testMapWithNull() {
        Option mapped = nullString.map(mapString);
        assertTrue(mapped.isEmpty());
    }

    @Test
    public void testMapWithNullFn1() {
        Option mapped = nullString.map(fn1(mapString));
        assertTrue(mapped.isEmpty());
    }

    @Test
    public void testFlatMapWithSome() {
        Option mapped = someString.flatMap(flatMapString);
        assertTrue(mapped.isDefined());
        assertTrue(mapped.get().equals("string-flat-mapped"));
    }

    @Test
    public void testFlatMapWithSomeFn1() {
        Option mapped = someString.flatMap(fn1(flatMapString));
        assertTrue(mapped.isDefined());
        assertTrue(mapped.get().equals("string-flat-mapped"));
    }

    @Test
    public void testFlatMapWithSomeButReturnNone() {
        Option mapped = someString.flatMap(flatMapStringReturnNone);
        assertTrue(mapped.isEmpty());
    }

    @Test
    public void testFlatMapWithSomeButReturnNoneFn1() {
        Option mapped = someString.flatMap(fn1(flatMapStringReturnNone));
        assertTrue(mapped.isEmpty());
    }

    @Test
    public void testFlatMapWithNull() {
        Option mapped = nullString.map(flatMapString);
        assertTrue(mapped.isEmpty());
    }

    @Test
    public void testFlatMapWithNullFn1() {
        Option mapped = nullString.map(fn1(flatMapString));
        assertTrue(mapped.isEmpty());
    }

    @Test
    public void testFoldWithSome() {
        String folded = someString.fold(foldString, foldNothing);
        assertTrue(folded.equals("string-folded"));
    }

    @Test
    public void testFoldWithSomeFn1() {
        String folded = someString.fold(fn1(foldString), fn1(foldNothing));
        assertTrue(folded.equals("string-folded"));
    }

    @Test
    public void testFoldWithNull() {
        String folded = nullString.fold(foldString, foldNothing);
        assertTrue(folded.equals("folded"));
    }

    @Test
    public void testFoldWithNullFn1() {
        String folded = nullString.fold(fn1(foldString), fn1(foldNothing));
        assertTrue(folded.equals("folded"));
    }

    @Test
    public void testToEitherWithSome() {
        Either<String, String> either = Option.toEither(someString, foldNothing);
        assertTrue(either.isRight());
        assertTrue(either.right().get().equals("string"));
    }

    @Test
    public void testToEitherWithSomeFn1() {
        Either<String, String> either = Option.toEither(someString, fn1(foldNothing));
        assertTrue(either.isRight());
        assertTrue(either.right().get().equals("string"));
    }

    @Test
    public void testToEitherWithNull() {
        Either<String, String> either = Option.toEither(nullString, noneToLeftEither);
        assertTrue(either.isLeft());
        assertTrue(either.left().get().equals("left"));
    }

    @Test
    public void testToEitherWithNullFn1() {
        Either<String, String> either = Option.toEither(nullString, fn1(noneToLeftEither));
        assertTrue(either.isLeft());
        assertTrue(either.left().get().equals("left"));
    }

    @Test
    public void testApplyWithSome() {
        Option<String> stringOption = Option.apply("string");
        assertTrue(stringOption.isDefined());
        assertTrue(stringOption.get().equals("string"));
    }

    @Test
    public void testApplyWithNull() {
        Option<String> stringOption = Option.apply(null);
        assertTrue(stringOption.isEmpty());
    }
}
