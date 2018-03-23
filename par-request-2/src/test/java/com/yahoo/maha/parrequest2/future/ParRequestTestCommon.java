// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;


import scala.util.Right;
import com.yahoo.maha.parrequest2.GeneralError;
import com.yahoo.maha.parrequest2.ParCallable;
import scala.util.Either;

/**
 * Created by hiral on 6/25/14.
 */
public class ParRequestTestCommon {

    //all strings
    public static final ParCallable<Either<GeneralError, String>>
            stringSleep1000 =
            ParCallable.from(() -> {
                Thread.sleep(1000);
                return new Right<GeneralError, String>("100");
            });

    public static final ParCallable<Either<GeneralError, String>>
            stringSleep1000Failure =
            ParCallable.from(() -> {
                Thread.sleep(1000);
                return GeneralError.either("stage", "failed");
            });

    public static final ParCallable<Either<GeneralError, String>>
            stringSleep1000Exception =
            ParCallable.from(() -> {
                Thread.sleep(1000);
                throw new IllegalArgumentException("blah1");
            });

    //all longs
    public static final ParCallable<Either<GeneralError, Long>>
            longSleep500 =
            ParCallable.from(() -> {
                Thread.sleep(500);
                return new Right<GeneralError, Long>(100L);
            });

    public static final ParCallable<Either<GeneralError, Long>>
            longSleep500Failure =
            ParCallable.from(() -> {
                Thread.sleep(500);
                return GeneralError.either("stage", "failed");
            });

    public static final ParCallable<Either<GeneralError, Long>>
            longSleep500Exception =
            ParCallable.from(() -> {
                Thread.sleep(500);
                throw new IllegalArgumentException("blah1");
            });

    public static final ParCallable<Either<GeneralError, Long>>
            longSleep10 =
            ParCallable.from(() -> {
                Thread.sleep(10);
                return new Right<GeneralError, Long>(100L);
            });

    public static final ParCallable<Either<GeneralError, Long>>
            longSleep10Failure =
            ParCallable.from(() -> {
                Thread.sleep(10);
                return GeneralError.either("stage", "failed");
            });

    public static final ParCallable<Either<GeneralError, Long>>
            longSleep10Exception =
            ParCallable.from(() -> {
                Thread.sleep(10);
                throw new IllegalArgumentException("blah1");
            });

    //all integers
    public static final ParCallable<Either<GeneralError, Integer>>
            integerSleep200 =
            ParCallable.from(() -> {
                Thread.sleep(200);
                return new Right<GeneralError, Integer>(10);
            });
    public static final ParCallable<Either<GeneralError, Integer>>
            integerSleep200Failure =
            ParCallable.from(() -> {
                Thread.sleep(200);
                return GeneralError.either("stage", "failed");
            });
    public static final ParCallable<Either<GeneralError, Integer>>
            integerSleep200Exception =
            ParCallable.from(() -> {
                Thread.sleep(200);
                throw new IllegalArgumentException("blah1");
            });
}
