// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.yahoo.maha.parrequest2.GeneralError;
import com.yahoo.maha.parrequest2.Nothing;
import com.yahoo.maha.parrequest2.ParCallable;
import com.yahoo.maha.parrequest2.RetryAnalyzerImpl;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import scala.Option;
import scala.Tuple3;
import scala.util.Either;
import scala.util.Right;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestParRequest3Option {
    @BeforeSuite(alwaysRun = true)
    public void beforeSuite(ITestContext context) {
        for (ITestNGMethod method : context.getAllTestMethods()) {
            method.setRetryAnalyzer(new RetryAnalyzerImpl());
        }
    }

    private ParallelServiceExecutor executor;

    private ParFunction<Tuple3<Option<String>, Option<Long>, Option<Integer>>, Integer> stringLongIntAssert =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().get() == 100);
                assertTrue(input._3().get() == 10);
                return 200;
            });

    private ParFunction<Tuple3<Option<String>, Option<Long>, Option<Integer>>, Integer> stringEmptyLongIntAssert =
            ParFunction.from((input) -> {
                assertTrue(input._1().isEmpty());
                assertTrue(input._2().get() == 100);
                assertTrue(input._3().get() == 10);
                return 200;
            });

    private ParFunction<Tuple3<Option<String>, Option<Long>, Option<Integer>>, Integer> stringLongEmptyIntAssert =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().isEmpty());
                assertTrue(input._3().get() == 10);
                return 200;
            });

    private ParFunction<Tuple3<Option<String>, Option<Long>, Option<Integer>>, Integer> stringLongIntEmptyAssert =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().get() == 100);
                assertTrue(input._3().isEmpty());
                return 200;
            });

    private ParFunction<Tuple3<Option<String>, Option<Long>, Option<Integer>>, Either<GeneralError, Integer>>
            stringLongIntAssertMap =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().get() == 100);
                assertTrue(input._3().get() == 10);
                return new Right<>(200);
            });

    private ParFunction<Tuple3<Option<String>, Option<Long>, Option<Integer>>, Either<GeneralError, Integer>>
            stringEmptyLongIntAssertMap =
            ParFunction.from((input) -> {
                assertTrue(input._1().isEmpty());
                assertTrue(input._2().get() == 100);
                assertTrue(input._3().get() == 10);
                return new Right<>(200);
            });

    private ParFunction<Tuple3<Option<String>, Option<Long>, Option<Integer>>, Either<GeneralError, Integer>>
            stringLongEmptyIntAssertMap =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().isEmpty());
                assertTrue(input._3().get() == 10);
                return new Right<>(200);
            });

    private ParFunction<Tuple3<Option<String>, Option<Long>, Option<Integer>>, Either<GeneralError, Integer>>
            stringLongIntEmptyAssertMap =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().get() == 100);
                assertTrue(input._3().isEmpty());
                return new Right<>(200);
            });


    @BeforeClass
    public void setUp() throws Exception {
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(20000);
        executor.setPoolName("test-par-request3");
        executor.setQueueSize(20);
        executor.setThreadPoolSize(10);
        executor.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        executor.destroy();
    }

    @Test
    public void testParRequest3OptionResultMapSuccess() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongIntAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequest3OptionResultMapFailureAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);
        builder.allMustSucceed(true);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().get().equals("100"));
                    assertTrue(input._2().get() == 100);
                    assertTrue(input._3().get() == 10);
                    throw new IllegalArgumentException("failed");
                }));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequest3OptionResultMapFailure() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().get().equals("100"));
                    assertTrue(input._2().get() == 100);
                    assertTrue(input._3().get() == 10);
                    throw new IllegalArgumentException("failed");
                }));
    }

    @Test
    public void testParRequest3OptionResultMapFailureInFirstParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Failure);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);
        builder.allMustSucceed(true);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongIntAssert);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest3OptionResultMapFailureInFirstParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Failure);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringEmptyLongIntAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest3OptionResultMapExceptionInFirstParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Exception);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);
        builder.allMustSucceed(true);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongIntAssert);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest3OptionResultMapExceptionInFirstParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Exception);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringEmptyLongIntAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequest3OptionResultMapFailureInSecondParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10Failure);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);
        builder.allMustSucceed(true);

        Long startTime = System.currentTimeMillis();
        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongIntAssert);
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest3OptionResultMapFailureInSecondParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10Failure);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongEmptyIntAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest3OptionResultMapExceptionInSecondParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10Exception);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);
        builder.allMustSucceed(true);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongIntAssert);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest3OptionResultMapExceptionInSecondParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10Exception);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongEmptyIntAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequest3OptionResultMapFailureInThirdParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200Failure);
        builder.allMustSucceed(true);

        Long startTime = System.currentTimeMillis();
        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongIntAssert);
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest3OptionResultMapFailureInThirdParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200Failure);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongIntEmptyAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest3OptionResultMapExceptionInThirdParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200Exception);
        builder.allMustSucceed(true);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongIntAssert);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest3OptionResultMapExceptionInThirdParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200Exception);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongIntEmptyAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequest3OptionMapSuccess() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapSuccess",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().get().equals("100"));
                            assertTrue(input._2().get() == 100);
                            assertTrue(input._3().get() == 10);
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest3OptionMapFailure() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapFailure",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().get().equals("100"));
                            assertTrue(input._2().get() == 100);
                            assertTrue(input._3().get() == 10);
                            throw new IllegalArgumentException("failed");
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("failed"));
    }

    @Test
    public void testParRequest3OptionMapFailureInFirstParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Failure);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        builder.allMustSucceed(true);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapFailureInFirstParCallableAllMustSucceed", stringLongIntAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest3OptionMapFailureInFirstParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Failure);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapFailureInFirstParCallable", stringEmptyLongIntAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest3OptionMapExceptionInFirstParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Exception);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        builder.allMustSucceed(true);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapExceptionInFirstParCallableAllMustSucceed", stringLongIntAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest3OptionMapExceptionInFirstParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Exception);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapExceptionInFirstParCallable", stringEmptyLongIntAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequest3OptionMapFailureInSecondParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10Failure);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        builder.allMustSucceed(true);

        Long startTime = System.currentTimeMillis();
        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapFailureInSecondParCallableAllMustSucceed", stringLongIntAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest3OptionMapFailureInSecondParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10Failure);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        Long startTime = System.currentTimeMillis();
        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapFailureInSecondParCallable", stringLongEmptyIntAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest3OptionMapExceptionInSecondParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10Exception);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        builder.allMustSucceed(true);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapExceptionInSecondParCallableAllMustSucceed", stringLongIntAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest3OptionMapExceptionInSecondParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10Exception);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapExceptionInSecondParCallable", stringLongEmptyIntAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequest3OptionMapFailureInThirdParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200Failure);

        builder.allMustSucceed(true);

        Long startTime = System.currentTimeMillis();
        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapFailureInThirdParCallableAllMustSucceed", stringLongIntAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest3OptionMapFailureInThirdParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200Failure);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapFailureInThirdParCallable", stringLongIntEmptyAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest3OptionMapExceptionInThirdParCallableAllMustSucceed() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200Exception);

        builder.allMustSucceed(true);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapExceptionInThirdParCallableAllMustSucceed", stringLongIntAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest3OptionMapExceptionInThirdParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200Exception);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3OptionMapExceptionInThirdParCallable", stringLongIntEmptyAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequest3OptionFlatMapSuccess() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest3OptionFlatMapSuccess",
                        ParFunction.from((input) -> {
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
                                assertTrue(input._1().get().equals("100"));
                                assertTrue(input._2().get() == 100);
                                assertTrue(input._3().get() == 10);
                                return new Right<>(200);
                            }));
                            return builder2.build();
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest3OptionFlatMapFailure() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        ParRequest3Option<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest3OptionFlatMapFailure",
                        ParFunction.from((input) -> {
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
                                assertTrue(input._1().get().equals("100"));
                                assertTrue(input._2().get() == 100);
                                assertTrue(input._3().get() == 10);
                                throw new IllegalArgumentException("failed");
                            }));
                            return builder2.build();
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("failed"));
    }

    class TestResponse {

        public String err = "";
        public String success = "";
    }

    @Test
    public void testParRequest3OptionFoldSuccess() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        final TestResponse response = new TestResponse();
        ParRequest3Option<String, Long, Integer> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s-%s", input._1().get(), input._2().get(), input._3().get());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("100-100-10"));
    }

    @Test
    public void testParRequest3OptionFoldFailureInFirstParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Failure);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        final TestResponse response = new TestResponse();
        ParRequest3Option<String, Long, Integer> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            assertTrue(input._1().isEmpty());
            response.success = String.format("-%s-%s", input._2().get(), input._3().get());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertEquals(response.success, "-100-10");
    }

    @Test
    public void testParRequest3OptionFoldExceptionInSecondParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500Exception);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200);

        final TestResponse response = new TestResponse();
        ParRequest3Option<String, Long, Integer> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            assertTrue(input._2().isEmpty());
            response.success = String.format("%s--%s", input._1().get(), input._3().get());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertEquals(response.success, "100--10");
    }

    @Test
    public void testParRequest3OptionFoldExceptionInThirdParCallable() {
        ParRequest3Option.Builder<String, Long, Integer> builder = executor.parRequest3OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.setThirdParCallable(ParRequestTestCommon.integerSleep200Exception);

        final TestResponse response = new TestResponse();
        ParRequest3Option<String, Long, Integer> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            assertTrue(input._3().isEmpty());
            response.success = String.format("%s-%s-", input._1().get(), input._2().get());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertEquals(response.success, "100-100-");
    }
}
