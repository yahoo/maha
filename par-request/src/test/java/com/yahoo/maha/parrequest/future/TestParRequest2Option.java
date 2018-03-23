// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest.future;

import com.yahoo.maha.parrequest.Either;
import com.yahoo.maha.parrequest.GeneralError;
import com.yahoo.maha.parrequest.Nothing;
import com.yahoo.maha.parrequest.Option;
import com.yahoo.maha.parrequest.ParCallable;
import com.yahoo.maha.parrequest.Right;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import scala.Tuple2;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestParRequest2Option {

    private ParallelServiceExecutor executor;

    private ParFunction<Tuple2<Option<String>, Option<Long>>, Integer>
            stringLongAssert =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().get() == 100);
                return 200;
            });

    private ParFunction<Tuple2<Option<String>, Option<Long>>, Integer> stringEmptyLongAssert =
            ParFunction.from((input) -> {
                assertTrue(input._1().isEmpty());
                assertTrue(input._2().get() == 100);
                return 200;
            });

    private ParFunction<Tuple2<Option<String>, Option<Long>>, Integer> stringLongEmptyAssert =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().isEmpty());
                return 200;
            });

    private ParFunction<Tuple2<Option<String>, Option<Long>>, Either<GeneralError, Integer>> stringLongAssertMap =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().get() == 100);
                return new Right<>(200);
            });

    private ParFunction<Tuple2<Option<String>, Option<Long>>, Either<GeneralError, Integer>> stringEmptyLongAssertMap =
            ParFunction.from((input) -> {
                assertTrue(input._1().isEmpty());
                assertTrue(input._2().get() == 100);
                return new Right<>(200);
            });

    private ParFunction<Tuple2<Option<String>, Option<Long>>, Either<GeneralError, Integer>> stringLongEmptyAssertMap =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().isEmpty());
                return new Right<>(200);
            });

    private ParFunction<Tuple2<Option<String>, Option<Long>>, CombinableRequest<Integer>> stringLongAssertFlatMap =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().get() == 100);
                ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                builder2.setParCallable(ParCallable.from(() -> {
                    return new Right<>(200);
                }));
                return builder2.build();
            });

    private ParFunction<Tuple2<Option<String>, Option<Long>>, CombinableRequest<Integer>>
            stringLongAssertFlatMapFailure =
            ParFunction.from((input) -> {
                assertTrue(input._1().get().equals("100"));
                assertTrue(input._2().get() == 100);
                ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                builder2.setParCallable(ParCallable.from(() -> {
                    throw new IllegalArgumentException("failed");
                }));
                return builder2.build();
            });

    @BeforeClass
    public void setUp() throws Exception {
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(10000);
        executor.setPoolName("test-par-request2");
        executor.setQueueSize(20);
        executor.setThreadPoolSize(20);
        executor.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        executor.destroy();
    }

    @Test
    public void testParRequest2OptionResultMapSuccess() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        ParRequest2Option<String, Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequest2OptionResultMapFailure() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        ParRequest2Option<String, Long> request = builder.build();
        request.resultMap(ParFunction.from((input) -> {
            assertTrue(input._1().get().equals("100"));
            assertTrue(input._2().get() == 100);
            throw new IllegalArgumentException("failed");
        }));
    }

    @Test
    public void testParRequest2OptionResultMapFailureInFirstParCallable() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Failure);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        ParRequest2Option<String, Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringEmptyLongAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest2OptionResultMapExceptionInFirstParCallable() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Exception);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        ParRequest2Option<String, Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringEmptyLongAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequest2OptionResultMapFailureInSecondParCallable() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500Failure);

        ParRequest2Option<String, Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongEmptyAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest2OptionResultMapExceptionInSecondParCallable() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500Exception);

        ParRequest2Option<String, Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringLongEmptyAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequest2OptionMapSuccess() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer> composedRequest = request.map("testParRequest2OptionMapSuccess", stringLongAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest2OptionMapFailure() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2OptionMapFailure",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().get().equals("100"));
                            assertTrue(input._2().get() == 100);
                            throw new IllegalArgumentException("failed");
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("failed"));
    }

    @Test
    public void testParRequest2OptionMapFailureInFirstParCallableAllMustSucceed() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Failure);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.allMustSucceed(true);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2OptionMapFailureInFirstParCallableAllMustSucceed", stringLongAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest2OptionMapFailureInFirstParCallable() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Failure);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2OptionMapFailureInFirstParCallable", stringEmptyLongAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest2OptionMapExceptionInFirstParCallableAllMustSucceed() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Exception);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);
        builder.allMustSucceed(true);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2OptionMapExceptionInFirstParCallableAllMustSucceed", stringLongAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest2OptionMapExceptionInFirstParCallable() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Exception);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2OptionMapExceptionInFirstParCallable", stringEmptyLongAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequest2OptionMapFailureInSecondParCallableAllMustSucceed() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10Failure);
        builder.allMustSucceed(true);

        Long startTime = System.currentTimeMillis();
        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2OptionMapFailureInSecondParCallableAllMustSucceed", stringLongAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest2OptionMapFailureInSecondParCallable() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep10Failure);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2OptionMapFailureInSecondParCallable", stringLongEmptyAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest2OptionMapExceptionInSecondParCallableAllMustSucceed() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500Exception);
        builder.allMustSucceed(true);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2OptionMapExceptionInSecondParCallableAllMustSucceed", stringLongAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest2OptionMapExceptionInSecondParCallable() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500Exception);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2OptionMapExceptionInSecondParCallable", stringLongEmptyAssertMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequest2OptionFlatMapSuccess() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest2OptionMapSuccess", stringLongAssertFlatMap);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest2OptionFlatMapFailure() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        ParRequest2Option<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest2OptionFlatMapFailure", stringLongAssertFlatMapFailure);
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("failed"));
    }

    class TestResponse {

        public String err = "";
        public String success = "";
    }

    @Test
    public void testParRequest2OptionFoldSuccess() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        final TestResponse response = new TestResponse();
        ParRequest2Option<String, Long> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s", input._1().get(), input._2().get());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("100-100"));
    }

    @Test
    public void testParRequest2OptionFoldFailureInFirstParCallable() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000Failure);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500);

        final TestResponse response = new TestResponse();
        ParRequest2Option<String, Long> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            assertTrue(input._1().isEmpty());
            response.success = String.format("%s", input._2().get());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertEquals(response.success, "100");
    }

    @Test
    public void testParRequest2OptionFoldExceptionInSecondParCallable() {
        ParRequest2Option.Builder<String, Long> builder = executor.parRequest2OptionBuilder();
        builder.setFirstParCallable(ParRequestTestCommon.stringSleep1000);
        builder.setSecondParCallable(ParRequestTestCommon.longSleep500Exception);

        final TestResponse response = new TestResponse();
        ParRequest2Option<String, Long> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            assertTrue(input._2().isEmpty());
            response.success = String.format("%s", input._1().get());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertEquals(response.success, "100");
    }
}
