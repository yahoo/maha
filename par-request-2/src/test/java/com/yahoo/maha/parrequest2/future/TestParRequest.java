// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.yahoo.maha.parrequest2.GeneralError;
import com.yahoo.maha.parrequest2.Nothing;
import com.yahoo.maha.parrequest2.ParCallable;

import com.yahoo.maha.parrequest2.future.NoopRequest;
import com.yahoo.maha.parrequest2.future.ParFunction;
import com.yahoo.maha.parrequest2.future.ParRequest;
import com.yahoo.maha.parrequest2.future.ParallelServiceExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.util.Either;
import scala.util.Right;

import java.util.function.Function;

import static org.testng.Assert.assertTrue;

/**
 * Created by hiral on 6/12/14.
 */
public class TestParRequest {

    private ParallelServiceExecutor executor;


    <T,U> Function1<T, U> fn1(Function<T, U> fn) {
        return new AbstractFunction1<T, U>() {
            @Override
            public U apply(T t) {
                return fn.apply(t);
            }
        };
    }

    @BeforeClass
    public void setUp() throws Exception {
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(10000);
        executor.setPoolName("test-par-request");
        executor.setQueueSize(20);
        executor.setThreadPoolSize(20);
        executor.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        executor.destroy();
    }

    @Test
    public void testParRequestResultMapSuccess() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input == 100);
            return 200;
        }));
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequestResultMapSuccessUsingWith() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.with(fn1((unit) -> {
            try {
                Thread.sleep(1000);
            } catch(Exception e) {
            }
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input == 100);
            return 200;
        }));
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequestResultMapFailure() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input == 100);
            throw new IllegalArgumentException("failed");
        }));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequestResultMapFailureUsingWith() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.with(fn1((unit) -> {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
            }
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input == 100);
            throw new IllegalArgumentException("failed");
        }));
    }

    @Test
    public void testParRequestResultMapFailureInParCallable() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));

        ParRequest<Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input == 100);
            return 200;
        }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequestResultMapExceptionInParCallable() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            throw new IllegalArgumentException("blah1");
        }));

        ParRequest<Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input == 100);
            return 200;
        }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequestMapSuccess() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequestMapSuccess", ParFunction.from((input) -> {
                    assertTrue(input == 100);
                    return new Right<GeneralError, Integer>(200);
                }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequestMapFailure() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequestMapFailure", ParFunction.from((input) -> {
                    assertTrue(input == 100);
                    throw new IllegalArgumentException("custom error message");
                }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.contains("custom error message"));
    }

    @Test
    public void testParRequestMapFailureInParCallable() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequestMapFailureInParCallable",
                        ParFunction.from((input) -> {
                            assertTrue(input == 100);
                            return new Right<GeneralError, Integer>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequestMapExceptionInParCallable() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            throw new IllegalArgumentException("blah1");
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequestMapExceptionInParCallable",
                        ParFunction.from((input) -> {
                            assertTrue(input == 100);
                            return new Right<GeneralError, Integer>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequestFlatMapSuccess() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequestFlatMapSuccess", ParFunction.from((input) -> {
                    ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                    builder2.setParCallable(ParCallable.from(() -> {
                        return new Right<GeneralError, Integer>(input + 100);
                    }));
                    return builder2.build();
                }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequestFlatMapFailureInFuture() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequestFlatMapFailureInFuture",
                        ParFunction.from((input) -> {
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
                                assertTrue(input == 100);
                                return GeneralError.either("future", "custom error message");
                            }));
                            return builder2.build();
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.contains("custom error message"));
    }

    @Test
    public void testParRequestFlatMapFailureInParCallable() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return GeneralError.either("testParRequestFlatMapFailure", "failed");
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequestFlatMapFailureInParCallable",
                        ParFunction.from((input) -> {
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
                                return new Right<GeneralError, Integer>(input + 100);
                            }));
                            return builder2.build();
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequestFlatMapExceptionInParCallable() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            throw new IllegalArgumentException("blah1");
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequestFlatMapExceptionInParCallable",
                        ParFunction.from((input) -> {
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
                                return new Right<GeneralError, Integer>(input + 100);
                            }));
                            return builder2.build();
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequestFlatMapExceptionInFuture() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequestFlatMapFailure", ParFunction.from((input) -> {
                    ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                    builder2.setParCallable(ParCallable.from(() -> {
                        assertTrue(input == 100);
                        throw new IllegalArgumentException("custom error message");
                    }));
                    return builder2.build();
                }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.contains("custom error message"));
    }

    class TestResponse {

        public String err = "";
        public String success = "";
    }

    @Test
    public void testParRequestFoldSuccess() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        final TestResponse response = new TestResponse();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = "success";
            return Nothing.get();
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("success"));
    }

    @Test
    public void testParRequestFoldFailure() {
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            throw new IllegalArgumentException("blah1");
        }));

        ParRequest<Integer> request = builder.build();
        final TestResponse response = new TestResponse();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = "success";
            return Nothing.get();
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.err.contains("blah1"));
    }

    @Test
    public void testParRequestImmediateResult() {
        ParRequest<Integer>
                parRequest =
                ParRequest
                        .immediateResult("testParRequestImmediateResult", executor, new Right<GeneralError, Integer>(100));
        Either<GeneralError, Integer> result = parRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 100);
    }
}
