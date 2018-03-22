// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.yahoo.maha.parrequest2.GeneralError;
import com.yahoo.maha.parrequest2.Nothing;
import com.yahoo.maha.parrequest2.ParCallable;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.util.Either;
import scala.util.Right;

import static org.testng.Assert.assertTrue;

/**
 * Created by hiral on 6/16/14.
 */
public class TestParRequest2 {

    private ParallelServiceExecutor executor;

    @BeforeClass
    public void setUp() throws Exception {
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(10000);
        executor.setPoolName("test-par-request2");
        executor.setQueueSize(20);
        executor.setThreadPoolSize(10);
        executor.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        executor.destroy();
    }

    @Test
    public void testParRequest2ResultMapSuccess() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        ParRequest2<String, Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input._1().equals("100"));
            assertTrue(input._2() == 100);
            return 200;
        }));
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequest2ResultMapFailure() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        ParRequest2<String, Long> request = builder.build();
        request.resultMap(ParFunction.from((input) -> {
            assertTrue(input._1().equals("100"));
            assertTrue(input._2() == 100);
            throw new IllegalArgumentException("failed");
        }));
    }

    @Test
    public void testParRequest2ResultMapFailureInFirstParCallable() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return GeneralError.either("testParRequestResultMapFailure", "failed");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        ParRequest2<String, Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input._1().equals("100"));
            assertTrue(input._2() == 100);
            return 200;
        }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest2ResultMapExceptionInFirstParCallable() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            throw new IllegalArgumentException("blah1");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        ParRequest2<String, Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input._1().equals("100"));
            assertTrue(input._2() == 100);
            return 200;
        }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest2ResultMapFailureInSecondParCallable() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(10);
            return GeneralError.either("testParRequestResultMapFailure", "failed");
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest2<String, Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input._1().equals("100"));
            assertTrue(input._2() == 100);
            return 200;
        }));
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest2ResultMapExceptionInSecondParCallable() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            throw new IllegalArgumentException("blah1");
        }));

        ParRequest2<String, Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input._1().equals("100"));
            assertTrue(input._2() == 100);
            return 200;
        }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest2MapSuccess() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        ParRequest2<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2MapSuccess",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().equals("100"));
                            assertTrue(input._2() == 100);
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest2MapFailure() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        ParRequest2<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2MapFailure",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().equals("100"));
                            assertTrue(input._2() == 100);
                            throw new IllegalArgumentException("failed");
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("failed"));
    }

    @Test
    public void testParRequest2MapFailureInFirstParCallable() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        ParRequest2<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2MapFailureInFirstParCallable",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().equals("100"));
                            assertTrue(input._2() == 100);
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest2MapExceptionInFirstParCallable() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            throw new IllegalArgumentException("blah1");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        ParRequest2<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2MapExceptionInFirstParCallable",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().equals("100"));
                            assertTrue(input._2() == 100);
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest2MapFailureInSecondParCallable() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(10);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest2<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2MapFailureInSecondParCallable",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().equals("100"));
                            assertTrue(input._2() == 100);
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest2MapExceptionInSecondParCallable() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            throw new IllegalArgumentException("blah1");
        }));

        ParRequest2<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest2MapExceptionInSecondParCallable",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().equals("100"));
                            assertTrue(input._2() == 100);
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest2FlatMapSuccess() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        ParRequest2<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest2FlatMapSuccess",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().equals("100"));
                            assertTrue(input._2() == 100);
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
                                return new Right<GeneralError, Integer>(200);
                            }));
                            return builder2.build();
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest2FlatMapFailure() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        ParRequest2<String, Long> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest2FlatMapFailure",
                        ParFunction.from((input) -> {
                            assertTrue(input._1().equals("100"));
                            assertTrue(input._2() == 100);
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
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
    public void testParRequest2FoldSuccess() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        final TestResponse response = new TestResponse();
        ParRequest2<String, Long> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s", input._1(), input._2());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("100-100"));
    }

    @Test
    public void testParRequest2FoldFailureInFirstParCallable() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return GeneralError.either("executeRequest", "blah1");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));

        final TestResponse response = new TestResponse();
        ParRequest2<String, Long> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s", input._1(), input._2());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.err.contains("blah1"));
    }

    @Test
    public void testParRequest2FoldFailureInSecondParCallable() {
        ParRequest2.Builder<String, Long> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            throw new IllegalArgumentException("blah1");
        }));

        final TestResponse response = new TestResponse();
        ParRequest2<String, Long> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s", input._1(), input._2());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.err.contains("blah1"));
    }
}
