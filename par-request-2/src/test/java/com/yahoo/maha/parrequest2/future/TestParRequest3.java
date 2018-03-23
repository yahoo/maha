// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.yahoo.maha.parrequest2.ParCallable;
import com.yahoo.maha.parrequest2.GeneralError;
import com.yahoo.maha.parrequest2.Nothing;

import com.yahoo.maha.parrequest2.future.NoopRequest;
import com.yahoo.maha.parrequest2.future.ParFunction;
import com.yahoo.maha.parrequest2.future.ParRequest;
import com.yahoo.maha.parrequest2.future.ParRequest3;
import com.yahoo.maha.parrequest2.future.ParallelServiceExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.util.Either;
import scala.util.Right;

import static org.testng.Assert.assertTrue;

/**
 * Created by hiral on 6/16/14.
 */
public class TestParRequest3 {

    private ParallelServiceExecutor executor;

    @BeforeClass
    public void setUp() throws Exception {
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(10000);
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
    public void testParRequest3ResultMapSuccess() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    return 200;
                }));
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequest3ResultMapFailure() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    throw new IllegalArgumentException("failed");
                }));
    }

    @Test
    public void testParRequest3ResultMapFailureInFirstParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    return 200;
                }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest3ResultMapExceptionInFirstParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            throw new IllegalArgumentException("blah1");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    return 200;
                }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest3ResultMapFailureInSecondParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(10);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest3<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    return 200;
                }));
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest3ResultMapExceptionInSecondParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            throw new IllegalArgumentException("blah1");
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    return 200;
                }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest3ResultMapFailureInThirdParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(10);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest3<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    return 200;
                }));
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest3ResultMapExceptionInThirdParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            throw new IllegalArgumentException("blah1");
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    return 200;
                }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest3MapSuccess() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3MapSuccess",
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
    public void testParRequest3MapFailure() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3MapFailure",
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
    public void testParRequest3MapFailureInFirstParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3MapFailureInFirstParCallable",
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
    public void testParRequest3MapExceptionInFirstParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            throw new IllegalArgumentException("blah1");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3MapExceptionInFirstParCallable",
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
    public void testParRequest3MapFailureInSecondParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(10);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest3<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3MapFailureInSecondParCallable",
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
    public void testParRequest3MapExceptionInSecondParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            throw new IllegalArgumentException("blah1");
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3MapExceptionInSecondParCallable",
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
    public void testParRequest3MapFailureInThirdParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(10);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest3<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3MapFailureInThirdParCallable",
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
    public void testParRequest3MapExceptionInThirdParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            throw new IllegalArgumentException("blah1");
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest3MapExceptionInThirdParCallable",
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
    public void testParRequest3FlatMapSuccess() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest3FlatMapSuccess",
                        ParFunction.from((input) -> {
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
                                assertTrue(input._1().equals("100"));
                                assertTrue(input._2() == 100);
                                return new Right<>(200);
                            }));
                            return builder2.build();
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testParRequest3FlatMapFailure() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        ParRequest3<String, Long, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest3FlatMapSuccess",
                        ParFunction.from((input) -> {
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
                                assertTrue(input._1().equals("100"));
                                assertTrue(input._2() == 100);
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
    public void testParRequest3FoldSuccess() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        final TestResponse response = new TestResponse();
        ParRequest3<String, Long, Integer> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s-%s", input._1(), input._2(), input._3());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("100-100-10"));
    }

    @Test
    public void testParRequest3FoldFailureInFirstParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return GeneralError.either("executeRequest", "blah1");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        final TestResponse response = new TestResponse();
        ParRequest3<String, Long, Integer> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s-%s", input._1(), input._2(), input._3());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.err.contains("blah1"));
    }

    @Test
    public void testParRequest3FoldFailureInSecondParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            throw new IllegalArgumentException("blah1");
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(10);
        }));

        final TestResponse response = new TestResponse();
        ParRequest3<String, Long, Integer> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s-%s", input._1(), input._2(), input._3());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.err.contains("blah1"));
    }

    @Test
    public void testParRequest3FoldFailureInThirdParCallable() {
        ParRequest3.Builder<String, Long, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, String>("100");
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(500);
            return new Right<GeneralError, Long>(100L);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            throw new IllegalArgumentException("blah1");
        }));

        final TestResponse response = new TestResponse();
        ParRequest3<String, Long, Integer> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s-%s", input._1(), input._2(), input._3());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.err.contains("blah1"));
    }
}
