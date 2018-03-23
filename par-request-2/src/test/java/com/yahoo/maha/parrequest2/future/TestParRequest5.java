// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.yahoo.maha.parrequest2.GeneralError;
import com.yahoo.maha.parrequest2.Nothing;
import com.yahoo.maha.parrequest2.ParCallable;
import com.yahoo.maha.parrequest2.future.NoopRequest;
import com.yahoo.maha.parrequest2.future.ParFunction;
import com.yahoo.maha.parrequest2.future.ParRequest;
import com.yahoo.maha.parrequest2.future.ParRequest5;
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
public class TestParRequest5 {

    private ParallelServiceExecutor executor;

    @BeforeClass
    public void setUp() throws Exception {
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(10000);
        executor.setPoolName("test-par-request4");
        executor.setQueueSize(20);
        executor.setThreadPoolSize(20);
        executor.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        executor.destroy();
    }

    @Test
    public void testParRequest5ResultMapSuccess() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10.0D);
                    assertTrue(input._5() == 10.0F);
                    return 200;
                }));
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequest5ResultMapFailure() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10.0D);
                    assertTrue(input._5() == 10.0F);
                    throw new IllegalArgumentException("failed");
                }));
    }

    @Test
    public void testParRequest5ResultMapFailureInFirstParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));


        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10.0D);
                    assertTrue(input._5() == 10.0F);
                    return 200;
                }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest5ResultMapExceptionInFirstParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10.0D);
                    assertTrue(input._5() == 10.0F);
                    return 200;
                }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest5ResultMapFailureInSecondParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));


        Long startTime = System.currentTimeMillis();
        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10.0D);
                    assertTrue(input._5() == 10.0F);
                    return 200;
                }));
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest5ResultMapExceptionInSecondParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10D);
                    assertTrue(input._5() == 10.0F);
                    return 200;
                }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest5ResultMapFailureInThirdParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
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
    public void testParRequest5ResultMapExceptionInThirdParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
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
    public void testParRequest5ResultMapFailureInFourthParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return GeneralError.either("testParRequestMapFailure", "failed");
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
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
    public void testParRequest5ResultMapExceptionInFourthParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            throw new IllegalArgumentException("blah1");
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
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
    public void testParRequest5ResultMapExceptionInFifthParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            throw new IllegalArgumentException("blah1");
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10.0D);
                    return 200;
                }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }
    @Test
    public void testParRequest5MapSuccess() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest5MapSuccess",
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
    public void testParRequest5MapFailure() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest5MapFailure",
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
    public void testParRequest5MapFailureInFirstParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest5MapFailureInFirstParCallable",
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
    public void testParRequest5MapExceptionInFirstParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest5MapExceptionInFirstParCallable",
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
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest5MapFailureInSecondParCallable",
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
    public void testParRequest5MapExceptionInSecondParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest5MapExceptionInSecondParCallable",
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
    public void testParRequest5MapFailureInThirdParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest5MapFailureInThirdParCallable",
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
    public void testParRequest5MapExceptionInThirdParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest5MapExceptionInThirdParCallable",
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
    public void testParRequest5FlatMapSuccess() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest5FlatMapSuccess",
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
    public void testParRequest5FlatMapFailure() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest5FlatMapSuccess",
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
    public void testParRequest5FoldSuccess() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        final TestResponse response = new TestResponse();
        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s-%s-%s", input._1(), input._2(), input._3(), input._4());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("100-100-10-10.0"));
    }

    @Test
    public void testParRequest5FoldFailureInFirstParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        final TestResponse response = new TestResponse();
        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s-%s-%s", input._1(), input._2(), input._3(), input._4());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.err.contains("blah1"));
    }

    @Test
    public void testParRequest5FoldFailureInSecondParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        final TestResponse response = new TestResponse();
        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s-%s-%s", input._1(), input._2(), input._3(), input._4());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.err.contains("blah1"));
    }

    @Test
    public void testParRequest5FoldFailureInThirdParCallable() {
        ParRequest5.Builder<String, Long, Integer, Double, Float> builder = executor.parRequest5Builder();
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
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Double>(10.0D);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Float>(10.0F);
        }));

        final TestResponse response = new TestResponse();
        ParRequest5<String, Long, Integer, Double, Float> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.format("%s-%s-%s-%s", input._1(), input._2(), input._3(), input._4());
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.err.contains("blah1"));
    }
}
