// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.yahoo.maha.parrequest2.GeneralError;
import com.yahoo.maha.parrequest2.Nothing;
import com.yahoo.maha.parrequest2.ParCallable;
import com.yahoo.maha.parrequest2.future.NoopRequest;
import com.yahoo.maha.parrequest2.future.ParFunction;
import com.yahoo.maha.parrequest2.future.ParRequest;
import com.yahoo.maha.parrequest2.future.ParRequest6;
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
public class TestParRequest6 {

    private ParallelServiceExecutor executor;

    @BeforeClass
    public void setUp() throws Exception {
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(10000);
        executor.setPoolName("test-par-request4");
        executor.setQueueSize(20);
        executor.setThreadPoolSize(10);
        executor.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        executor.destroy();
    }

    @Test
    public void testParRequest6ResultMapSuccess() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10.0D);
                    assertTrue(input._5() == 10.0F);
                    assertTrue(input._6() == 100);
                    return 200;
                }));
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequest6ResultMapFailure() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6ResultMapFailureInFirstParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));


        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6ResultMapExceptionInFirstParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6ResultMapFailureInSecondParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));


        Long startTime = System.currentTimeMillis();
        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6ResultMapExceptionInSecondParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6ResultMapFailureInThirdParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6ResultMapExceptionInThirdParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6ResultMapFailureInFourthParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6ResultMapExceptionInFourthParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6ResultMapExceptionInFifthParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6ResultMapExceptionInSixthParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            throw new IllegalArgumentException("blah1");
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6MapSuccess() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest6MapSuccess",
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
    public void testParRequest6MapFailure() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest6MapFailure",
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
    public void testParRequest6MapFailureInFirstParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest6MapFailureInFirstParCallable",
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
    public void testParRequest6MapExceptionInFirstParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest6MapExceptionInFirstParCallable",
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
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest6MapFailureInSecondParCallable",
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
    public void testParRequest6MapExceptionInSecondParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest6MapExceptionInSecondParCallable",
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
    public void testParRequest6MapFailureInThirdParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        Long startTime = System.currentTimeMillis();
        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest6MapFailureInThirdParCallable",
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
    public void testParRequest6MapExceptionInThirdParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest6MapExceptionInThirdParCallable",
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
    public void testParRequest6FlatMapSuccess() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest6FlatMapSuccess",
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
    public void testParRequest6FlatMapFailure() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest6FlatMapSuccess",
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
    public void testParRequest6FoldSuccess() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        final TestResponse response = new TestResponse();
        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6FoldFailureInFirstParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        final TestResponse response = new TestResponse();
        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6FoldFailureInSecondParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        final TestResponse response = new TestResponse();
        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
    public void testParRequest6FoldFailureInThirdParCallable() {
        ParRequest6.Builder<String, Long, Integer, Double, Float, Integer> builder = executor.parRequest6Builder();
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
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(200);
            return new Right<GeneralError, Integer>(100);
        }));

        final TestResponse response = new TestResponse();
        ParRequest6<String, Long, Integer, Double, Float, Integer> request = builder.build();
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
