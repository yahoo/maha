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
import scala.util.Either;
import scala.util.Right;

import static org.testng.Assert.assertTrue;

/**
 * Created by hiral on 6/16/14.
 */
public class TestParRequest4 {
    @BeforeSuite(alwaysRun = true)
    public void beforeSuite(ITestContext context) {
        for (ITestNGMethod method : context.getAllTestMethods()) {
            method.setRetryAnalyzer(new RetryAnalyzerImpl());
        }
    }

    private ParallelServiceExecutor executor;

    @BeforeClass
    public void setUp() throws Exception {
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(20000);
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
    public void testParRequest4ResultMapSuccess() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10.0D);
                    return 200;
                }));
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequest4ResultMapFailure() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10.0D);
                    throw new IllegalArgumentException("failed");
                }));
    }

    @Test
    public void testParRequest4ResultMapFailureInFirstParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
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
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest4ResultMapExceptionInFirstParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
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
    public void testParRequest4ResultMapFailureInSecondParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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


        Long startTime = System.currentTimeMillis();
        ParRequest4<String, Long, Integer, Double> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10.0D);
                    return 200;
                }));
        Long endTime = System.currentTimeMillis();
        assertTrue((endTime - startTime) < 500);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"));

    }

    @Test
    public void testParRequest4ResultMapExceptionInSecondParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        Either<GeneralError, Integer>
                result =
                request.resultMap(ParFunction.from((input) -> {
                    assertTrue(input._1().equals("100"));
                    assertTrue(input._2() == 100);
                    assertTrue(input._3() == 10);
                    assertTrue(input._4() == 10D);
                    return 200;
                }));
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequest4ResultMapFailureInThirdParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        Long startTime = System.currentTimeMillis();
        ParRequest4<String, Long, Integer, Double> request = builder.build();
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
    public void testParRequest4ResultMapExceptionInThirdParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
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
    public void testParRequest4ResultMapFailureInFourthParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        Long startTime = System.currentTimeMillis();
        ParRequest4<String, Long, Integer, Double> request = builder.build();
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
    public void testParRequest4ResultMapExceptionInFourthParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
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
    public void testParRequest4MapSuccess() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest4MapSuccess",
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
    public void testParRequest4MapFailure() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest4MapFailure",
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
    public void testParRequest4MapFailureInFirstParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest4MapFailureInFirstParCallable",
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
    public void testParRequest4MapExceptionInFirstParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest4MapExceptionInFirstParCallable",
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
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        Long startTime = System.currentTimeMillis();
        ParRequest4<String, Long, Integer, Double> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest4MapFailureInSecondParCallable",
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
    public void testParRequest4MapExceptionInSecondParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest4MapExceptionInSecondParCallable",
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
    public void testParRequest4MapFailureInThirdParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        Long startTime = System.currentTimeMillis();
        ParRequest4<String, Long, Integer, Double> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest4MapFailureInThirdParCallable",
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
    public void testParRequest4MapExceptionInThirdParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequest4MapExceptionInThirdParCallable",
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
    public void testParRequest4FlatMapSuccess() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest4FlatMapSuccess",
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
    public void testParRequest4FlatMapFailure() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        ParRequest4<String, Long, Integer, Double> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequest4FlatMapSuccess",
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
    public void testParRequest4FoldSuccess() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        final TestResponse response = new TestResponse();
        ParRequest4<String, Long, Integer, Double> request = builder.build();
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
    public void testParRequest4FoldFailureInFirstParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        final TestResponse response = new TestResponse();
        ParRequest4<String, Long, Integer, Double> request = builder.build();
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
    public void testParRequest4FoldFailureInSecondParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        final TestResponse response = new TestResponse();
        ParRequest4<String, Long, Integer, Double> request = builder.build();
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
    public void testParRequest4FoldFailureInThirdParCallable() {
        ParRequest4.Builder<String, Long, Integer, Double> builder = executor.parRequest4Builder();
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

        final TestResponse response = new TestResponse();
        ParRequest4<String, Long, Integer, Double> request = builder.build();
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
