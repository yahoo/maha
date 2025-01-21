// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.yahoo.maha.parrequest2.*;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import scala.util.Either;
import scala.util.Right;

import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertTrue;

/**
 * Created by jians on 4/1/15.
 */
public class TestParRequestListEither {
    private String printError(Either<GeneralError, Integer> result) {
        return result.left().get().message +
               "\nThrowable stacktrace:\n" +
               EitherUtils.map(ParFunction.from(ExceptionUtils::getStackTrace), result.left().get().throwableOption);
    }

    private ParallelServiceExecutor executor;

    private ParFunction<List<Either<GeneralError, String>>, Integer> stringAssert =
            ParFunction.from((input) -> {
                for (Either<GeneralError, String> stringOption : input) {
                    assertTrue(stringOption.right().get().equals("100"));
                }
                return 200;
            });

    private ParFunction<List<Either<GeneralError, String>>, Integer> stringAssertOnlyFirstRequestEmpty =
            ParFunction.from((input) -> {
                for (int i = 0; i < 8; i++) {
                    assertTrue(
                            (i == 0 && input.get(i).isLeft()) ||
                                    (i > 0 && input.get(i).right().get().equals("100")),
                            "first request should fail and the rest should succeed"
                    );
                }
                return 200;
            });

    @BeforeClass
    public void beforeSuite(ITestContext context) throws Exception {
        for (ITestNGMethod method : context.getAllTestMethods()) {
            method.setRetryAnalyzerClass(RetryAnalyzerImpl.class);
        }
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(20000);
        executor.setPoolName("test-par-request-list");
        executor.setQueueSize(20);
        executor.setThreadPoolSize(10);
        executor.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        executor.destroy();
    }

    @Test
    public void testParRequestListEitherResultMapSuccess() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        for (int i = 0; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }
        ParRequestListEither<String> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringAssert);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequestListEitherResultMapFailureAllMustSucceed() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        for (int i = 0; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }
        builder.allMustSucceed(true);

        ParRequestListEither<String> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            for (Either<GeneralError, String> stringEither : input) {
                assertTrue(stringEither.right().get().equals("100"));
            }
            throw new IllegalArgumentException("failed");
        }));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParRequestListEitherResultMapFailure() {
        ParRequestListEither.Builder<Long> builder = executor.parRequestListEitherBuilder();
        for (int i = 0; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.longSleep500);
        }

        ParRequestListEither<Long> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            for (Either<GeneralError, Long> longEither : input) {
                assertTrue(longEither.right().get() == 100);
            }
            throw new IllegalArgumentException("failed");
        }));
    }

    @Test
    public void testParRequestListEitherResultMapFailureInFirstParCallableAllMustSucceed() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        builder.addParCallable(ParRequestTestCommon.stringSleep1000Failure);
        for (int i = 1; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }
        builder.allMustSucceed(true);

        ParRequestListEither<String> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringAssert);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"), printError(result));
    }

    @Test
    public void testParRequestListEitherResultMapFailureInFirstParCallable() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        builder.addParCallable(ParRequestTestCommon.stringSleep1000Failure);
        for (int i = 1; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }

        ParRequestListEither<String> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringAssertOnlyFirstRequestEmpty);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequestListEitherResultMapExceptionInFirstParCallableAllMustSucceed() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        builder.addParCallable(ParRequestTestCommon.stringSleep1000Exception);
        for (int i = 1; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }
        builder.allMustSucceed(true);

        ParRequestListEither<String> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringAssertOnlyFirstRequestEmpty);
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequestListEitherResultMapExceptionInFirstParCallable() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        builder.addParCallable(ParRequestTestCommon.stringSleep1000Exception);
        for (int i = 1; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }

        ParRequestListEither<String> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(stringAssertOnlyFirstRequestEmpty);
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequestListEitherMapSuccess() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        for (int i = 0; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }

        ParRequestListEither<String> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequestListEitherMapSuccess",
                        ParFunction.from((input) -> {
                            for (Either<GeneralError, String> stringEither : input) {
                                assertTrue(stringEither.right().get().equals("100"));
                            }
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequestListEitherMapFailure() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        for (int i = 0; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }

        ParRequestListEither<String> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequestListEitherMapFailure",
                        ParFunction.from((input) -> {
                            for (Either<GeneralError, String> stringEither : input) {
                                assertTrue(stringEither.right().get().equals("100"));
                            }
                            throw new IllegalArgumentException("failed");
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("failed"), printError(result));
    }

    @Test
    public void testParRequestListEitherMapFailureInFirstParCallableAllMustSucceed() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        builder.addParCallable(ParRequestTestCommon.stringSleep1000Failure);
        for (int i = 1; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }
        builder.allMustSucceed(true);

        ParRequestListEither<String> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequestListEitherMapFailureInFirstParCallableAllMustSucceed",
                        ParFunction.from((input) -> {
                            for (int i = 0; i < 8; i++) {
                                assertTrue(
                                        (i == 0 && input.get(i).isLeft()) ||
                                                (i > 0 && input.get(i).right().get().equals("100")),
                                        "first request should fail and the rest should succeed"
                                );
                            }
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().message.equals("failed"), printError(result));
    }

    @Test
    public void testParRequestListEitherMapFailureInFirstParCallable() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        builder.addParCallable(ParRequestTestCommon.stringSleep1000Failure);
        for (int i = 1; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }

        ParRequestListEither<String> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequestListEitherMapFailureInFirstParCallableAllMustSucceed",
                        ParFunction.from((input) -> {
                            for (int i = 0; i < 8; i++) {
                                assertTrue(
                                        (i == 0 && input.get(i).isLeft()) ||
                                                (i > 0 && input.get(i).right().get().equals("100")),
                                        "first request should fail and the rest should succeed"
                                );
                            }
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequestListEitherMapExceptionInFirstParCallableAllMustSucceed() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        builder.addParCallable(ParRequestTestCommon.stringSleep1000Exception);
        for (int i = 1; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }
        builder.allMustSucceed(true);

        ParRequestListEither<String> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequestListEitherMapExceptionInSecondParCallableAllMustSucceed",
                        ParFunction.from((input) -> {
                            for (int i = 0; i < 8; i++) {
                                assertTrue(
                                        (i == 0 && input.get(i).isLeft()) ||
                                                (i > 0 && input.get(i).right().get().equals("100")),
                                        "first request should fail and the rest should succeed"
                                );
                            }
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("blah1"));
    }

    @Test
    public void testParRequestListEitherMapExceptionInFirstParCallable() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        builder.addParCallable(ParRequestTestCommon.stringSleep1000Exception);
        for (int i = 1; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }

        ParRequestListEither<String> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testParRequestListEitherMapExceptionInSecondParCallableAllMustSucceed",
                        ParFunction.from((input) -> {
                            for (int i = 0; i < 8; i++) {
                                assertTrue(
                                        (i == 0 && input.get(i).isLeft()) ||
                                                (i > 0 && input.get(i).right().get().equals("100")),
                                        "first request should fail and the rest should succeed"
                                );
                            }
                            return new Right<>(200);
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequestListEitherFlatMapSuccess() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        for (int i = 0; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }

        ParRequestListEither<String> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequestListEitherFlatMapSuccess",
                        ParFunction.from((input) -> {
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
                                for (Either<GeneralError, String> stringEither : input) {
                                    assertTrue(stringEither.right().get().equals("100"));
                                }
                                return new Right<>(200);
                            }));
                            return builder2.build();
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testParRequestListEitherFlatMapFailure() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        for (int i = 0; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }

        ParRequestListEither<String> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testParRequestListEitherFlatMapSuccess",
                        ParFunction.from((input) -> {
                            ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                            builder2.setParCallable(ParCallable.from(() -> {
                                for (Either<GeneralError, String> stringEither : input) {
                                    assertTrue(stringEither.right().get().equals("100"));
                                }
                                throw new IllegalArgumentException("failed");
                            }));
                            return builder2.build();
                        }));
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().throwableOption.get().getMessage().contains("failed"), printError(result));
    }

    class TestResponse {

        public String err = "";
        public String success = "";
    }

    @Test
    public void testParRequestListEitherFoldSuccess() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        for (int i = 0; i < 8; i++) {
            builder.addParCallable(ParRequestTestCommon.stringSleep1000);
        }

        final TestResponse response = new TestResponse();
        ParRequestListEither<String> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.join("-", input.stream().map((either) -> {
                return either.right().get();
            }).collect(Collectors.toList()));
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("100-100-100-100-100-100-100-100"));
    }

    @Test
    public void testParRequestListEitherFoldFailureInEvenNumberParCallable() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        for (int i = 0; i < 8; i++) {
            if ((i & 1) == 1) {
                builder.addParCallable(ParRequestTestCommon.stringSleep1000);
            } else {
                builder.addParCallable(ParRequestTestCommon.stringSleep1000Exception);
            }

        }
        final TestResponse response = new TestResponse();
        ParRequestListEither<String> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.join("-", input.stream().map((either) -> {
                if (either.isLeft()) {
                    return "Empty";
                } else {
                    return either.right().get();
                }
            }).collect(Collectors.toList()));
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("Empty-100-Empty-100-Empty-100-Empty-100"));

    }

    @Test
    public void testParRequestListEitherFoldExceptionInEvenNumberParCallable() {
        ParRequestListEither.Builder<String> builder = executor.parRequestListEitherBuilder();
        for (int i = 0; i < 8; i++) {
            if ((i & 1) == 1) {
                builder.addParCallable(ParRequestTestCommon.stringSleep1000);
            } else {
                builder.addParCallable(ParRequestTestCommon.stringSleep1000Exception);
            }
        }

        final TestResponse response = new TestResponse();
        ParRequestListEither<String> request = builder.build();
        NoopRequest<Nothing> composedRequest = request.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return Nothing.get();
        }), ParFunction.from((input) -> {
            response.success = String.join("-", input.stream().map((either) -> {
                if (either.isLeft()) {
                    return "Empty";
                } else {
                    return either.right().get();
                }
            }).collect(Collectors.toList()));
            return null;
        }));
        Either<GeneralError, Nothing> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("Empty-100-Empty-100-Empty-100-Empty-100"));
    }
}
