// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import com.yahoo.maha.parrequest2.CustomRejectPolicy;
import com.yahoo.maha.parrequest2.GeneralError;
import com.yahoo.maha.parrequest2.ParCallable;
import com.yahoo.maha.parrequest2.future.ParFunction;
import com.yahoo.maha.parrequest2.future.ParallelServiceExecutor;
import com.yahoo.maha.parrequest2.future.NoopRequest;
import com.yahoo.maha.parrequest2.future.ParRequest;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import scala.util.Either;
import scala.util.Right;

import java.util.concurrent.RejectedExecutionException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by jians on 8/6/15.
 */
public class TestCustomRejectPolicy {

    private ParallelServiceExecutor parallelServiceExecutor;
    private final int MAX_QUEUE_SIZE = 5;
    private final int MAX_THREAD_POOL_SIZE = 5;
    private final int TASK_EXECUTE_TIME_IN_MILLIS = 4000;
    private final int DEFAULT_TIMEOUT_MILLIS = 5000;

    @BeforeTest
    public void init() throws Exception {
        parallelServiceExecutor = new ParallelServiceExecutor();
        parallelServiceExecutor.setDefaultTimeoutMillis(DEFAULT_TIMEOUT_MILLIS);
        parallelServiceExecutor.setQueueSize(MAX_QUEUE_SIZE);
        parallelServiceExecutor.setThreadPoolSize(MAX_THREAD_POOL_SIZE);
        parallelServiceExecutor.setRejectedExecutionHandler(new CustomRejectPolicy());
        parallelServiceExecutor.init();
    }

    @AfterTest
    public void destroy() throws Exception {
        if (parallelServiceExecutor != null) {
            parallelServiceExecutor.destroy();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void submitParcallableShouldThrowRejectedExecutionError() {
        ParRequest[] requests = new ParRequest[MAX_QUEUE_SIZE + MAX_THREAD_POOL_SIZE + 1];
        for (int i = 0; i < MAX_QUEUE_SIZE + MAX_THREAD_POOL_SIZE + 1; i++) {
            requests[i] = genearteDummyRequest(TASK_EXECUTE_TIME_IN_MILLIS);
        }
        Either<GeneralError, String> failResultEither = requests[requests.length - 1].get(DEFAULT_TIMEOUT_MILLIS);
        assertTrue(failResultEither.isLeft());
        GeneralError generalError = failResultEither.left().get();
        assertTrue(generalError.throwableOption.isDefined());
        assertTrue(generalError.throwableOption.get() instanceof RejectedExecutionException);
    }

    @Test
    public void addListenerShouldThrowRejectedExcutionError() {
        // first request should succeed
        // but since it is 1 sec faster than others,
        // it should try to execute listeners when others are not done
        // the listener will be rejected but it should still succeed, ran by the calling thread
        NoopRequest<String> shouldSuccessResult = genearteDummyRequest(TASK_EXECUTE_TIME_IN_MILLIS - 1000).fold(
            ParFunction.from((generalError) -> {
                    return "Failed with: " + generalError.message;
            }),
            ParFunction.from((input) -> {
                    return input;
            })
        );
        for (int i = 0; i < MAX_QUEUE_SIZE + MAX_THREAD_POOL_SIZE; i++) {
            genearteDummyRequest(TASK_EXECUTE_TIME_IN_MILLIS);
        }
        // last request should be rejected right away since queue is already full at this time
        // and the listener will be rejected also
        // but the error should be pass on to the listener using the calling thread
        NoopRequest<String> shouldFailResult = genearteDummyRequest(TASK_EXECUTE_TIME_IN_MILLIS)
            .fold(
                ParFunction.from((generalError) -> {
                        return "Failed with: " + generalError.throwableOption.get().getClass().getCanonicalName();
                }),
                ParFunction.from((input) -> {
                    return input;
                })
            );

        assertEquals("done", shouldSuccessResult.get().right().get());
        assertEquals("Failed with: java.util.concurrent.RejectedExecutionException",
                     shouldFailResult.get().right().get());
    }

    private ParRequest<String> genearteDummyRequest(final int sleepInMillis) {
        return parallelServiceExecutor.<String>parRequestBuilder()
            .setLabel("test-parRequest")
            .setParCallable(ParCallable.from(() -> {
                    Thread.sleep(sleepInMillis);
                    return new Right<>("done");
            }))
            .build();
    }
}
