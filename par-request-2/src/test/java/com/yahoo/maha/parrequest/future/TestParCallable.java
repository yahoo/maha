// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest.future;

import com.yahoo.maha.parrequest2.GeneralError;
import com.yahoo.maha.parrequest2.ParCallable;

import com.yahoo.maha.parrequest2.future.ParFunction;
import com.yahoo.maha.parrequest2.future.ParRequest;
import com.yahoo.maha.parrequest2.future.ParallelServiceExecutor;
import org.slf4j.MDC;
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
public class TestParCallable {

    <T,U> Function1<T, U> fn1(Function<T, U> fn) {
        return new AbstractFunction1<T, U>() {
            @Override
            public U apply(T t) {
                return fn.apply(t);
            }
        };
    }

    static {
        ParCallable.setEnableMDCInject(true);
        ParFunction.setEnableMDCInject(true);
        ParCallable.setRequestIdMDCKey("request-id");
        ParCallable.setUserInfoMDCKey("user-info");
        ParFunction.setRequestIdMDCKey("request-id");
        ParFunction.setUserInfoMDCKey("user-info");
    }

    private ParallelServiceExecutor executor;

    @BeforeClass
    public void setUp() throws Exception {
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(10000);
        executor.setPoolName("test-par-callable");
        executor.setQueueSize(20);
        executor.setThreadPoolSize(3);
        executor.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        executor.destroy();
    }

    @Test
    public void testMDCRequestMap() {
        MDC.put("request-id", "123");
        MDC.put("user-info", "user-1");
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            assertTrue("123".equals(MDC.get("request-id")));
            assertTrue("user-1".equals(MDC.get("user-info")));
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.from((input) -> {
            assertTrue(input == 100);
            assertTrue("123".equals(MDC.get("request-id")));
            assertTrue("user-1".equals(MDC.get("user-info")));
            return 200;
        }));
        MDC.remove("request-id");
        MDC.remove("user-info");
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testMDCRequestMapFn1() {
        MDC.put("request-id", "123");
        MDC.put("user-info", "user-1");
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.fromScala(fn1((nothing) -> {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
            }
            assertTrue("123".equals(MDC.get("request-id")));
            assertTrue("user-1".equals(MDC.get("user-info")));
            return new Right<GeneralError, Integer>(100);
        })));

        ParRequest<Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.resultMap(ParFunction.fromScala(fn1((input) -> {
            assertTrue(input == 100);
            assertTrue("123".equals(MDC.get("request-id")));
            assertTrue("user-1".equals(MDC.get("user-info")));
            return 200;
        })));
        MDC.remove("request-id");
        MDC.remove("user-info");
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testMDCMapSuccess() {
        MDC.put("request-id", "456");
        MDC.put("user-info", "user-2");
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            assertTrue("456".equals(MDC.get("request-id")));
            assertTrue("user-2".equals(MDC.get("user-info")));
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testMDCMapSuccess", ParFunction.from((input) -> {
                    assertTrue(input == 100);
                    assertTrue("456".equals(MDC.get("request-id")));
                    assertTrue("user-2".equals(MDC.get("user-info")));
                    return new Right<GeneralError, Integer>(200);
                }));
        MDC.remove("request-id");
        MDC.remove("user-info");
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }


    @Test
    public void testMDCMapSuccessFn1() {
        MDC.put("request-id", "456");
        MDC.put("user-info", "user-2");
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.fromScala(fn1((nothing) -> {
            try {
                Thread.sleep(1000);
            } catch(Exception e) {
            }
            assertTrue("456".equals(MDC.get("request-id")));
            assertTrue("user-2".equals(MDC.get("user-info")));
            return new Right<GeneralError, Integer>(100);
        })));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.map("testMDCMapSuccess", ParFunction.fromScala(fn1((input) -> {
                    assertTrue(input == 100);
                    assertTrue("456".equals(MDC.get("request-id")));
                    assertTrue("user-2".equals(MDC.get("user-info")));
                    return new Right<GeneralError, Integer>(200);
                })));
        MDC.remove("request-id");
        MDC.remove("user-info");
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);
    }

    @Test
    public void testMDCFlatMapSuccess() {
        MDC.put("request-id", "789");
        MDC.put("user-info", "user-3");
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            assertTrue("789".equals(MDC.get("request-id")));
            assertTrue("user-3".equals(MDC.get("user-info")));
            return new Right<GeneralError, Integer>(100);
        }));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testMDCFlatMapSuccess", ParFunction.from((input) -> {
                    assertTrue("789".equals(MDC.get("request-id")));
                    assertTrue("user-3".equals(MDC.get("user-info")));
                    ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                    builder2.setParCallable(ParCallable.from(() -> {
                        assertTrue("789".equals(MDC.get("request-id")));
                        assertTrue("user-3".equals(MDC.get("user-info")));
                        return new Right<GeneralError, Integer>(input + 100);
                    }));
                    return builder2.build();
                }));
        MDC.remove("request-id");
        MDC.remove("user-info");
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }

    @Test
    public void testMDCFlatMapSuccessFn1() {
        MDC.put("request-id", "789");
        MDC.put("user-info", "user-3");
        ParRequest.Builder<Integer> builder = new ParRequest.Builder<>(executor);
        builder.setParCallable(ParCallable.fromScala(fn1((nothing) -> {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
            }
            assertTrue("789".equals(MDC.get("request-id")));
            assertTrue("user-3".equals(MDC.get("user-info")));
            return new Right<GeneralError, Integer>(100);
        })));

        ParRequest<Integer> request = builder.build();
        ParRequest<Integer>
                composedRequest =
                request.flatMap("testMDCFlatMapSuccess", ParFunction.fromScala(fn1((input) -> {
                    assertTrue("789".equals(MDC.get("request-id")));
                    assertTrue("user-3".equals(MDC.get("user-info")));
                    ParRequest.Builder<Integer> builder2 = new ParRequest.Builder<>(executor);
                    builder2.setParCallable(ParCallable.from(() -> {
                        assertTrue("789".equals(MDC.get("request-id")));
                        assertTrue("user-3".equals(MDC.get("user-info")));
                        return new Right<GeneralError, Integer>(input + 100);
                    }));
                    return builder2.build();
                })));
        MDC.remove("request-id");
        MDC.remove("user-info");
        Either<GeneralError, Integer> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get() == 200);

    }
}
