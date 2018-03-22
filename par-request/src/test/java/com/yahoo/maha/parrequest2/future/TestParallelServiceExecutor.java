// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest.future;

import com.yahoo.maha.parrequest.Either;
import com.yahoo.maha.parrequest.Right;
import com.yahoo.maha.parrequest.GeneralError;
import com.yahoo.maha.parrequest.Nothing;
import com.yahoo.maha.parrequest.Option;
import com.yahoo.maha.parrequest.ParCallable;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import scala.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertTrue;

/**
 * Created by hiral on 6/17/14.
 */
public class TestParallelServiceExecutor {

    private ParallelServiceExecutor executor;

    @BeforeClass
    public void setUp() throws Exception {
        executor = new ParallelServiceExecutor();
        executor.setDefaultTimeoutMillis(1000);
        executor.setPoolName("test-pse");
        executor.setQueueSize(40);
        executor.setThreadPoolSize(40);
        executor.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        executor.destroy();
    }

    @Test
    public void testPoolSize() {
        assertTrue(executor.getThreadPoolSize() == 40, "Invalid pool size");
    }

    @Test
    public void testLongRunningParRequest() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest<Integer> request = builder.build();
        Either<GeneralError, Integer> result = request.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("timeout"));
    }

    @Test
    public void testLongRunningParRequest2() throws Exception {
        ParRequest2.Builder<Integer, Integer> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest2<Integer, Integer> request = builder.build();
        Either<GeneralError, Tuple2<Integer, Integer>> result = request.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("timeout"));
    }

    @Test
    public void testLongRunningParRequest3() throws Exception {
        ParRequest3.Builder<Integer, Integer, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest3<Integer, Integer, Integer> request = builder.build();
        Either<GeneralError, Tuple3<Integer, Integer, Integer>> result = request.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("timeout"));
    }

    @Test
    public void testLongRunningParRequest4() throws Exception {
        ParRequest4.Builder<Integer, Integer, Integer, Integer> builder = executor.parRequest4Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest4<Integer, Integer, Integer, Integer> request = builder.build();
        Either<GeneralError, Tuple4<Integer, Integer, Integer, Integer>> result = request.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("timeout"));
    }


    @Test
    public void testCanceledParRequest() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest<Integer> request = builder.build();
        request.asFuture().cancel(false);
        Either<GeneralError, Integer> result = request.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("cancel"));
    }

    @Test
    public void testCanceledParRequest2() throws Exception {
        ParRequest2.Builder<Integer, Integer> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest2<Integer, Integer> request = builder.build();
        request.asFuture().cancel(false);
        Either<GeneralError, Tuple2<Integer, Integer>> result = request.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("cancel"));
    }

    @Test
    public void testCanceledParRequest3() throws Exception {
        ParRequest3.Builder<Integer, Integer, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest3<Integer, Integer, Integer> request = builder.build();
        request.asFuture().cancel(false);
        Either<GeneralError, Tuple3<Integer, Integer, Integer>> result = request.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("cancel"));
    }

    @Test
    public void testCanceledParRequest4() throws Exception {
        ParRequest4.Builder<Integer, Integer, Integer, Integer> builder = executor.parRequest4Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest4<Integer, Integer, Integer, Integer> request = builder.build();
        request.asFuture().cancel(false);
        Either<GeneralError, Tuple4<Integer, Integer, Integer, Integer>> result = request.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("cancel"));
    }

    @Test
    public void testCanceledParRequest5() throws Exception {
        ParRequest5.Builder<Integer, Integer, Integer, Integer, Integer> builder = executor.parRequest5Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest5<Integer, Integer, Integer, Integer, Integer> request = builder.build();
        request.asFuture().cancel(false);
        Either<GeneralError, Tuple5<Integer, Integer, Integer, Integer, Integer>> result = request.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("cancel"));
    }

    @Test
    public void testCanceledParRequest6() throws Exception {
        ParRequest6.Builder<Integer, Integer, Integer, Integer, Integer, Integer> builder = executor.parRequest6Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setFourthParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setFifthParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSixthParCallable(ParCallable.from(() -> {
            Thread.sleep(2000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest6<Integer, Integer, Integer, Integer, Integer, Integer> request = builder.build();
        request.asFuture().cancel(false);
        Either<GeneralError, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> result = request.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("cancel"));
    }


    @Test
    public void testCanceledNoopRequest() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest<Integer> request = builder.build();
        NoopRequest<Nothing> noopRequest = request.fold(ParFunction.from((input) -> {
            return Nothing.get();
        }), ParFunction.from((input) -> {
            return Nothing.get();
        }));
        noopRequest.asFuture().cancel(true);
        Either<GeneralError, Nothing> result = noopRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("cancel"));
    }

    @Test
    public void testCanceledParRequestFromFlatMap() throws Exception {
        ParRequest3.Builder<Integer, Integer, Integer> builder = executor.parRequest3Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(1000);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest3<Integer, Integer, Integer> request = builder.build();
        ParRequest<String>
                flatMapRequest =
                request.map("testCanceledParRequestFromFlatMap",
                        ParFunction.from((input) -> {
                            return new Right<GeneralError, String>(
                                    String.format("%s-%s-%s", input._1(), input._2(), input._3()));
                        }));
        flatMapRequest.asFuture().cancel(true);
        Either<GeneralError, String> result = flatMapRequest.get();
        assertTrue(result.isLeft());
        assertTrue(result.left().get().toString().toLowerCase().contains("cancel"));
    }

    @Test
    public void testCombine2() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();
        ParRequest2<Integer, Integer> combined = executor.combine2(request, request2);
        Either<GeneralError, Tuple2<Integer, Integer>> result = combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1() == 10);
        assertTrue(result.right().get()._2() == 11);
    }

    class TestResponse {

        public String err = "";
        public String success = "";
    }

    @Test
    public void testCombineList() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();

        List<CombinableRequest<Integer>> parRequestList = new ArrayList<>();
        parRequestList.add(request);
        parRequestList.add(request2);

        ParRequestListOption<Integer> parRequestListOption = executor.combineList(parRequestList);
        final TestResponse response = new TestResponse();
        NoopRequest<String> composedRequest = parRequestListOption.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return response.err;
        }), ParFunction.from((input) -> {
            response.success = String.join("-", input.stream().map((opt) -> {
                if (opt.isEmpty()) {
                    return "Empty";
                } else {
                    return opt.get().toString();
                }
            }).collect(Collectors.toList()));
            return response.success;
        }));
        Either<GeneralError, String> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("10-11"));
    }


    @Test
    public void testCombineListWithFailure() {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            throw new IllegalArgumentException("failed");
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        List<CombinableRequest<Integer>> parRequestList = new ArrayList<>();
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();
        parRequestList.add(request);
        parRequestList.add(request2);

        ParRequestListOption<Integer> parRequestListOption = executor.combineList(parRequestList);
        final TestResponse response = new TestResponse();
        NoopRequest<String> composedRequest = parRequestListOption.fold(ParFunction.from((input) -> {
            response.err = input.toString();
            return response.err;
        }), ParFunction.from((input) -> {
            response.success = String.join("-", input.stream().map((opt) -> {
                if (opt.isEmpty()) {
                    return "Empty";
                } else {
                    return opt.get().toString();
                }
            }).collect(Collectors.toList()));
            return response.success;
        }));
        Either<GeneralError, String> result = composedRequest.get();
        assertTrue(result.isRight());
        assertTrue(response.success.equals("Empty-11"));
    }

    @Test
    public void testCombine2FromPar2Par3() throws Exception {
        ParRequest2.Builder<Integer, Integer> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest3.Builder<String, String, String> builder2 = executor.parRequest3Builder();
        builder2.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("100");
        }));
        builder2.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("101");
        }));
        builder2.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("102");
        }));
        ParRequest2<Integer, Integer> request = builder.build();
        ParRequest3<String, String, String> request2 = builder2.build();
        ParRequest2<Tuple2<Integer, Integer>, Tuple3<String, String, String>>
                combined =
                executor.combine2(request, request2);
        Either<GeneralError, Tuple2<Tuple2<Integer, Integer>, Tuple3<String, String, String>>> result = combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1()._1() == 10);
        assertTrue(result.right().get()._1()._2() == 11);
        assertTrue(result.right().get()._2()._1().equals("100"));
        assertTrue(result.right().get()._2()._2().equals("101"));
        assertTrue(result.right().get()._2()._3().equals("102"));
    }

    @Test
    public void testCombine3() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest.Builder<Integer> builder3 = executor.parRequestBuilder();
        builder3.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(12);
        }));
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();
        ParRequest<Integer> request3 = builder3.build();
        ParRequest3<Integer, Integer, Integer> combined = executor.combine3(request, request2, request3);
        Either<GeneralError, Tuple3<Integer, Integer, Integer>> result = combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1() == 10);
        assertTrue(result.right().get()._2() == 11);
        assertTrue(result.right().get()._3() == 12);
    }

    @Test
    public void testCombine4() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest.Builder<Integer> builder3 = executor.parRequestBuilder();
        builder3.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(12);
        }));
        ParRequest.Builder<Integer> builder4 = executor.parRequestBuilder();
        builder4.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(13);
        }));
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();
        ParRequest<Integer> request3 = builder3.build();
        ParRequest<Integer> request4 = builder4.build();
        ParRequest4<Integer, Integer, Integer, Integer> combined = executor.combine4(request, request2, request3, request4);
        Either<GeneralError, Tuple4<Integer, Integer, Integer, Integer>> result = combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1() == 10);
        assertTrue(result.right().get()._2() == 11);
        assertTrue(result.right().get()._3() == 12);
        assertTrue(result.right().get()._4() == 13);
    }

    @Test
    public void testCombine5() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest.Builder<Integer> builder3 = executor.parRequestBuilder();
        builder3.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(12);
        }));
        ParRequest.Builder<Integer> builder4 = executor.parRequestBuilder();
        builder4.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(13);
        }));
        ParRequest.Builder<Integer> builder5 = executor.parRequestBuilder();
        builder5.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(14);
        }));
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();
        ParRequest<Integer> request3 = builder3.build();
        ParRequest<Integer> request4 = builder4.build();
        ParRequest<Integer> request5 = builder5.build();
        ParRequest5<Integer, Integer, Integer, Integer, Integer> combined = executor.combine5(request, request2, request3, request4, request5);
        Either<GeneralError, Tuple5<Integer, Integer, Integer, Integer, Integer>> result = combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1() == 10);
        assertTrue(result.right().get()._2() == 11);
        assertTrue(result.right().get()._3() == 12);
        assertTrue(result.right().get()._4() == 13);
        assertTrue(result.right().get()._5() == 14);
    }

    @Test
    public void testCombine6() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest.Builder<Integer> builder3 = executor.parRequestBuilder();
        builder3.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(12);
        }));
        ParRequest.Builder<Integer> builder4 = executor.parRequestBuilder();
        builder4.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(13);
        }));
        ParRequest.Builder<Integer> builder5 = executor.parRequestBuilder();
        builder5.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(14);
        }));
        ParRequest.Builder<Integer> builder6 = executor.parRequestBuilder();
        builder6.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(15);
        }));
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();
        ParRequest<Integer> request3 = builder3.build();
        ParRequest<Integer> request4 = builder4.build();
        ParRequest<Integer> request5 = builder5.build();
        ParRequest<Integer> request6 = builder6.build();
        ParRequest6<Integer, Integer, Integer, Integer, Integer, Integer> combined = executor.combine6(request, request2, request3, request4, request5, request6);
        Either<GeneralError, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> result = combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1() == 10);
        assertTrue(result.right().get()._2() == 11);
        assertTrue(result.right().get()._3() == 12);
        assertTrue(result.right().get()._4() == 13);
        assertTrue(result.right().get()._5() == 14);
        assertTrue(result.right().get()._6() == 15);
    }

    @Test
    public void testCombine3FromPar1Par2Par3() throws Exception {
        ParRequest2.Builder<Integer, Integer> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest3.Builder<String, String, String> builder2 = executor.parRequest3Builder();
        builder2.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("100");
        }));
        builder2.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("101");
        }));
        builder2.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("102");
        }));
        ParRequest.Builder<Integer> builder3 = executor.parRequestBuilder();
        builder3.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest2<Integer, Integer> request = builder.build();
        ParRequest3<String, String, String> request2 = builder2.build();
        ParRequest<Integer> request3 = builder3.build();

        ParRequest3<Tuple2<Integer, Integer>, Tuple3<String, String, String>, Integer>
                combined =
                executor.combine3(request, request2, request3);
        Either<GeneralError, Tuple3<Tuple2<Integer, Integer>, Tuple3<String, String, String>, Integer>>
                result =
                combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1()._1() == 10);
        assertTrue(result.right().get()._1()._2() == 11);
        assertTrue(result.right().get()._2()._1().equals("100"));
        assertTrue(result.right().get()._2()._2().equals("101"));
        assertTrue(result.right().get()._2()._3().equals("102"));
        assertTrue(result.right().get()._3() == 10);
    }

    @Test
    public void testOptionalCombine2() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();
        ParRequest2Option<Integer, Integer> combined = executor.optionalCombine2(request, request2);
        Either<GeneralError, Tuple2<Option<Integer>, Option<Integer>>> result = combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1().get() == 10);
        assertTrue(result.right().get()._2().get() == 11);
    }

    @Test
    public void testOptionalCombine2FromPar2Par3() throws Exception {
        ParRequest2.Builder<Integer, Integer> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest3.Builder<String, String, String> builder2 = executor.parRequest3Builder();
        builder2.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("100");
        }));
        builder2.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("101");
        }));
        builder2.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("102");
        }));
        ParRequest2<Integer, Integer> request = builder.build();
        ParRequest3<String, String, String> request2 = builder2.build();
        ParRequest2Option<Tuple2<Integer, Integer>, Tuple3<String, String, String>>
                combined =
                executor.optionalCombine2(request, request2);
        Either<GeneralError, Tuple2<Option<Tuple2<Integer, Integer>>, Option<Tuple3<String, String, String>>>>
                result =
                combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1().get()._1() == 10);
        assertTrue(result.right().get()._1().get()._2() == 11);
        assertTrue(result.right().get()._2().get()._1().equals("100"));
        assertTrue(result.right().get()._2().get()._2().equals("101"));
        assertTrue(result.right().get()._2().get()._3().equals("102"));
    }

    @Test
    public void testOptionalCombine2WithFailure() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            throw new IllegalArgumentException("failed");
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();
        ParRequest2Option<Integer, Integer> combined = executor.optionalCombine2(request, request2);
        Either<GeneralError, Tuple2<Option<Integer>, Option<Integer>>> result = combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1().isEmpty());
        assertTrue(result.right().get()._2().get() == 11);
    }

    @Test
    public void testOptionalCombine3() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest.Builder<Integer> builder3 = executor.parRequestBuilder();
        builder3.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(12);
        }));
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();
        ParRequest<Integer> request3 = builder3.build();
        ParRequest3Option<Integer, Integer, Integer> combined = executor.optionalCombine3(request, request2, request3);
        Either<GeneralError, Tuple3<Option<Integer>, Option<Integer>, Option<Integer>>> result = combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1().get() == 10);
        assertTrue(result.right().get()._2().get() == 11);
        assertTrue(result.right().get()._3().get() == 12);
    }

    @Test
    public void testOptionalCombine3FromPar1Par2Par3() throws Exception {
        ParRequest2.Builder<Integer, Integer> builder = executor.parRequest2Builder();
        builder.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        builder.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(11);
        }));
        ParRequest3.Builder<String, String, String> builder2 = executor.parRequest3Builder();
        builder2.setFirstParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("100");
        }));
        builder2.setSecondParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("101");
        }));
        builder2.setThirdParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, String>("102");
        }));
        ParRequest.Builder<Integer> builder3 = executor.parRequestBuilder();
        builder3.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest2<Integer, Integer> request = builder.build();
        ParRequest3<String, String, String> request2 = builder2.build();
        ParRequest<Integer> request3 = builder3.build();

        ParRequest3Option<Tuple2<Integer, Integer>, Tuple3<String, String, String>, Integer> combined =
                executor.optionalCombine3(request, request2, request3);
        Either<GeneralError, Tuple3<Option<Tuple2<Integer, Integer>>, Option<Tuple3<String, String, String>>, Option<Integer>>>
                result =
                combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1().get()._1() == 10);
        assertTrue(result.right().get()._1().get()._2() == 11);
        assertTrue(result.right().get()._2().get()._1().equals("100"));
        assertTrue(result.right().get()._2().get()._2().equals("101"));
        assertTrue(result.right().get()._2().get()._3().equals("102"));
        assertTrue(result.right().get()._3().get() == 10);
    }

    @Test
    public void testOptionalCombine3WithFailure() throws Exception {
        ParRequest.Builder<Integer> builder = executor.parRequestBuilder();
        builder.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(10);
        }));
        ParRequest.Builder<Integer> builder2 = executor.parRequestBuilder();
        builder2.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            throw new IllegalArgumentException("failed");
        }));
        ParRequest.Builder<Integer> builder3 = executor.parRequestBuilder();
        builder3.setParCallable(ParCallable.from(() -> {
            Thread.sleep(300);
            return new Right<GeneralError, Integer>(12);
        }));
        ParRequest<Integer> request = builder.build();
        ParRequest<Integer> request2 = builder2.build();
        ParRequest<Integer> request3 = builder3.build();
        ParRequest3Option<Integer, Integer, Integer> combined = executor.optionalCombine3(request, request2, request3);
        Either<GeneralError, Tuple3<Option<Integer>, Option<Integer>, Option<Integer>>> result = combined.get();
        assertTrue(result.isRight());
        assertTrue(result.right().get()._1().get() == 10);
        assertTrue(result.right().get()._2().isEmpty());
        assertTrue(result.right().get()._3().get() == 12);
    }
}
