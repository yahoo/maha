// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import java.util.function.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.maha.parrequest2.CustomRejectPolicy;
import com.yahoo.maha.parrequest2.ParCallable;
import scala.util.Either;
import scala.util.Right;
import com.yahoo.maha.parrequest2.GeneralError;
import scala.Option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class ParallelServiceExecutor {

    private int threadPoolSize = 100;

    private int defaultTimeoutMillis = 10000;

    private String poolName = "pse";

    private int queueSize = 100;

    private RejectedExecutionHandler rejectedExecutionHandler = new CustomRejectPolicy();

    private static Logger logger = LoggerFactory.getLogger(ParallelServiceExecutor.class);

    private ListeningExecutorService threadService;

    public void setRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
        Preconditions.checkArgument(rejectedExecutionHandler != null, "rejectedExecutionHandler cannot be null");
        this.rejectedExecutionHandler = rejectedExecutionHandler;
    }

    public void setPoolName(String poolName) {
        Preconditions.checkArgument(poolName != null, "Pool name cannot be null");
        this.poolName = poolName;
    }

    public void setDefaultTimeoutMillis(int defaultTimeoutMillis) {
        this.defaultTimeoutMillis = defaultTimeoutMillis;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        Preconditions.checkArgument(threadPoolSize > 1, "Pool size must be > 1");
        this.threadPoolSize = threadPoolSize;
    }

    public void setQueueSize(int queueSize) {
        Preconditions.checkArgument(queueSize > 1, "Queue size must be > 1");
        this.queueSize = queueSize;
    }

    public int getThreadPoolSize() {
        return this.threadPoolSize;
    }

    public void init() throws Exception {
        ExecutorService executorService = new ThreadPoolExecutor(this.threadPoolSize, this.threadPoolSize,
                                                                 0L, TimeUnit.MILLISECONDS,
                                                                 new LinkedBlockingQueue<Runnable>(queueSize),
                                                                 new ThreadFactoryBuilder()
                                                                     .setNameFormat(poolName + "-%d").build(),
                                                                 rejectedExecutionHandler
        );
        threadService = MoreExecutors.listeningDecorator(executorService);
    }

    public void destroy() throws Exception {
        threadService.shutdown();
    }

    public <T> List<T> execute(List<ParCallable<T>> callables) throws Exception {
        return execute(callables, defaultTimeoutMillis);
    }

    public <T> List<T> execute(List<ParCallable<T>> callables, int timeoutMillis) throws Exception {
        return execute(callables, timeoutMillis, true, null);
    }

    public <T> List<T> execute(List<ParCallable<T>> callables, Function<Throwable, T> generateDefaultValue)
        throws Exception {
        return execute(callables, defaultTimeoutMillis, false, generateDefaultValue);
    }

    public <T> List<T> execute(List<ParCallable<T>> callables, boolean failFast,
                               Function<Throwable, T> generateDefaultValue) throws Exception {
        return execute(callables, defaultTimeoutMillis, failFast, generateDefaultValue);
    }

    public <T> List<T> execute(List<ParCallable<T>> callables, int timeoutMillis, boolean failFast,
                               Function<Throwable, T> generateDefaultValue) throws Exception {
        List<Future<T>> futureResultList = null;
        List<T> resultList = new ArrayList<T>();

        try {
            if (callables != null && !callables.isEmpty()) {
                futureResultList = threadService.invokeAll(callables, timeoutMillis, TimeUnit.MILLISECONDS);
            }

            if (futureResultList != null && !futureResultList.isEmpty()) {
                for (Future<T> future : futureResultList) {
                    try {
                        T output = future.get();
                        if (output != null) {
                            resultList.add(output);
                        } else {
                            logger.warn("The result of a future was null!");
                        }
                        logger.info("is future done: " + future.isDone() + " is cancelled: " + future.isCancelled());
                        logger.info("future get response: " + future.get());
                    } catch (CancellationException | ExecutionException | InterruptedException e) {
                        if (failFast) {
                            throw e;
                        } else {
                            logger.warn("CancellationException/Interrupted/Execution exception: ", e);
                            if (generateDefaultValue != null) {
                                resultList.add(generateDefaultValue.apply(e));
                            }
                        }
                    }
                }
            } else {
                logger.error("futureResultList is empty");
            }
            return resultList;
        } catch (Exception e) {
            logger.error("Execution exception: ", e);
            throw new Exception("UNKNOWN_SERVER_ERROR", e);
        }
    }

    @Deprecated
    public <T> Either<GeneralError, T> produceResult(ParCallable<Either<GeneralError, T>> callable) {
        try {
            ListenableFuture<Either<GeneralError, T>> results = threadService.submit(callable);
            return results.get();
        } catch (Exception e) {
            logger.error("Parallel execution error", e);
            return GeneralError.either("produceResult", "execution failed", e);
        }
    }

    @Deprecated
    public <T> ListenableFuture<Either<GeneralError, T>> asyncProduceResult(
        ParCallable<Either<GeneralError, T>> callable) {
        try {
            ListenableFuture<Either<GeneralError, T>> results = threadService.submit(callable);
            return results;
        } catch (Exception e) {
            logger.error("Parallel execution error", e);
            return Futures.<Either<GeneralError, T>>immediateFuture(
                    GeneralError.<T>either("asyncProduceResult", "execution failed", e));
        }
    }

    <T> ListenableFuture<Either<GeneralError, T>> submitParCallable(ParCallable<Either<GeneralError, T>> callable) {
        try {
            ListenableFuture<Either<GeneralError, T>> results = threadService.submit(callable);
            return results;
        } catch (Exception e) {
            logger.error("Parallel execution error", e);
            return Futures.<Either<GeneralError, T>>immediateFuture(
                    GeneralError.<T>either("asyncProduceResult", "execution failed", e));
        }
    }

    /**
     * Add a listener to given future which executes the given runnable, backed by internal executor
     */
    public <T> void addListener(ListenableFuture<T> future, Runnable runnable) {
        future.addListener(runnable, threadService);
    }

    /**
     * Create a builder for ParRequest backed by internal executor
     */
    public <T> ParRequest.Builder<T> parRequestBuilder() {
        return new ParRequest.Builder<T>(this);
    }

    /**
     * Create a builder for ParRequest2 backed by internal executor
     */
    public <T, U> ParRequest2.Builder<T, U> parRequest2Builder() {
        return new ParRequest2.Builder<T, U>(this);
    }

    /**
     * Create a builder for ParRequest2 backed by internal executor
     */
    public <T, U> ParRequest2Option.Builder<T, U> parRequest2OptionBuilder() {
        return new ParRequest2Option.Builder<T, U>(this);
    }

    /**
     * Create a builder for ParRequest3 backed by internal executor
     */
    public <T, U, V> ParRequest3.Builder<T, U, V> parRequest3Builder() {
        return new ParRequest3.Builder<T, U, V>(this);
    }

    /**
     * Create a builder for ParRequest3Option backed by internal executor
     */
    public <T, U, V> ParRequest3Option.Builder<T, U, V> parRequest3OptionBuilder() {
        return new ParRequest3Option.Builder<T, U, V>(this);
    }


    /**
     * Create a builder for ParRequest4 backed by internal executor
     */
    public <T, U, V, W> ParRequest4.Builder<T, U, V, W> parRequest4Builder() {
        return new ParRequest4.Builder<T, U, V, W>(this);
    }

    /**
     * Create a builder for ParRequest5 backed by internal executor
     */
    public <T, U, V, W, X> ParRequest5.Builder<T, U, V, W, X> parRequest5Builder() {
        return new ParRequest5.Builder<T, U, V, W, X>(this);
    }

    /**
     * Create a builder for ParRequest6 backed by internal executor
     */
    public <T, U, V, W, X, Y> ParRequest6.Builder<T, U, V, W, X, Y> parRequest6Builder() {
        return new ParRequest6.Builder<T, U, V, W, X, Y>(this);
    }

    public <T> ParRequestListOption.Builder<T> parRequestListOptionBuilder() {
        return new ParRequestListOption.Builder<T>(this);
    }

    public <T> ParRequestListEither.Builder<T> parRequestListEitherBuilder() {
        return new ParRequestListEither.Builder<T>(this);
    }

    /**
     * Combine list of combinable requests, meaning the result ParRequestListOption will finish when all combinable requests in the list finish
     */
    public <T> ParRequestListOption<T> combineList(
            final List<CombinableRequest<T>> requestList) {
        return combineList(requestList, false);
    }

    /**
     * Combine list of combinable requests, meaning the result ParRequestListOption will finish when all combinable requests in the list finish
     */
    public <T> ParRequestListOption<T> combineList(
            final List<CombinableRequest<T>> requestList, boolean allMustSucceed) {
        String joinedLabel = requestList.stream().map(x -> x.label).collect(Collectors.joining("-"));
        return new ParRequestListOption<T>(joinedLabel, this, new ArrayList<CombinableRequest<T>>(requestList), allMustSucceed);
    }

    /**
     * Combine list of combinable requests, meaning the result ParRequestListEither will finish when all combinable requests in the list finish
     */
    public <T> ParRequestListEither<T> combineListEither(
            final List<CombinableRequest<T>> requestList) {
        return combineListEither(requestList, false);
    }

    /**
     * Combine list of combinable requests, meaning the result ParRequestListOption will finish when all combinable requests in the list finish
     */
    public <T> ParRequestListEither<T> combineListEither(
            final List<CombinableRequest<T>> requestList, boolean allMustSucceed) {
        String joinedLabel = requestList.stream().map(x -> x.label).collect(Collectors.joining("-"));
        return new ParRequestListEither<T>(joinedLabel, this, new ArrayList<CombinableRequest<T>>(requestList), allMustSucceed);
    }

    /**
     * Combine 2 combinable requests, meaning the result ParRequest2 will finish when the 2 combinable requests finish
     */
    public <T, U> ParRequest2<T, U> combine2(
        final CombinableRequest<T> firstRequest,
        final CombinableRequest<U> secondRequest) {
        return new ParRequest2<T, U>(firstRequest.label + secondRequest.label, this, firstRequest, secondRequest);
    }

    /**
     * Combine 2 combinable requests meaning the result ParRequest2Option will finish when the 2 combinable requests
     * finish.  The result is optionally available, meaning available if future completed without error.
     */
    public <T, U> ParRequest2Option<T, U> optionalCombine2(
        final CombinableRequest<T> firstRequest,
        final CombinableRequest<U> secondRequest) {
        return new ParRequest2Option<T, U>(firstRequest.label + secondRequest.label, this, firstRequest, secondRequest,
                                           false);
    }

    /**
     * Combine 3 combinable requests, meaning the result ParRequest3 will finish when the 3 combinable requests finish
     */
    public <T, U, V> ParRequest3<T, U, V> combine3(
        final CombinableRequest<T> firstRequest,
        final CombinableRequest<U> secondRequest,
        final CombinableRequest<V> thirdRequest) {
        return new ParRequest3<T, U, V>(firstRequest.label + secondRequest.label + thirdRequest.label, this,
                                        firstRequest, secondRequest, thirdRequest);
    }

    /**
     * Combine 3 combinable requests meaning the result ParRequest3Option will finish when the 3 combinable requests
     * finish.  The result is optionally available, meaning available if future completed without error.
     */
    public <T, U, V> ParRequest3Option<T, U, V> optionalCombine3(
        final CombinableRequest<T> firstRequest,
        final CombinableRequest<U> secondRequest,
        final CombinableRequest<V> thirdRequest) {
        return new ParRequest3Option<T, U, V>(firstRequest.label + secondRequest.label + thirdRequest.label, this,
                                              firstRequest, secondRequest, thirdRequest, false);
    }

    /**
     * Combine 4 combinable requests, meaning the result ParRequest4 will finish when the 4 combinable requests finish
     */
    public <T, U, V, W> ParRequest4<T, U, V, W> combine4(
            final CombinableRequest<T> firstRequest,
            final CombinableRequest<U> secondRequest,
            final CombinableRequest<V> thirdRequest,
            final CombinableRequest<W> fourthRequest) {
        return new ParRequest4<T, U, V, W>(firstRequest.label + secondRequest.label + thirdRequest.label + fourthRequest.label, this,
                firstRequest, secondRequest, thirdRequest, fourthRequest);
    }


    /**
     * Combine 5 combinable requests, meaning the result ParRequest4 will finish when the 5 combinable requests finish
     */
    public <T, U, V, W, X> ParRequest5<T, U, V, W, X> combine5(
            final CombinableRequest<T> firstRequest,
            final CombinableRequest<U> secondRequest,
            final CombinableRequest<V> thirdRequest,
            final CombinableRequest<W> fourthRequest,
            final CombinableRequest<X> fifthRequest) {
        return new ParRequest5<T, U, V, W, X>(firstRequest.label + secondRequest.label + thirdRequest.label + fourthRequest.label + fifthRequest.label, this,
                firstRequest, secondRequest, thirdRequest, fourthRequest, fifthRequest);
    }


    /**
     * Combine 6 combinable requests, meaning the result ParRequest6 will finish when the 6 combinable requests finish
     */
    public <T, U, V, W, X, Y> ParRequest6<T, U, V, W, X, Y> combine6(
            final CombinableRequest<T> firstRequest,
            final CombinableRequest<U> secondRequest,
            final CombinableRequest<V> thirdRequest,
            final CombinableRequest<W> fourthRequest,
            final CombinableRequest<X> fifthRequest,
            final CombinableRequest<Y> sixthRequest) {
        return new ParRequest6<T, U, V, W, X, Y>(firstRequest.label + secondRequest.label + thirdRequest.label + fourthRequest.label + fifthRequest.label + sixthRequest.label, this,
                firstRequest, secondRequest, thirdRequest, fourthRequest, fifthRequest, sixthRequest);
    }

    /**
     * Given a future which is holding an Either<GeneralError, T>, block on result and return an Either<GeneralError,T>.
     * All expected exceptions are caught and returns as GeneralError inside an Either
     */
    public <T> Either<GeneralError, T> getEitherSafely(String label, ListenableFuture<Either<GeneralError, T>> future,
                                                       long timeoutMillis) {
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (CancellationException e) {
            return GeneralError
                .<T>either("getEitherSafely", String.format("%s request cancelled : %s", label, e.getMessage()), e);
        } catch (TimeoutException e) {
            return GeneralError
                .<T>either("getEitherSafely", String.format("%s request timeout : %s", label, e.getMessage()), e);
        } catch (ExecutionException e) {
            return GeneralError
                .<T>either("getEitherSafely", String.format("%s execution failed : %s", label, e.getMessage()), e);
        } catch (InterruptedException e) {
            return GeneralError
                .<T>either("getEitherSafely", String.format("%s execution interrupted : %s", label, e.getMessage()), e);
        }
    }

    /**
     * Given a future which is holding an Either<GeneralError, T>, block on result and return an Either<GeneralError,T>.
     * All expected exceptions are caught and returns as GeneralError inside an Either
     */
    public <T> Either<GeneralError, T> getEitherSafely(String label, ListenableFuture<Either<GeneralError, T>> future) {
        return getEitherSafely(label, future, defaultTimeoutMillis);
    }

    public <T> Option<Either<GeneralError, T>> getSafely(String label,
                                                         Option<ListenableFuture<Either<GeneralError, T>>> optionalFuture) {
        if (optionalFuture.isDefined()) {
            return Option.apply(getEitherSafely(label, optionalFuture.get()));
        } else {
            return Option.empty();
        }
    }

    /**
     * Given a future which is holding a type T, block on result and return an Either<GeneralError, T>. All expected
     * exceptions are caught and returns as GeneralError inside an Either
     */
    public <T> Either<GeneralError, T> getSafely(String label, ListenableFuture<T> future, long timeoutMillis) {
        try {
            return new Right<>(future.get(timeoutMillis, TimeUnit.MILLISECONDS));
        } catch (CancellationException e) {
            return GeneralError.<T>either("getSafely", label + " request cancelled", e);
        } catch (TimeoutException e) {
            return GeneralError.<T>either("getSafely", label + " request timeout", e);
        } catch (ExecutionException e) {
            return GeneralError.<T>either("getSafely", label + " execution failed", e);
        } catch (InterruptedException e) {
            return GeneralError.<T>either("getSafely", label + " execution interrupted", e);
        }
    }

    /**
     * Given a future which is holding a type T, block on result and return an Either<GeneralError, T>. All expected
     * exceptions are caught and returns as GeneralError inside an Either
     */
    public <T> Either<GeneralError, T> getSafely(String label, ListenableFuture<T> future) {
        return getSafely(label, future, defaultTimeoutMillis);
    }
}
