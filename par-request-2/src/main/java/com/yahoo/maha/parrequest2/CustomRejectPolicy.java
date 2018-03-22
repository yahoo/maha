// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by jians on 8/6/15.
 */
public class CustomRejectPolicy implements RejectedExecutionHandler {

    public CustomRejectPolicy() {
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (r instanceof AbstractFuture) {
            if (!executor.isShutdown()) {
                r.run();
            } else {
                throw new RejectedExecutionException(String.format("Task %s get rejected, executor %s is shutdown ", r.toString(), executor.toString()));
            }
        } else if (r instanceof ListenableFutureTask) {
            throw new RejectedExecutionException("Task " + r.toString() +
                                                 " rejected from " +
                                                 executor.toString());
        }
    }
}
