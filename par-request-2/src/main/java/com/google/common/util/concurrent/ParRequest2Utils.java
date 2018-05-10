// Copyright 2018, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.google.common.util.concurrent;

/**
 * Created by hiral on 5/10/18.
 */
final public class ParRequest2Utils {
    public static boolean isTrustedListenableFutureTask(Object o) {
        return o instanceof TrustedListenableFutureTask;
    }
}
