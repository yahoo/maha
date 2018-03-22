// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

/**
 * Created by hiral on 3/20/14.
 */
final public class Nothing {

    private static final Nothing NOTHING = new Nothing();

    public static Nothing get() {
        return NOTHING;
    }

    private Nothing() {
    }
}
