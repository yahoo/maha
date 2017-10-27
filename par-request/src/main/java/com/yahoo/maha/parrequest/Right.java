// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

/**
 * Created by hiral on 5/21/15.
 */
public class Right<E, A> extends Either<E, A> {

    public final A a;

    public Right(A a) {
        //Preconditions.checkState(a != null, "Cannot set null value with right!");
        this.a = a;
    }

    @Override
    public boolean isRight() {
        return true;
    }

    @Override
    public boolean isLeft() {
        return false;
    }

    public A get() {
        return a;
    }

    @Override
    public String toString() {
        return String.format("Right(%s)", a);
    }
}
