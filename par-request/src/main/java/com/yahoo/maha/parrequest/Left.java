// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest;

/**
 * Created by hiral on 5/21/15.
 */
public class Left<E, A> extends Either<E, A> {

    public final E e;

    public Left(E e) {
        //Preconditions.checkState(e != null, "Cannot set null value with left!");
        this.e = e;
    }

    @Override
    public boolean isRight() {
        return false;
    }

    @Override
    public boolean isLeft() {
        return true;
    }

    public E get() {
        return e;
    }

    @Override
    public String toString() {
        return String.format("Left(%s)", e);
    }
}
