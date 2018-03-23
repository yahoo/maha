// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest.future;

import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.maha.parrequest.Either;
import com.yahoo.maha.parrequest.GeneralError;

/**
 * Base class for combinable requests, a future needs to be exposed for combining
 */
public abstract class CombinableRequest<T> {

    protected String label;

    abstract ListenableFuture<Either<GeneralError, T>> asFuture();
}
