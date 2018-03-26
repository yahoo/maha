// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.util.Either;
import scala.util.Left;

/**
 * hiral, srikalyan
 */
public class GeneralError {

    private static Logger logger = LoggerFactory.getLogger(GeneralError.class);
    public final String stage;
    public final String message;
    public final Option<? extends Throwable> throwableOption;

    public GeneralError(String stage, String message, Option<? extends Throwable> throwableOption) {
        this.stage = stage;
        this.message = message;
        this.throwableOption = throwableOption;
        logger.debug("Stage {}, Message {}", stage, message);
        logger.debug("StackTrace {}", throwableOption);
    }

    public GeneralError prependStage(String s) {
        return new GeneralError(String.format("%s :: %s", s, this.stage), this.message, this.throwableOption);
    }

    @Override
    public String toString() {
        return "GeneralError{" +
               "stage='" + stage + '\'' +
               ", message='" + message + '\'' +
               ", throwableOption=" + throwableOption +
               '}';
    }

    public static GeneralError from(String stage, String message) {
        return new GeneralError(stage, message, Option.<Throwable>empty());
    }

    public static GeneralError from(String stage, String message, Throwable t) {
        return new GeneralError(stage, message, Option.apply(t));
    }

    public static <T> Either<GeneralError, T> either(String stage, String message) {
        return new Left<GeneralError, T>(from(stage, message));
    }

    public static <T> Either<GeneralError, T> either(String stage, String message, Throwable t) {
        return new Left<GeneralError, T>(from(stage, message, t));
    }

    public static <T> Either<GeneralError, T> prependStage(Either<GeneralError, T> either, final String s) {
                return new Left<GeneralError, T>(either.left().get().prependStage(s));
    }

    public static <A, B> Either<GeneralError, B> prependStageAndCastLeft(Either<GeneralError, A> either,
                                                                         final String s) {
        return new Left<GeneralError, B>(either.left().get().prependStage(s));
    }
}
