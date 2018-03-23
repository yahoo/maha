// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import com.yahoo.maha.parrequest2.GeneralError;
import org.testng.annotations.Test;
import scala.Option;
import scala.util.Either;

import static org.testng.Assert.assertTrue;

/**
 * Created by hiral on 5/24/14.
 */
public class TestGeneralError {

    @Test
    public void testGeneralError() {
        GeneralError generalError = new GeneralError("stage", "msg", Option.<Throwable>empty());
        assertTrue(generalError.stage.equals("stage"));
        assertTrue(generalError.message.equals("msg"));
        assertTrue(generalError.throwableOption.isEmpty());
    }

    @Test
    public void testFrom() {
        GeneralError generalError = GeneralError.from("stage", "msg");
        assertTrue(generalError.stage.equals("stage"));
        assertTrue(generalError.message.equals("msg"));
        assertTrue(generalError.throwableOption.isEmpty());
    }

    @Test
    public void testFromWithThrowable() {
        Throwable t = new IllegalArgumentException("bad robot");
        GeneralError generalError = GeneralError.from("stage", "msg", t);
        assertTrue(generalError.stage.equals("stage"));
        assertTrue(generalError.message.equals("msg"));
        assertTrue(generalError.throwableOption.isDefined());
        assertTrue(generalError.throwableOption.get().equals(t));
    }

    @Test
    public void testPrependStage() {
        GeneralError generalError = GeneralError.from("stage", "msg").prependStage("prepend");
        assertTrue(generalError.stage.equals("prepend :: stage"));
        assertTrue(generalError.message.equals("msg"));
        assertTrue(generalError.throwableOption.isEmpty());
    }

    @Test
    public void testEither() {
        Either<GeneralError, Integer> generalError = GeneralError.either("stage", "msg");
        assertTrue(generalError.isLeft());
        assertTrue(generalError.left().get().stage.equals("stage"));
        assertTrue(generalError.left().get().message.equals("msg"));
        assertTrue(generalError.left().get().throwableOption.isEmpty());
    }

    @Test
    public void testEitherWithThrowable() {
        Throwable t = new IllegalArgumentException("bad robot");
        Either<GeneralError, Integer> generalError = GeneralError.either("stage", "msg", t);
        assertTrue(generalError.isLeft());
        assertTrue(generalError.left().get().stage.equals("stage"));
        assertTrue(generalError.left().get().message.equals("msg"));
        assertTrue(generalError.left().get().throwableOption.isDefined());
        assertTrue(generalError.left().get().throwableOption.get().equals(t));
    }

    @Test
    public void testPrependStageWithEither() {
        Either<GeneralError, Integer>
            generalError =
            GeneralError.prependStage(GeneralError.<Integer>either("stage", "msg"), "prepend");
        assertTrue(generalError.isLeft());
        assertTrue(generalError.left().get().stage.equals("prepend :: stage"));
        assertTrue(generalError.left().get().message.equals("msg"));
        assertTrue(generalError.left().get().throwableOption.isEmpty());
    }

    @Test
    public void testPrependStageAndCastWithEither() {
        Either<GeneralError, Integer>
            generalError =
            GeneralError.prependStageAndCastLeft(GeneralError.<String>either("stage", "msg"), "prepend");
        assertTrue(generalError.isLeft());
        assertTrue(generalError.left().get().stage.equals("prepend :: stage"));
        assertTrue(generalError.left().get().message.equals("msg"));
        assertTrue(generalError.left().get().throwableOption.isEmpty());
    }
}
