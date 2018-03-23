// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2.future;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import scala.Function1;

import java.util.function.Function;

/**
 * This class extends the Function class from guava to allow capturing and setting of MDC objects to enforce proper
 * logging
 */
public class ParFunction<T, U> implements Function<T, U> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParFunction.class);
    private final String requestId;
    private final String userInfo;
    private final Function<T, U> fn;
    private final Function1<T, U> fn1;

    private static String requestIdMDCKey = "requestId";
    private static String userInfoMDCKey = "user";
    private static boolean enableMDCInject = true;

    protected void injectMDC() {
        if (enableMDCInject) {
            if (userInfo != null) {
                MDC.put(userInfoMDCKey, userInfo);
            }
            if (requestId != null) {
                MDC.put(requestIdMDCKey, requestId);
            }
        }
    }

    protected void clearMDC() {
        if (enableMDCInject) {
            MDC.remove(userInfoMDCKey);
            MDC.remove(requestIdMDCKey);
        }
    }

    protected static String getFromMDC(String key) {
        if (enableMDCInject) {
            if (key != null) {
                return MDC.get(key);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * Set the request id mdc key for all par callables, should only be done once during initialization of app
     */
    public static synchronized void setRequestIdMDCKey(String s) {
        Preconditions.checkArgument(StringUtils.isNotBlank(s), "Cannot have blank request id MDC key!");
        requestIdMDCKey = s;
    }

    /**
     * Set the user info mdc key for all par callables, should only be done once during initialization of app
     */
    public static synchronized void setUserInfoMDCKey(String s) {
        Preconditions.checkArgument(StringUtils.isNotBlank(s), "Cannot have blank user info MDC key!");
        userInfoMDCKey = s;
    }

    /**
     * Set the enable mdc inject for all par callables, should only be done once during initialization of app
     */
    public static synchronized void setEnableMDCInject(boolean b) {
        enableMDCInject = b;
    }

    public ParFunction(Function<T, U> fn) {
        requestId = getFromMDC(requestIdMDCKey);
        userInfo = getFromMDC(userInfoMDCKey);
        this.fn = fn;
        this.fn1 = null;
    }

    public ParFunction(Function1<T, U> fn1) {
        requestId = getFromMDC(requestIdMDCKey);
        userInfo = getFromMDC(userInfoMDCKey);
        this.fn = null;
        this.fn1 = fn1;
    }

    public U apply(T input) {
        try {
            injectMDC();
            if(fn!=null)
                return fn.apply(input);
            else
                return fn1.apply(input);
        } catch (Exception e) {
            LOGGER.error("Exception in callable", e);
            throw e;
        } finally {
            clearMDC();
        }
    }

    public static <I, O> ParFunction<I, O> from(Function<I,O> fn) {
        return new ParFunction<>(fn);
    }

    public static <I, O> ParFunction<I, O> fromScala(Function1<I,O> fn1) {
        return new ParFunction<>(fn1);
    }
}
