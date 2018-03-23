// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import scala.Function1;
import scala.Unit;

import java.util.concurrent.Callable;

/**
 * Created by hiral on 6/10/14.
 */
public class ParCallable<T> implements Callable<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParCallable.class);

    private final String requestId;
    private final String userInfo;
    private final Callable<T> callable;
    private final Function1<Unit, T> fn1;

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

    public ParCallable(Callable<T> callable) {
        requestId = getFromMDC(requestIdMDCKey);
        userInfo = getFromMDC(userInfoMDCKey);
        this.callable = callable;
        this.fn1 = null;
    }

    public ParCallable(Function1<Unit, T> fn1) {
        requestId = getFromMDC(requestIdMDCKey);
        userInfo = getFromMDC(userInfoMDCKey);
        this.callable = null;
        this.fn1 = fn1;
    }

    public ParCallable(String requestId, String userInfo, Callable<T> callable) {
        this.requestId = requestId;
        this.userInfo = userInfo;
        this.callable = callable;
        this.fn1 = null;
    }

    @Override
    public final T call() throws Exception {
        try {
            injectMDC();
            if(callable != null)
                return callable.call();
            else
                return fn1.apply(null);
        } catch (Exception e) {
            LOGGER.error("Exception in callable", e);
            throw e;
        } finally {
            clearMDC();
        }
    }

    public static <T> ParCallable<T> from(final Callable<T> callable) {
        return new ParCallable<T>(callable);
    }

    public static <T> ParCallable<T> fromScala(final Function1<Unit, T> fn1) {
        return new ParCallable<T>(fn1);
    }
}
