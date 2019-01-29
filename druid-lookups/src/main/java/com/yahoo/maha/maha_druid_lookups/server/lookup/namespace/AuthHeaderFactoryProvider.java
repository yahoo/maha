// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.commons.lang.StringUtils;

public class AuthHeaderFactoryProvider implements Provider<AuthHeaderFactory> {

  private final AuthHeaderFactory authHeaderFactory;

  @Inject
  public AuthHeaderFactoryProvider(MahaNamespaceExtractionConfig config) {
    String authHeaderFactoryClass = config.getAuthHeaderFactoryClass();
    Preconditions.checkArgument(StringUtils.isNotBlank(authHeaderFactoryClass), "authHeaderFactory cannot be blank!");
    Class clazz;

    try {
      clazz = Class.forName(authHeaderFactoryClass);
      if(AuthHeaderFactory.class.isAssignableFrom(clazz)) {
        authHeaderFactory = (AuthHeaderFactory) clazz.newInstance();
      } else {
        throw new IllegalArgumentException(String.format("%s must implement type AuthHeaderFactory!", authHeaderFactoryClass));
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }

  }

  @Override
  public AuthHeaderFactory get() {
    return authHeaderFactory;
  }
}
