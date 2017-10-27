// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider
import org.glassfish.jersey.jackson.JacksonFeature
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.server.spring.SpringComponentProvider

class MahaResourceConfig extends ResourceConfig {
  register(classOf[JacksonJaxbJsonProvider])
  register(classOf[JacksonFeature])
  register(classOf[SpringComponentProvider])
  register(classOf[MahaResource])
  register(classOf[GenericExceptionMapper])
}
