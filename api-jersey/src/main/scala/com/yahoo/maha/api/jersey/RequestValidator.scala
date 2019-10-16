// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey

import com.yahoo.maha.service.MahaRequestContext
import javax.ws.rs.container.ContainerRequestContext

trait RequestValidator {
  def validate(mahaRequestContext: MahaRequestContext, containerRequestContext: ContainerRequestContext): Unit
}

class NoopRequestValidator extends RequestValidator {
  override def validate(mahaRequestContext: MahaRequestContext, containerRequestContext: ContainerRequestContext): Unit = {}
}
