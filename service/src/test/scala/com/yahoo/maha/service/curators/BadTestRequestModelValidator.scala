package com.yahoo.maha.service.curators

import com.yahoo.maha.core.RequestModelResult
import com.yahoo.maha.service.MahaRequestContext

/**
 * Created by pranavbhole on 25/04/18.
 */
class BadTestRequestModelValidator extends CuratorRequestModelValidator{
  override def validate(mahaRequestContext: MahaRequestContext, requestModelResult: RequestModelResult): Unit =  {
    throw new IllegalArgumentException("Failed to validate by BadTestRequestModelValidator")
  }
}
