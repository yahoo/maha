package com.yahoo.maha.utils

import org.mockito.Mockito
import org.mockito.stubbing.Stubber

trait MockitoHelper {
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}