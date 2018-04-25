package com.yahoo.maha.service.output

import java.io.OutputStream

/**
 * Created by pranavbhole on 25/04/18.
 */
class StringStream extends OutputStream {
  val stringBuilder = new StringBuilder()
  override def write(b: Int): Unit = {
    stringBuilder.append(b.toChar)
  }
  override def toString() : String = stringBuilder.toString()
}

