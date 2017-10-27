// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.report

import java.io._

import org.scalatest.{Matchers, FunSuite}
import com.yahoo.maha.core.query.Row

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Ravi on 01/29/2016
  */

class RowCSVWriterTest extends FunSuite with Matchers {

  /**
    * Test the isNumeric function of RowCSVWriter. isNumeric should return true for all numbers.
    */
  test("isNumeric") {
    val isNumber1 = RowCSVWriter.isNumeric("123")
    assert(isNumber1, "isNumeric should return true as 123 is a correct number")
    val isNumber2 = RowCSVWriter.isNumeric("ab123")
    assert(isNumber2 === false, "isNumeric should return false as ab123 is not a correct number")
  }

  /**
    * Test the isStringStartingWithSpecialChar function of RowCSVWriter. isStringStartingWithSpecialChar should return
    * true if string starts with non-aplha numeric characters.
    */
  test("isStringStartingWithSpecialChar") {
    val isStringStartingWithChar1 = RowCSVWriter.isStringStartingWithSpecialChar("@abc", "@")
    //assert(isStringStartingWithChar1 === true)
    assert(isStringStartingWithChar1 === true, "String starts with special character @ should return true for isStringStartingWithSpecialChar")
    val isStringStartingWithChar2 = RowCSVWriter.isStringStartingWithSpecialChar(null, "@")
    assert(isStringStartingWithChar2 === false, "Input is null")
  }

  /**
    * Test the constructors in RowCSVWriter Class
    */
  test("RowCSVWriterClass") {
      val content = "This is the content to write into file"
      val file = new File("target/filename.txt")
      // if file doesnt exists, then create it
      if (!file.exists()) {
        file.createNewFile()
      }
      val fw = new FileWriter(file.getAbsoluteFile())
      val bw = new BufferedWriter(fw)
      bw.write(content)
      var csvWriter1 = new RowCSVWriter(bw)
      val csvWriter2 = new RowCSVWriter(bw, ',')
      val csvWriter3 = new RowCSVWriter(bw, ',', '\'')
      val csvWriter4 = new RowCSVWriter(bw, ',', '\'', '\\')
      csvWriter4.close()
      System.out.println("Done")
  }

  /**
    * Test WriteRow method of RowCSVWriter. This method internally calls all the other methods of RowCSVWriter Class.
    */
  test("WriteRow") {
    var csvWriter: RowCSVWriter = null
      //val content = "This is the content to write into file";
      val file = new File("target/filename.csv")
      // if file doesnt exist, then create it
      if (!file.exists()) {
        file.createNewFile()
      }
      val fw = new FileWriter(file.getAbsoluteFile())
      val bw = new BufferedWriter(fw)
      csvWriter = new RowCSVWriter(bw)
      val alias: Map[String, Int] = Map("A" -> 1, "B" -> 2)

      val arrayBuffer = new ArrayBuffer[Any]()
      arrayBuffer += "k_stats"
      arrayBuffer += "advertiser_id"
      val newRow = new Row(alias, arrayBuffer)

      val columnNames: IndexedSeq[String] = IndexedSeq("A", "B")
      //Writing row to the csv file created above
      csvWriter.writeRow(newRow, columnNames)
      //Very important to close Buffer otherwise write doesn't happen.
      csvWriter.close()
  }

  test("shouldQuoteStringsCorrectly") {
    // given
    val string = "Ilive Itb183b 2.0-channel, 32\\\" Bluetooth Soundbar"
    val writer = new StringWriter()
    val csvWriter = new RowCSVWriter(writer)

    val alias: Map[String, Int] = Map("A" -> 1, "B" -> 2)

    val arrayBuffer = new ArrayBuffer[Any]()
    arrayBuffer += "blah1"
    arrayBuffer += string
    val newRow = new Row(alias, arrayBuffer)
    val columnNames: IndexedSeq[String] = IndexedSeq("A", "B")

    csvWriter.writeRow(newRow, columnNames)

    // then
    val expected = """blah1,"Ilive Itb183b 2.0-channel, 32\"" Bluetooth Soundbar""""
    assert(writer.getBuffer.toString.trim === expected, writer.getBuffer.toString)
  }
}

