// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.report

/**
 * Created by hiral on 1/20/16.
 */

import com.opencsv.CSVWriter
import com.yahoo.maha.core.query.Row
import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}

import org.apache.commons.lang.StringUtils


/**
 * A CSV writer with support for Row.
 *
 * Makes use of the opencsv CSVWriter underneath
 */
object RowCSVWriter {
  /**
   * The character used for escaping quotes.
   */
  final val DEFAULT_ESCAPE_CHARACTER: Char = CSVWriter.DEFAULT_ESCAPE_CHARACTER
  /**
   * The default separator to use if none is supplied to the constructor.
   */
  final val DEFAULT_SEPARATOR: Char = CSVWriter.DEFAULT_SEPARATOR
  /**
   * The default quote character to use if none is supplied to the constructor.
   */
  final val DEFAULT_QUOTE_CHARACTER: Char = CSVWriter.DEFAULT_QUOTE_CHARACTER
  /**
   * The quote constant to use when you wish to suppress all quoting, unless it's require if the string contains
   * special characters.
   */
  final val NO_QUOTE_CHARACTER: Char = CSVWriter.NO_QUOTE_CHARACTER
  /**
   * The escape constant to use when you wish to suppress all escaping.
   */
  final val NO_ESCAPE_CHARACTER: Char = CSVWriter.NO_ESCAPE_CHARACTER
  /**
   * Default line terminator uses platform encoding.
   */
  final val DEFAULT_LINE_END: String = CSVWriter.DEFAULT_LINE_END
  /**
   * Excel Special Char prefix
   */
  final val EXCEL_SPECIAL_CHAR_PREFIX: String = " "
  final val EXCEL_SPECIAL_CHAR: String = "+-="

  /**
   * Determine whether String is non-numeric and begins with the given special chars
   */
  def isStringStartingWithSpecialChar(stringValue: String, specialCharsSet: String): Boolean = {
    val stringValueTrimed: String = if (stringValue != null) stringValue.trim else ""
    if (isNumeric(stringValueTrimed)) {
      return false
    }
    stringValueTrimed.length > 0 && specialCharsSet.indexOf(stringValueTrimed.charAt(0)) != -1
  }

  /**
   * Determine whether this String represents a numeric value.
   */
  def isNumeric(stringValue: String): Boolean = {
    (stringValue != null) && stringValue.matches("[-+]?\\d+(\\.\\d+)?")
  }
}

import RowCSVWriter._

class RowCSVWriter(pw: PrintWriter, separator: Char, quotechar: Char, escapechar: Char, lineEnd: String) extends AutoCloseable {

  import CSVWriter._

  val csvWriter = new CSVWriter(pw, separator, quotechar, escapechar, lineEnd)

  def this(writer: Writer) {
    this(new PrintWriter(writer), DEFAULT_SEPARATOR, DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER, DEFAULT_LINE_END)
  }

  def this(writer: Writer, separator: Char) {
    this(new PrintWriter(writer), separator, DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER, DEFAULT_LINE_END)
  }

  def this(writer: Writer, separator: Char, quotechar: Char) {
    this(new PrintWriter(writer), separator, quotechar, DEFAULT_ESCAPE_CHARACTER, DEFAULT_LINE_END)
  }

  def this(writer: Writer, separator: Char, quotechar: Char, escapechar: Char) {
    this(new PrintWriter(writer), separator, quotechar, escapechar, DEFAULT_LINE_END)
  }

  /**
   * Writes single row to a CSV file.
   *
   * @throws IOException  when an error occurs while writing data
   */
  def writeRow(row: Row, columnNames: IndexedSeq[String]) {
    val columnCount: Int = columnNames.size
    val nextLine: Array[String] = new Array[String](columnCount)
    var i: Int = 0
    row.getColumns.foreach {
      col =>
        nextLine(i) = getStringValue(col)
        i+=1
    }
    csvWriter.writeNext(nextLine, false)
  }

  private def getStringValue(columnValue: Any): String = {
    val stringValue: String = if(columnValue == null) {
      StringUtils.EMPTY
    } else {
      columnValue.toString
    }
    if (isStringStartingWithSpecialChar(stringValue, EXCEL_SPECIAL_CHAR)) {
      return EXCEL_SPECIAL_CHAR_PREFIX + stringValue
    }
    stringValue
  }

  def writeColumnNames(columnNames: IndexedSeq[String]) {
    csvWriter.writeNext(columnNames.toArray, false)
  }

  /**
   * Flush underlying stream to writer.
   *
   * @throws IOException if bad things happen
   */
  def flush() {
    csvWriter.flush()
  }

  /**
   * Close the underlying stream writer flushing any buffered content.
   *
   * @throws IOException if bad things happen
   */
  def close() {
    csvWriter.close()
  }

}

trait RowCSVWriterProvider {
  def newRowCSVWriter: RowCSVWriter
}

case class FileRowCSVWriterProvider(file: File) extends RowCSVWriterProvider {
  def newRowCSVWriter: RowCSVWriter = {
    if(file.exists() && file.length() > 0) {
      Files.write(file.toPath, Array[Byte](), StandardOpenOption.TRUNCATE_EXISTING) // Clear file
    }
    val fos = new FileOutputStream(file.getAbsoluteFile, true)
    val writerTry = safeCloseable(fos)(new OutputStreamWriter(_, StandardCharsets.UTF_8))
      .flatMap(safeCloseable(_)(new BufferedWriter(_)))
      .flatMap(safeCloseable(_)(new RowCSVWriter(_, RowCSVWriter.DEFAULT_SEPARATOR)))
    require(writerTry.isSuccess, s"Failed to create RowCSVWriter safely : $writerTry")
    writerTry.get
  }
}
