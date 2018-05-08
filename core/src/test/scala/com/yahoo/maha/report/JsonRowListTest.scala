// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.report

import java.io._
import java.nio.file.{Files, StandardOpenOption}

import com.yahoo.maha.core.CoreSchema.{AdvertiserSchema, ResellerSchema}
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension.{DimCol, OracleDerDimCol, PubCol}
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.query._
import com.yahoo.maha.core.query.oracle.OracleQueryGenerator
import com.yahoo.maha.core.registry.RegistryBuilder
import com.yahoo.maha.core.request.ReportingRequest
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Created by hiral on 5/6/16.
 */
class JsonRowListTest extends FunSuite with BaseQueryGeneratorTest with SharedDimSchema with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    OracleQueryGenerator.register(queryGeneratorRegistry,DefaultPartitionColumnRenderer)
  }

  def query : Query = {
    val jsonString = s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID", "alias": "campaign_id"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "Campaign Status"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"},
                            {"field": "Campaign Name", "operator": "=", "value": "MegaCampaign"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ],
                          "paginationStartIndex":-1,
                          "rowsPerPage":100
                        }"""

    val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestSync(jsonString))
    val registry = getDefaultRegistry()
    val requestModel = RequestModel.from(request, registry)
    assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


    val queryPipelineTry = generatePipeline(requestModel.toOption.get)
    assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

    queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]
  }

  test("successfully construct json row list") {
    val tmpPath = new File("target").toPath
    val tmpFile = Files.createTempFile(tmpPath, "pre", "suf").toFile
    tmpFile.deleteOnExit()
    Files.write(tmpFile.toPath, Array[Byte](1,2,3,4,5), StandardOpenOption.TRUNCATE_EXISTING) // Clear file
    val bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFile), "UTF-8"))
    val jsonGenerator = JsonRowList.jsonGenerator(bufferedWriter)
    val jsonRowList : QueryRowList = JsonRowList.jsonRowList(jsonGenerator, None, false)(query)
    assert(jsonRowList.columnNames === IndexedSeq("Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR", "TOTALROWS"))
    assert(jsonRowList.isEmpty)

    jsonGenerator.writeStartObject()
    jsonRowList.withLifeCycle {
      val row = jsonRowList.newRow

      row.addValue("Campaign ID", java.lang.Integer.valueOf(1))
      row.addValue("Impressions", java.lang.Integer.valueOf(2))
      row.addValue("Campaign Name", "\"name\"")
      row.addValue("Campaign Status", "o,n")
      row.addValue("CTR", java.lang.Double.valueOf(1.11D))
      row.addValue("TOTALROWS", java.lang.Integer.valueOf(1))
      assert(row.getValue("Campaign ID") === 1)
      assert(row.getValue("Impressions") === 2)
      assert(row.getValue("Campaign Name") === "\"name\"")
      assert(row.getValue("Campaign Status") === "o,n")
      assert(row.getValue("CTR") === 1.11D)
      assert(row.getValue("TOTALROWS") === 1)

      assert(row.aliasMap.size === 6)
      assert(row.getValue(0) === 1)
      assert(row.getValue(1) === 2)
      assert(row.getValue(2) === "\"name\"")
      assert(row.getValue(3) === "o,n")
      assert(row.getValue(4) === 1.11D)
      assert(row.getValue(5) === 1)

      jsonRowList.addRow(row)

      jsonRowList
    }
    jsonGenerator.writeEndObject()
    jsonGenerator.flush()
    bufferedWriter.close()
    val jsonString = scala.io.Source.fromFile(tmpFile, "UTF-8").getLines().mkString
    assert(jsonString === """{"header":{"cube":"k_stats","fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"},{"fieldName":"TotalRows","fieldType":"CONSTANT"}],"maxRows":100},"rows":[[1,2,"\"name\"","o,n",1.11,1]],"debug":{"fields":[{"fieldName":"Campaign ID","dataType":"Number"},{"fieldName":"Campaign Status","dataType":"String"},{"fieldName":"Impressions","dataType":"Number"},{"fieldName":"Campaign Name","dataType":"String"},{"fieldName":"CTR","dataType":"Number"},{"fieldName":"TOTALROWS","dataType":"Number"},{"fieldName":"TotalRows","dataType":"integer"}],"drivingQuery":{"tableName":"campaign_oracle","engine":"Oracle"}}}""")
  }

  test("successfully construct json row list from another row list") {
    val rowList : DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList("Campaign ID", query)
    assert(rowList.columnNames === IndexedSeq("Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR", "TOTALROWS"))
    assert(rowList.isEmpty)

    val row = rowList.newRow

    row.addValue("Campaign ID",java.lang.Integer.valueOf(1))
    row.addValue("Campaign Name","name")
    row.addValue("Campaign Status","on")
    row.addValue("CTR", java.lang.Double.valueOf(1.11D))
    row.addValue("TOTALROWS", java.lang.Integer.valueOf(1))

    assert(row.aliasMap.size === 6)
    assert(row.getValue(0) === 1)
    assert(row.getValue(1) === null)
    assert(row.getValue(2) === "name")
    assert(row.getValue(3) === "on")
    assert(row.getValue(4) === 1.11D)
    assert(row.getValue(5) === 1)
    assert(row.getValue("Campaign ID") === 1)
    assert(row.getValue("Impressions") === null)
    assert(row.getValue("Campaign Name") === "name")
    assert(row.getValue("Campaign Status") === "on")
    assert(row.getValue("CTR") === 1.11D)
    assert(row.getValue("TOTALROWS") === 1)

    rowList.addRow(row)

    val row2 = rowList.newRow
    row2.addValue("Campaign ID", java.lang.Integer.valueOf(1))
    row2.addValue("Impressions",java.lang.Integer.valueOf(2))
    rowList.addRow(row2)

    assert(row.getValue("Campaign ID") === 1)
    assert(row.getValue("Impressions") === 2)
    assert(row.getValue("Campaign Name") === "name")
    assert(row.getValue("Campaign Status") === "on")
    assert(row.getValue("CTR") === 1.11D)
    assert(row.getValue("TOTALROWS") === 1)

    val writer = new StringWriter()
    val jsonGenerator = JsonRowList.jsonGenerator(writer)
    jsonGenerator.writeStartObject()
    val jsonRowList : JsonRowList = JsonRowList.from(rowList, Some(10), jsonGenerator, false)
    assert(jsonRowList.columnNames === IndexedSeq("Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR", "TOTALROWS"))
    assert(jsonRowList.isEmpty)
    jsonGenerator.writeEndObject()
    jsonGenerator.flush()
    val jsonString = writer.getBuffer.toString
    assert(jsonString === """{"header":{"cube":"k_stats","fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"},{"fieldName":"TotalRows","fieldType":"CONSTANT"}],"maxRows":100},"rows":[[1,2,"name","on",1.11,10]],"debug":{"fields":[{"fieldName":"Campaign ID","dataType":"Number"},{"fieldName":"Campaign Status","dataType":"String"},{"fieldName":"Impressions","dataType":"Number"},{"fieldName":"Campaign Name","dataType":"String"},{"fieldName":"CTR","dataType":"Number"},{"fieldName":"TOTALROWS","dataType":"Number"},{"fieldName":"TotalRows","dataType":"integer"}],"drivingQuery":{"tableName":"campaign_oracle","engine":"Oracle"}}}""")
  }

  test("successfully construct json row list in compatibility mode") {
    val tmpPath = new File("target").toPath
    val tmpFile = Files.createTempFile(tmpPath, "pre", "suf").toFile
    tmpFile.deleteOnExit()
    val bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFile), "UTF-8"))
    val jsonGenerator = JsonRowList.jsonGenerator(bufferedWriter)
    val jsonRowList : QueryRowList = JsonRowList.jsonRowList(jsonGenerator, None, true)(query)
    assert(jsonRowList.columnNames === IndexedSeq("Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR", "TOTALROWS"))
    assert(jsonRowList.isEmpty)

    jsonGenerator.writeStartObject()
    jsonRowList.withLifeCycle {
      val row = jsonRowList.newRow

      row.addValue("Campaign ID", java.lang.Integer.valueOf(1))
      row.addValue("Impressions", java.lang.Integer.valueOf(2))
      row.addValue("Campaign Name", "\"name\"")
      row.addValue("Campaign Status", "o,n")
      row.addValue("CTR", java.lang.Double.valueOf(1.11D))
      row.addValue("TOTALROWS", java.lang.Integer.valueOf(1))
      assert(row.getValue("Campaign ID") === 1)
      assert(row.getValue("Impressions") === 2)
      assert(row.getValue("Campaign Name") === "\"name\"")
      assert(row.getValue("Campaign Status") === "o,n")
      assert(row.getValue("CTR") === 1.11D)
      assert(row.getValue("TOTALROWS") === 1)

      assert(row.aliasMap.size === 6)
      assert(row.getValue(0) === 1)
      assert(row.getValue(1) === 2)
      assert(row.getValue(2) === "\"name\"")
      assert(row.getValue(3) === "o,n")
      assert(row.getValue(4) === 1.11D)
      assert(row.getValue(5) === 1)

      jsonRowList.addRow(row)
    }
    jsonGenerator.writeEndObject()
    jsonGenerator.flush()
    bufferedWriter.close()
    val jsonString = scala.io.Source.fromFile(tmpFile, "UTF-8").getLines().mkString
    assert(jsonString === """{"header":{"cube":"k_stats","fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"},{"fieldName":"TotalRows","fieldType":"CONSTANT"}],"maxRows":100},"fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"},{"fieldName":"TotalRows","fieldType":"CONSTANT"}],"rows":[[1,2,"\"name\"","o,n",1.11,1]],"debug":{"fields":[{"fieldName":"Campaign ID","dataType":"Number"},{"fieldName":"Campaign Status","dataType":"String"},{"fieldName":"Impressions","dataType":"Number"},{"fieldName":"Campaign Name","dataType":"String"},{"fieldName":"CTR","dataType":"Number"},{"fieldName":"TOTALROWS","dataType":"Number"},{"fieldName":"TotalRows","dataType":"integer"}],"drivingQuery":{"tableName":"campaign_oracle","engine":"Oracle"}}}""")
  }

  test("successfully construct json row list from another row list in compatibility mode") {
    val rowList : DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList("Campaign ID", query)
    assert(rowList.columnNames === IndexedSeq("Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR", "TOTALROWS"))
    assert(rowList.isEmpty)

    val row = rowList.newRow

    row.addValue("Campaign ID",java.lang.Integer.valueOf(1))
    row.addValue("Campaign Name","name")
    row.addValue("Campaign Status","on")
    row.addValue("CTR", java.lang.Double.valueOf(1.11D))
    row.addValue("TOTALROWS", java.lang.Integer.valueOf(1))

    assert(row.aliasMap.size === 6)
    assert(row.getValue(0) === 1)
    assert(row.getValue(1) === null)
    assert(row.getValue(2) === "name")
    assert(row.getValue(3) === "on")
    assert(row.getValue(4) === 1.11D)
    assert(row.getValue(5) === 1)
    assert(row.getValue("Campaign ID") === 1)
    assert(row.getValue("Impressions") === null)
    assert(row.getValue("Campaign Name") === "name")
    assert(row.getValue("Campaign Status") === "on")
    assert(row.getValue("CTR") === 1.11D)
    assert(row.getValue("TOTALROWS") === 1)

    rowList.addRow(row)

    val row2 = rowList.newRow
    row2.addValue("Campaign ID", java.lang.Integer.valueOf(1))
    row2.addValue("Impressions",java.lang.Integer.valueOf(2))
    rowList.addRow(row2)

    assert(row.getValue("Campaign ID") === 1)
    assert(row.getValue("Impressions") === 2)
    assert(row.getValue("Campaign Name") === "name")
    assert(row.getValue("Campaign Status") === "on")
    assert(row.getValue("CTR") === 1.11D)
    assert(row.getValue("TOTALROWS") === 1)

    val writer = new StringWriter()
    val jsonGenerator = JsonRowList.jsonGenerator(writer)
    jsonGenerator.writeStartObject()
    val jsonRowList : JsonRowList = JsonRowList.from(rowList, Some(10), jsonGenerator, true)
    assert(jsonRowList.columnNames === IndexedSeq("Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR", "TOTALROWS"))
    assert(jsonRowList.isEmpty)
    jsonGenerator.writeEndObject()
    jsonGenerator.flush()
    val jsonString = writer.getBuffer.toString
    
    assert(jsonString === """{"header":{"cube":"k_stats","fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"},{"fieldName":"TotalRows","fieldType":"CONSTANT"}],"maxRows":100},"fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"},{"fieldName":"TotalRows","fieldType":"CONSTANT"}],"rows":[[1,2,"name","on",1.11,10]],"debug":{"fields":[{"fieldName":"Campaign ID","dataType":"Number"},{"fieldName":"Campaign Status","dataType":"String"},{"fieldName":"Impressions","dataType":"Number"},{"fieldName":"Campaign Name","dataType":"String"},{"fieldName":"CTR","dataType":"Number"},{"fieldName":"TOTALROWS","dataType":"Number"},{"fieldName":"TotalRows","dataType":"integer"}],"drivingQuery":{"tableName":"campaign_oracle","engine":"Oracle"}}}""")
  }

  test("no si or mr in request") {
    def query : Query = {
      val jsonString =
        s"""{
                          "cube": "k_stats",
                          "selectFields": [
                            {"field": "Campaign ID", "alias": "campaign_id"},
                            {"field": "Impressions"},
                            {"field": "Campaign Name"},
                            {"field": "Campaign Status"},
                            {"field": "CTR"}
                          ],
                          "filterExpressions": [
                            {"field": "Day", "operator": "between", "from": "$fromDate", "to": "$toDate"},
                            {"field": "Advertiser ID", "operator": "=", "value": "213"},
                            {"field": "Campaign Name", "operator": "=", "value": "MegaCampaign"}
                          ],
                          "sortBy": [
                            {"field": "Campaign Name", "order": "Asc"}
                          ]
                        }"""

      val request: ReportingRequest = ReportingRequest.enableDebug(getReportingRequestAsync(jsonString))
      val registry = getDefaultRegistry()
      val requestModel = RequestModel.from(request, registry)
      assert(requestModel.isSuccess, requestModel.errorMessage("Building request model failed"))


      val queryPipelineTry = generatePipeline(requestModel.toOption.get)
      assert(queryPipelineTry.isSuccess, queryPipelineTry.errorMessage("Fail to get the query pipeline"))

      queryPipelineTry.toOption.get.queryChain.drivingQuery.asInstanceOf[OracleQuery]

    }
    val rowList : DimDrivenFactOrderedPartialRowList = new DimDrivenFactOrderedPartialRowList("Campaign ID", query)
    assert(rowList.columnNames === IndexedSeq("Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR"))
    assert(rowList.isEmpty)

    val row = rowList.newRow

    row.addValue("Campaign ID",java.lang.Integer.valueOf(1))
    row.addValue("Campaign Name","name")
    row.addValue("Campaign Status","on")
    row.addValue("CTR", java.lang.Double.valueOf(1.11D))

    assert(row.aliasMap.size === 5)
    assert(row.getValue(0) === 1)
    assert(row.getValue(1) === null)
    assert(row.getValue(2) === "name")
    assert(row.getValue(3) === "on")
    assert(row.getValue(4) === 1.11D)
    assert(row.getValue("Campaign ID") === 1)
    assert(row.getValue("Impressions") === null)
    assert(row.getValue("Campaign Name") === "name")
    assert(row.getValue("Campaign Status") === "on")
    assert(row.getValue("CTR") === 1.11D)

    rowList.addRow(row)

    val row2 = rowList.newRow
    row2.addValue("Campaign ID", java.lang.Integer.valueOf(1))
    row2.addValue("Impressions",java.lang.Integer.valueOf(2))
    rowList.addRow(row2)

    assert(row.getValue("Campaign ID") === 1)
    assert(row.getValue("Impressions") === 2)
    assert(row.getValue("Campaign Name") === "name")
    assert(row.getValue("Campaign Status") === "on")
    assert(row.getValue("CTR") === 1.11D)

    val writer = new StringWriter()
    val jsonGenerator = JsonRowList.jsonGenerator(writer)
    jsonGenerator.writeStartObject()
    val jsonRowList : JsonRowList = JsonRowList.from(rowList, Some(10), jsonGenerator, true)
    assert(jsonRowList.columnNames === IndexedSeq("Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR"))
    assert(jsonRowList.isEmpty)
    jsonGenerator.writeEndObject()
    jsonGenerator.flush()
    val jsonString = writer.getBuffer.toString
    
    assert(jsonString === """{"header":{"cube":"k_stats","fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"}]},"fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"}],"rows":[[1,2,"name","on",1.11]],"rowCount":10,"debug":{"fields":[{"fieldName":"Campaign ID","dataType":"Number"},{"fieldName":"Campaign Status","dataType":"String"},{"fieldName":"Impressions","dataType":"Number"},{"fieldName":"Campaign Name","dataType":"String"},{"fieldName":"CTR","dataType":"Number"}],"drivingQuery":{"tableName":"fact_table_keywords","engine":"Oracle"}}}""")

    val writer2 = new StringWriter()
    val jsonGenerator2 = JsonRowList.jsonGenerator(writer2)
    jsonGenerator2.writeStartObject()
    val jsonRowList2 : JsonRowList = JsonRowList.from(rowList, None, jsonGenerator2, true)
    jsonGenerator2.writeEndObject()
    jsonGenerator2.flush()
    val jsonString2 = writer2.getBuffer.toString
    assert(jsonString2 === """{"header":{"cube":"k_stats","fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"}]},"fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"}],"rows":[[1,2,"name","on",1.11]],"rowCount":1,"debug":{"fields":[{"fieldName":"Campaign ID","dataType":"Number"},{"fieldName":"Campaign Status","dataType":"String"},{"fieldName":"Impressions","dataType":"Number"},{"fieldName":"Campaign Name","dataType":"String"},{"fieldName":"CTR","dataType":"Number"}],"drivingQuery":{"tableName":"fact_table_keywords","engine":"Oracle"}}}""")
  }

  test("successfully construct file json row list") {
    val tmpPath = new File("target").toPath
    val tmpFile = Files.createTempFile(tmpPath, "pre2", "suf2").toFile
    tmpFile.deleteOnExit()
    Files.write(tmpFile.toPath, Array[Byte](1,2,3,4,5), StandardOpenOption.TRUNCATE_EXISTING) // Clear file
    val jsonRowList : QueryRowList = FileJsonRowList.fileJsonRowList(tmpFile, None, false)(query)
    assert(jsonRowList.columnNames === IndexedSeq("Campaign ID", "Impressions", "Campaign Name", "Campaign Status", "CTR", "TOTALROWS"))
    assert(jsonRowList.isEmpty)
    jsonRowList.withLifeCycle {
      val row = jsonRowList.newRow

      row.addValue("Campaign ID", java.lang.Integer.valueOf(1))
      row.addValue("Impressions", java.lang.Integer.valueOf(2))
      row.addValue("Campaign Name", "\"name\"")
      row.addValue("Campaign Status", "o,n")
      row.addValue("CTR", java.lang.Double.valueOf(1.11D))
      row.addValue("TOTALROWS", java.lang.Integer.valueOf(1))
      assert(row.getValue("Campaign ID") === 1)
      assert(row.getValue("Impressions") === 2)
      assert(row.getValue("Campaign Name") === "\"name\"")
      assert(row.getValue("Campaign Status") === "o,n")
      assert(row.getValue("CTR") === 1.11D)
      assert(row.getValue("TOTALROWS") === 1)

      assert(row.aliasMap.size === 6)
      assert(row.getValue(0) === 1)
      assert(row.getValue(1) === 2)
      assert(row.getValue(2) === "\"name\"")
      assert(row.getValue(3) === "o,n")
      assert(row.getValue(4) === 1.11D)
      assert(row.getValue(5) === 1)

      jsonRowList.addRow(row)

      jsonRowList
    }
    val jsonString = scala.io.Source.fromFile(tmpFile, "UTF-8").getLines().mkString
    assert(jsonString === """{"header":{"cube":"k_stats","fields":[{"fieldName":"campaign_id","fieldType":"DIM"},{"fieldName":"Impressions","fieldType":"FACT"},{"fieldName":"Campaign Name","fieldType":"DIM"},{"fieldName":"Campaign Status","fieldType":"DIM"},{"fieldName":"CTR","fieldType":"FACT"},{"fieldName":"TotalRows","fieldType":"CONSTANT"}],"maxRows":100},"rows":[[1,2,"\"name\"","o,n",1.11,1]],"debug":{"fields":[{"fieldName":"Campaign ID","dataType":"Number"},{"fieldName":"Campaign Status","dataType":"String"},{"fieldName":"Impressions","dataType":"Number"},{"fieldName":"Campaign Name","dataType":"String"},{"fieldName":"CTR","dataType":"Number"},{"fieldName":"TOTALROWS","dataType":"Number"},{"fieldName":"TotalRows","dataType":"integer"}],"drivingQuery":{"tableName":"campaign_oracle","engine":"Oracle"}}}""")
  }
  test("fail to construct file json row list with no write perm") {
    val tmpFile= new File("/blah")
    val jsonRowList : RowList = FileJsonRowList.fileJsonRowList(tmpFile, None, false)(query)
    val thrown = intercept[FileNotFoundException]{
      jsonRowList.withLifeCycle {
        
      }
    }
    assert(thrown.getMessage === "/blah (Permission denied)")
  }

  override protected[this] def registerFacts(forcedFilters: Set[ForcedFilter], registryBuilder: RegistryBuilder): Unit = {
    registryBuilder.register(pubfact(forcedFilters))
  }

  def pubfact(forcedFilters: Set[ForcedFilter] = Set.empty): PublicFact = {
    import OracleExpression._
    ColumnContext.withColumnContext { implicit dc: ColumnContext =>
      Fact.newFact(
        "fact0_table_keywords", DailyGrain, OracleEngine, Set(AdvertiserSchema, ResellerSchema),
        Set(
          DimCol("keyword_id", IntType(), annotations = Set(ForeignKey("keyword")))
          , DimCol("ad_id", IntType(), annotations = Set(ForeignKey("ad")))
          , DimCol("ad_group_id", IntType(), annotations = Set(ForeignKey("ad_group")))
          , DimCol("campaign_id", IntType(), annotations = Set(ForeignKey("campaign")))
          , DimCol("advertiser_id", IntType(), annotations = Set(ForeignKey("advertiser")))
          , DimCol("stats_source", IntType(3))
          , DimCol("price_type", IntType(3, (Map(1 -> "CPC", 2 -> "CPA", 3 -> "CPM", 6 -> "CPV", 7 -> "CPCV", -10 -> "CPE", -20 -> "CPF"), "NONE")))
          , DimCol("start_time", IntType())
          , DimCol("landing_page_url", StrType(), annotations = Set(EscapingRequired))
          , DimCol("stats_date", DateType("YYYY-MM-DD"))
          , DimCol("column_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned")))
          , DimCol("column2_id", IntType(), annotations = Set(ForeignKey("non_hash_partitioned_with_singleton")))
          , OracleDerDimCol("Ad Group Start Date Full", StrType(), TIMESTAMP_TO_FORMATTED_DATE("{start_time}", "YYYY-MM-dd HH:mm:ss"))
          , OracleDerDimCol("Month", DateType(), GET_INTERVAL_DATE("{stats_date}", "M"))
          , OracleDerDimCol("Week", DateType(), GET_INTERVAL_DATE("{stats_date}", "W"))
        ),
        Set(
          FactCol("impressions", IntType(3, 1))
          , FactCol("clicks", IntType(3, 0, 1, 800))
          , FactCol("spend", DecType(0, "0.0"))
          , FactCol("max_bid", DecType(0, "0.0"), MaxRollup)
          , FactCol("Average CPC", DecType(), OracleCustomRollup("{spend}" / "{clicks}"))
          , FactCol("CTR", DecType(), OracleCustomRollup(SUM("{clicks}" /- "{impressions}")))
          , FactCol("avg_pos", DecType(3, "0.0", "0.1", "500"), OracleCustomRollup(SUM("{avg_pos}" * "{impressions}") /- SUM("{impressions}")))
        ),
        annotations = Set(
          OracleFactStaticHint("PARALLEL_INDEX(cb_campaign_k_stats 4)"),
          OracleFactDimDrivenHint("PUSH_PRED PARALLEL_INDEX(cb_campaign_k_stats 4)")
        )
      )
    }
      .newRollUp("fact_table_keywords", "fact0_table_keywords", discarding = Set("ad_id"), columnAliasMap = Map("price_type" -> "pricing_type"))
      .toPublicFact("k_stats",
        Set(
          PubCol("stats_date", "Day", InBetweenEquality),
          PubCol("keyword_id", "Keyword ID", InEquality),
          PubCol("ad_id", "Ad ID", InEquality),
          PubCol("ad_group_id", "Ad Group ID", InEquality),
          PubCol("campaign_id", "Campaign ID", InEquality),
          PubCol("advertiser_id", "Advertiser ID", InEquality),
          PubCol("stats_source", "Source", Equality),
          PubCol("price_type", "Pricing Type", In),
          PubCol("landing_page_url", "Destination URL", Set.empty),
          PubCol("column_id", "Column ID", Equality),
          PubCol("column2_id", "Column2 ID", Equality),
          PubCol("Month", "Month", Equality),
          PubCol("Week", "Week", Equality)
        ),
        Set(
          PublicFactCol("impressions", "Impressions", InBetweenEquality),
          PublicFactCol("impressions", "Total Impressions", InBetweenEquality),
          PublicFactCol("clicks", "Clicks", InBetweenEquality),
          PublicFactCol("spend", "Spend", Set.empty),
          PublicFactCol("avg_pos", "Average Position", Set.empty),
          PublicFactCol("max_bid", "Max Bid", Set.empty),
          PublicFactCol("Average CPC", "Average CPC", InBetweenEquality),
          PublicFactCol("CTR", "CTR", InBetweenEquality)
        ),
        Set(EqualityFilter("Source", "2", isForceFilter = true)),
        getMaxDaysWindow, getMaxDaysLookBack
      )
  }

}
