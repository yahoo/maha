// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.core.ddl

import com.yahoo.maha.core.HiveEngine

/**
 * Created by shengyao on 3/10/16.
 */
class HiveDDLGeneratorTest extends BaseDDLGeneratorTest {
  val hiveDDLGenerator = new HiveDDLGenerator

  test("test hive fact toDDL") {
    val hiveFacts = pubFact.factList.filter(f => f.engine.equals(HiveEngine))
    val ddlMap : Map[String, String] = hiveFacts.map(fact => fact.name -> hiveDDLGenerator.toDDL(fact)).toMap
    assert(ddlMap.contains("ad_k_stats"), "DDL not generated for hive table ad_k_stats")
    assert(!ddlMap.contains("cb_ad_k_stats"), "DDL should not be generated for oracle table cb_ad_k_stats")
    println(ddlMap)

    val ddl_ad_k_stats = ddlMap.get("ad_k_stats").get
    assert(!ddl_ad_k_stats.contains("PARTITION BY"), "Should not generate partition columns")
    assert(ddl_ad_k_stats.contains("ROW FORMAT DELIMITED FIELDS TERMINATED BY\n'\\001'\nLINES TERMINATED BY '\\n'"),
      "TEXT storage clause not generated correctly")
    assert(!ddl_ad_k_stats.contains("LOCATION"), "Should not generate location clause")
    assert(ddl_ad_k_stats.toString().contains("CREATE TABLE ad_k_stats\n(account_id tinyint, \ncampaign_id tinyint, \nad_group_id tinyint, \nad_id tinyint, \nterm_id tinyint, \nstats_date string, \nimpressions bigint, \nclicks bigint, \nconversions tinyint, \npost_imp_conversions tinyint, \nspend double, \nstats_source tinyint, \nprice_type tinyint, \nlanding_page_url string, \ndevice_type_id bigint, \nmax_bid double, \navg_pos double)"), "Result must match expected contents")
    println(ddl_ad_k_stats.toString())
  }

  test("test hive dim toDDL") {
    val hiveDim = pubDim.dimList.filter(d => d.engine.equals(HiveEngine))
    val ddlMap : Map[String, String] = hiveDim.map(dim => dim.name -> hiveDDLGenerator.toDDL(dim)).toMap

    assert(ddlMap.contains("cache_advertiser"), "DDL not generated for hive table cache_advertiser")
    assert(!ddlMap.contains("oracle_advertiser"), "DDL should not be generated for oracle table oracle_advertiser")

    val ddl_cache_advertiser = ddlMap.get("cache_advertiser").get
    assert(ddl_cache_advertiser.contains("PARTITIONED BY"), "Should generate partition columns")
    assert(ddl_cache_advertiser.contains("STORED AS ORC"), "ORC storage clause not generated correctly")
    assert(ddl_cache_advertiser.contains("LOCATION $(hivePath)/reports/cache_advertiser/"),
      "Location clause not generated correctly")
  }

  test("test hive ddl with column ordering") {
    val hiveDim = pubDim.dimList.filter(d => d.engine.equals(HiveEngine))
    val ddlMap : Map[String, String] = hiveDim.map(dim => dim.name -> hiveDDLGenerator.toDDL(dim)).toMap

    assert(ddlMap.contains("cache_advertiser"), "DDL not generated for hive table cache_advertiser")
    assert(!ddlMap.contains("oracle_advertiser"), "DDL should not be generated for oracle table oracle_advertiser")

    val ddl_cache_advertiser = ddlMap.get("cache_advertiser").get
    val sortedColsExpr = IndexedSeq("id tinyint", "mdm_company_name string", "mdm_id tinyint", "status string")
    assert(ddl_cache_advertiser.contains(sortedColsExpr.mkString(", \n")),
      "Columns are not generated in the predefined order")
  }

}
