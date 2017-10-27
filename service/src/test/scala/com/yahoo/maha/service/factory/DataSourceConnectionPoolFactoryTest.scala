// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.factory

import java.util.UUID
import javax.sql.DataSource
import com.yahoo.maha.jdbc.JdbcConnection
import com.zaxxer.hikari.HikariDataSource
import org.json4s.jackson.JsonMethods._
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by pranavbhole on 31/05/17.
 */
class DataSourceConnectionPoolFactoryTest extends BaseFactoryTest{

  test("Test Creation of HikariDataSource") {
    val uuid = UUID.randomUUID().toString.replace("-","")
    val jsonString =   s"""
                         |{
                         |"driverClassName" : "org.h2.Driver",
                         |"jdbcUrl" : "jdbc:h2:mem:$uuid;MODE=Oracle;DB_CLOSE_DELAY=-1",
                         |"username" : "sa",
                         |"passwordProviderFactoryClassName" : "com.yahoo.maha.service.factory.PassThroughPasswordProviderFactory",
                         |"passwordProviderConfig" : [{"key" : "value"}],
                         |"passwordKey" : "h2.test.database.password",
                         |"poolName" : "test-pool",
                         |"maximumPoolSize" : 10,
                         |"minimumIdle" : 1,
                         |"autoCommit": true,
                         |"connectionTestQuery" : "SELECT 1 FROM DUAL",
                         |"validationTimeout" : 1000000,
                         |"idleTimeout" : 1000000,
                         |"maxLifetime" : 10000000,
                         |"dataSourceProperties": [{"key": "propertyKey" , "value": "propertyValue"}]
                         |}
                       """.stripMargin

    val factoryResult =  getFactory[DataSourceFactory]("com.yahoo.maha.service.factory.HikariDataSourceFactory", closer)
    assert(factoryResult.isSuccess)
    val factory = factoryResult.toOption.get
    val json = parse(jsonString)
    val generatorResult = factory.fromJson(json)
    assert(generatorResult.isSuccess, generatorResult)
    assert(generatorResult.toList.head.isInstanceOf[DataSource])
    generatorResult.foreach {
      ds=>
        val connection = new JdbcConnection(ds)
        assert(ds.asInstanceOf[HikariDataSource].getIdleTimeout == 1000000)
        assert(ds.asInstanceOf[HikariDataSource].getPoolName == "test-pool")
        val ddlResult = connection.executeUpdate("create table test(key varchar(20), value varchar(20));")
        assert(ddlResult.isSuccess)
    }
  }
}