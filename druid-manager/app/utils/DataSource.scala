// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package utils

import java.util.Properties

import com.zaxxer.hikari.{HikariDataSource, HikariConfig}

case class DatabaseConnDetails (jdbcUrl: String, dbUsername: String, dbPassword: String, maxPoolSize: Int)

object HikariCpDataSource {

  private var dataSource: Option[HikariDataSource] = None

  def get(databaseConnDetails: DatabaseConnDetails): Option[HikariDataSource] = {

    val properties = new Properties()
    properties.put("oracle.net.CONNECT_TIMEOUT",5000.asInstanceOf[AnyRef])
    properties.put("oracle.jdbc.ReadTimeout",60000.asInstanceOf[AnyRef])

    val config = new HikariConfig()
    config.setJdbcUrl(databaseConnDetails.jdbcUrl)
    config.setUsername(databaseConnDetails.dbUsername)
    config.setPassword(databaseConnDetails.dbPassword)
    config.setMaximumPoolSize(databaseConnDetails.maxPoolSize)
    config.setValidationTimeout(1000)
    config.setMaxLifetime(1800000)
    config.setConnectionTimeout(60000)
    config.setIdleTimeout(60000)
    config.setDataSourceProperties(properties)

    Option(new HikariDataSource(config))
  }

  def close() = {
      dataSource.foreach(_.close())
  }

}



