// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package loader

class DruidManagerLoader extends ApplicationLoader {

 override def load(context:Context):Application = {
   new ApplicationComponents(context).application
 }

  class ApplicationComponents(context: Context) extends BuiltInComponentsFromContext(context) with I18nComponents with AssetsComponents {

    //APIs for Druid
    val druidCoordinator = context.initialConfiguration.getString("druid.coordinator").getOrElse(throw new UnsupportedOperationException("druid.coordinator not defined in config"))
    val druidIndexer = context.initialConfiguration.getString("druid.indexer").getOrElse(throw new UnsupportedOperationException("druid.indexer not defined in config"))
    val druidBroker = context.initialConfiguration.getString("druid.broker").getOrElse(throw new UnsupportedOperationException("druid.broker not defined in config"))
    val druidHistoricalsHttpScheme = context.initialConfiguration.getString("druid.historicals.http.scheme").getOrElse("https")
    val wsTimeout = context.initialConfiguration.getInt("mgr.ws.timeout.time").getOrElse(throw new UnsupportedOperationException("mgr.ws.timeout.time not defined in config"))

    val client = AhcWSClient()

    val authConfig: Map[String, String] = context.initialConfiguration.get[Map[String, String]]("auth.config")
    val authValidator: AuthValidator = Class.forName(authConfig.getOrElse("authValidatorClass", throw new IllegalStateException("authValidatorClass is not defined"))).newInstance().asInstanceOf[AuthValidator]
    authValidator.init(context.initialConfiguration)

    val authFilter = new AuthenticationFilter(authValidator)
    override lazy val httpFilters = Seq(authFilter)

    val druidAuthHeaderProviderConfig: Map[String, String] = context.initialConfiguration.get[Map[String, String]]("druid.auth.header.provider.config")
    val druidAuthHeaderProvider: DruidAuthHeaderProvider = Class.forName(druidAuthHeaderProviderConfig.getOrElse("druidAuthHeaderProviderClass", throw new IllegalStateException("druidAuthHeaderProviderClass is not defined"))).newInstance().asInstanceOf[DruidAuthHeaderProvider]
    druidAuthHeaderProvider.init(context.initialConfiguration)

    val druidLookupConfig: Map[String, String] = context.initialConfiguration.get[Map[String, String]]("druid.lookup")

    val lookupCountSql: Option[String] = druidLookupConfig.get("countSql")
    val lookupTimestampSql: Option[String] = druidLookupConfig.get("timestampSql")
    val hikariMaximumPoolSize: Int = context.initialConfiguration.getOptional[Int]("custom.hikaricp.maximumPoolSize").getOrElse(10)

    val jdbcConnectionToGetLookupCount = if(druidLookupConfig.get("db.url.for.countSql").isDefined &&
      druidLookupConfig.get("db.password.for.countSql").isDefined &&
      druidLookupConfig.get("db.username.for.countSql").isDefined) {
      val passwordProvider: PasswordProvider = Class.forName(druidLookupConfig.getOrElse("passwordProviderClass", throw new IllegalStateException("passwordProviderClass is not defined"))).newInstance().asInstanceOf[PasswordProvider]
      getJdbcConnection(druidLookupConfig("db.url.for.countSql"), druidLookupConfig("db.username.for.countSql"), passwordProvider.getPassword(druidLookupConfig("db.password.for.countSql")), hikariMaximumPoolSize)
    } else {
      None
    }

    val jdbcConnectionToGetLookupTimeStamp = if(druidLookupConfig.get("db.url.for.timestampSql").isDefined &&
      druidLookupConfig.get("db.password.for.timestampSql").isDefined &&
      druidLookupConfig.get("db.username.for.timestampSql").isDefined) {
      val passwordProvider: PasswordProvider = Class.forName(druidLookupConfig.getOrElse("passwordProviderClass", throw new IllegalStateException("passwordProviderClass is not defined"))).newInstance().asInstanceOf[PasswordProvider]
      getJdbcConnection(druidLookupConfig("db.url.for.timestampSql"), druidLookupConfig("db.username.for.timestampSql"), passwordProvider.getPassword(druidLookupConfig("db.password.for.timestampSql")), hikariMaximumPoolSize)
    } else {
      None
    }

    lazy val dashboard = new controllers.DashBoard(
      client,
      druidCoordinator,
      druidIndexer,
      druidBroker,
      druidHistoricalsHttpScheme,
      jdbcConnectionToGetLookupCount,
      lookupCountSql,
      jdbcConnectionToGetLookupTimeStamp,
      lookupTimestampSql,
      druidAuthHeaderProvider,
      authValidator
    )

    private[this] lazy val assetsController = new controllers.Assets(httpErrorHandler, assetsMetadata)

    override def router: Router = new Routes(
      httpErrorHandler,
      dashboard,
      assetsController
    )

    private def getJdbcConnection(jdbcUrl: String, dbUsername: String, dbPassword: String, maximumPoolSize: Int) = {
      val dbConnDetails = new DatabaseConnDetails(jdbcUrl, dbUsername, dbPassword, maximumPoolSize)
      val dataSource = HikariCpDataSource.get(dbConnDetails)
      dataSource.map(new JdbcConnection(_))
    }

  }
}
