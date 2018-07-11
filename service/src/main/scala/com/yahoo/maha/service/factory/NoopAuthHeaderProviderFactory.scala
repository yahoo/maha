package com.yahoo.maha.service.factory

import com.yahoo.maha.core.request.fieldExtended
import com.yahoo.maha.executor.druid.NoopAuthHeaderProvider
import com.yahoo.maha.service.{MahaServiceConfig, MahaServiceConfigContext}
import org.json4s.JValue
import scalaz.syntax.applicative._

class NoopAuthHeaderProviderFactory extends AuthHeaderProviderFactory {
  """
    |{
    |"domain" : "",
    |"service" :"",
    |"privateKeyName" : "",
    |"privateKeyId" : ""
    |}
  """.stripMargin

  override def fromJson(configJson: JValue)(implicit context: MahaServiceConfigContext): MahaServiceConfig.MahaConfigResult[NoopAuthHeaderProvider] = {
    import org.json4s.scalaz.JsonScalaz._
    val noopDomainResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("domain")(configJson)
    val noopServiceResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("service")(configJson)
    val noopPrivateKeyNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("privateKeyName")(configJson)
    val noopPrivateKeyIdResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("privateKeyId")(configJson)

    (noopDomainResult |@| noopServiceResult |@| noopPrivateKeyNameResult |@| noopPrivateKeyIdResult) {
      (_, _, _, _) => {
        new NoopAuthHeaderProvider
      }
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

