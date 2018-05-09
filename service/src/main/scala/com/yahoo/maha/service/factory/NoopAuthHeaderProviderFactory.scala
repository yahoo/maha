package com.yahoo.maha.service.factory

import com.yahoo.maha.core.request.fieldExtended
import com.yahoo.maha.executor.druid.NoopAuthHeaderProvider
import com.yahoo.maha.service.MahaServiceConfig
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

  override def fromJson(configJson: JValue): MahaServiceConfig.MahaConfigResult[NoopAuthHeaderProvider] = {
    import org.json4s.scalaz.JsonScalaz._
    val athensDomainResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("domain")(configJson)
    val athensServiceResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("service")(configJson)
    val athensPrivateKeyNameResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("privateKeyName")(configJson)
    val athensPrivateKeyIdResult: MahaServiceConfig.MahaConfigResult[String] = fieldExtended[String]("privateKeyId")(configJson)

    (athensDomainResult |@| athensServiceResult |@| athensPrivateKeyNameResult |@| athensPrivateKeyIdResult) {
      (_, _, _, _) => {
        new NoopAuthHeaderProvider
      }
    }
  }

  override def supportedProperties: List[(String, Boolean)] = List.empty
}

