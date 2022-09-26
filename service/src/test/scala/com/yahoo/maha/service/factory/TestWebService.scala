package com.yahoo.maha.service.factory

import org.http4s.{Header, HttpService}
import cats.effect.IO
import org.http4s.dsl.io._

import java.util.concurrent.atomic.AtomicBoolean

trait TestWebService {

  val failFirstBoolean = new AtomicBoolean(false)
  val service = HttpService[IO] {
    case POST -> Root / ("alternative_endpoint") =>
      val groupby =
        """[
          |  {
          |    "timestamp" : "2012-01-01T00:00:00.000Z",
          |    "event" : {
          |     "Day" : "2022-01-01",
          |     "Campaign ID": 12345,
          |     "Impressions":17,
          |     "Pricing Type": "Sure, why not.",
          |     "Student ID": 213,
          |     "Marks Obtained": 867
          |    }
          |  }
          |  ]""".stripMargin
      Ok(groupby)

    case POST -> Root / ("alternative_endpoint_2") =>
      val groupby =
        """[
          |  {
          |    "timestamp" : "2012-01-01T00:00:00.000Z",
          |    "event" : {
          |     "Day" : "2022-01-01",
          |     "Campaign ID": 12345,
          |     "Impressions":17,
          |     "Pricing Type": "Sure, why not.",
          |     "Student ID": 213,
          |     "Marks Obtained": 5309
          |    }
          |  }
          |  ]""".stripMargin
      Ok(groupby)
  }

}
