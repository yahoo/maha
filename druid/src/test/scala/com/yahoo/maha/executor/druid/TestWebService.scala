// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.executor.druid

import java.util.concurrent.atomic.AtomicBoolean

import org.http4s.{Header, HttpService}
import org.http4s.dsl._
/**
 * Created by vivekch on 4/8/16.
 */
trait TestWebService {
  val failFirstBoolean = new AtomicBoolean(false)
  val service = HttpService {
    case POST -> Root / ("uncoveredEmpty") =>
      val uncovered =
        """
          |  [
          |  {
          |    "timestamp" : "2012-01-01T00:00:00.000Z",
          |    "event" : {
          |      "Pricing Type" : 11,
          |      "Keyword ID": "10",
          |      "Average Bid": 9,
          |     "Max Bid": 163,
          |     "Impressions":175,
          |     "Conversions":15.0,
          |     "Min Bid":184,
          |     "Average Position":205,
          |     "Week":"2017-26",
          |     "show_sov_flag": "0",
          |     "Impression Share": 0.4567
          |    }
          |  },
          |  {
          |    "timestamp" : "2012-01-01T00:00:12.000Z",
          |    "event" : {
          |    "Pricing Type" : 13,
          |     "Keyword ID": "14",
          |     "Average Bid": 15,
          |     "Max Bid": 16,
          |     "Impressions":17,
          |     "Conversions":2.3,
          |     "Min Bid":18,
          |     "Average Position":20,
          |     "Week":"2017-28",
          |     "show_sov_flag": "1",
          |     "Impression Share": 0.0123
          |    }
          |  }
          |  ]
          |
          |
        """.stripMargin
      Ok(uncovered).putHeaders(Header("X-Druid-Response-Context", "{}"))

    case POST -> Root / ("uncoveredNonempty") =>
      val uncovered =
        """
          |  [
          |  {
          |    "timestamp" : "2012-01-01T00:00:00.000Z",
          |    "event" : {
          |      "Pricing Type" : 11,
          |      "Keyword ID": "10",
          |      "Average Bid": 9,
          |     "Max Bid": 163,
          |     "Impressions":175,
          |     "Conversions":15.0,
          |     "Min Bid":184,
          |     "Average Position":205,
          |     "Week":"2017-26",
          |     "show_sov_flag": "0",
          |     "Impression Share": 0.4567
          |    }
          |  },
          |  {
          |    "timestamp" : "2012-01-01T00:00:12.000Z",
          |    "event" : {
          |    "Pricing Type" : 13,
          |     "Keyword ID": "14",
          |     "Average Bid": 15,
          |     "Max Bid": 16,
          |     "Impressions":17,
          |     "Conversions":2.3,
          |     "Min Bid":18,
          |     "Average Position":20,
          |     "Week":"2017-28",
          |     "show_sov_flag": "1",
          |     "Impression Share": 0.0123
          |    }
          |  }
          |  ]
          |
          |
        """.stripMargin
      Ok(uncovered).putHeaders(Header("X-Druid-Response-Context", "{\"uncoveredIntervals\":[2017-11-13T21:00:00.000Z/2017-11-14T00:00:00.000Z\"],\"uncoveredIntervalsOverflowed\":false}"))

    case POST -> Root /("groupby") =>
      val groupby ="""[
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:00.000Z",
                     |    "event" : {
                     |      "Pricing Type" : 11,
                     |      "Keyword ID": "10",
                     |      "Average Bid": 9,
                     |     "Max Bid": 163,
                     |     "Impressions":175,
                     |     "Conversions":15.0,
                     |     "Min Bid":184,
                     |     "Keyword Value": 419,
                     |     "Average Position":205,
                     |     "Day":"20120101",
                     |     "Advertiser Status": "ON",
                     |     "show_sov_flag": "0",
                     |     "Impression Share": 0.4567
                     |    }
                     |  },
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:12.000Z",
                     |    "event" : {
                     |    "Pricing Type" : 13,
                     |     "Keyword ID": "14",
                     |     "Average Bid": 15,
                     |     "Max Bid": 16,
                     |     "Impressions":17,
                     |     "Conversions":2.3,
                     |     "Min Bid":18,
                     |     "Keyword Value": 19,
                     |     "Average Position":20,
                     |     "Day":"20120101",
                     |     "Advertiser Status": "ON",
                     |     "show_sov_flag": "1",
                     |     "Impression Share": 0.0123
                     |    }
                     |  }
                     |  ]""".stripMargin
      Ok(groupby)

    case POST -> Root /("groupby_empty_lookup") =>
      val groupby ="""[
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:00.000Z",
                     |    "event" : {
                     |      "Pricing Type" : 11,
                     |      "Keyword ID": "10",
                     |      "Average Bid": 9,
                     |     "Max Bid": 163,
                     |     "Impressions":175,
                     |     "Conversions":15.0,
                     |     "Min Bid":184,
                     |     "Keyword Value": 419,
                     |     "Average Position":205,
                     |     "Day":"20120101",
                     |     "Advertiser Status": "ON",
                     |     "show_sov_flag": "0",
                     |     "Impression Share": 0.4567
                     |    }
                     |  },
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:12.000Z",
                     |    "event" : {
                     |    "Pricing Type" : 13,
                     |     "Keyword ID": "14",
                     |     "Average Bid": 15,
                     |     "Max Bid": 16,
                     |     "Impressions":17,
                     |     "Conversions":2.3,
                     |     "Min Bid":18,
                     |     "Keyword Value": 19,
                     |     "Average Position":20,
                     |     "Day":"20120101",
                     |     "Advertiser Status": "MAHA_LOOKUP_EMPTY",
                     |     "show_sov_flag": "1",
                     |     "Impression Share": 0.0123
                     |    }
                     |  }
                     |  ]""".stripMargin
      Ok(groupby)

    case POST -> Root /("groupbybidmod") =>
      val groupby ="""[
                     |    {
                     |        "version": "v1",
                     |        "timestamp": "2017-08-07T00:00:00.000Z",
                     |        "event": {
                     |            "weighted_bid_usd": 878831.4919335842,
                     |            "Impressions": 2884485,
                     |            "Bid Mod": 8.999999637413556,
                     |            "weighted_bid_modifier": 74464998,
                     |            "Device Type": "'Desktop'",
                     |            "Modified Bid": 2.742078086296123,
                     |            "Advertiser ID": "5430",
                     |            "bid_mod_impressions": 8273889,
                     |            "Average Bid": 0.3046753551963641,
                     |            "Spend": 5793.1630783081055,
                     |            "Bid Modifier": 8.999999637413556,
                     |            "Day": "20170807"
                     |        }
                     |    },
                     |    {
                     |        "version": "v1",
                     |        "timestamp": "2017-08-07T00:00:00.000Z",
                     |        "event": {
                     |            "weighted_bid_usd": 811215.2593688965,
                     |            "Impressions": 2701909,
                     |            "Bid Mod": 8.997264297550355,
                     |            "weighted_bid_modifier": 71617594,
                     |            "Device Type": "'Desktop'",
                     |            "Modified Bid": 2.7013189899244656,
                     |            "Advertiser ID": "5430",
                     |            "bid_mod_impressions": 7959930,
                     |            "Average Bid": 0.30023781680615313,
                     |            "Spend": 5237.020672619343,
                     |            "Bid Modifier": 8.997264297550355,
                     |            "Day": "20170808"
                     |        }
                     |    },
                     |    {
                     |        "version": "v1",
                     |        "timestamp": "2017-08-07T00:00:00.000Z",
                     |        "event": {
                     |            "weighted_bid_usd": 681.9299856424332,
                     |            "Impressions": 2250,
                     |            "Bid Mod": 0,
                     |            "weighted_bid_modifier": 0,
                     |            "Device Type": "'SmartPhone'",
                     |            "Modified Bid": 0,
                     |            "Advertiser ID": "5430",
                     |            "bid_mod_impressions": 0,
                     |            "Average Bid": 0.3030799936188592,
                     |            "Spend": 1.4580000340938568,
                     |            "Bid Modifier": 0,
                     |            "Day": "20170807"
                     |        }
                     |    },
                     |    {
                     |        "version": "v1",
                     |        "timestamp": "2017-08-07T00:00:00.000Z",
                     |        "event": {
                     |            "weighted_bid_usd": 643.7700130343437,
                     |            "Impressions": 2138,
                     |            "Bid Mod": 0.19999999689559142,
                     |            "weighted_bid_modifier": 23.99999962747097,
                     |            "Device Type": "'SmartPhone'",
                     |            "Modified Bid": 0.06022170281026361,
                     |            "Advertiser ID": "5430",
                     |            "bid_mod_impressions": 120,
                     |            "Average Bid": 0.3011085187251374,
                     |            "Spend": 1.080000039190054,
                     |            "Bid Modifier": 0.19999999689559142,
                     |            "Day": "20170808"
                     |        }
                     |    }
                     |]
""".stripMargin
      Ok(groupby)

    case POST -> Root /("groupby_start_of_week") =>
      val groupby ="""[
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:00.000Z",
                     |    "event" : {
                     |      "Pricing Type" : 11,
                     |      "Keyword ID": "10",
                     |      "Average Bid": 9,
                     |     "Max Bid": 163,
                     |     "Impressions":175,
                     |     "Conversions":15.0,
                     |     "Min Bid":184,
                     |     "Average Position":205,
                     |     "Week":"2017-26",
                     |     "show_sov_flag": "0",
                     |     "Impression Share": 0.4567
                     |    }
                     |  },
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:12.000Z",
                     |    "event" : {
                     |    "Pricing Type" : 13,
                     |     "Keyword ID": "14",
                     |     "Average Bid": 15,
                     |     "Max Bid": 16,
                     |     "Impressions":17,
                     |     "Conversions":2.3,
                     |     "Min Bid":18,
                     |     "Average Position":20,
                     |     "Week":"2017-28",
                     |     "show_sov_flag": "1",
                     |     "Impression Share": 0.0123
                     |    }
                     |  }
                     |  ]""".stripMargin
      Ok(groupby)

    case POST -> Root /("groupby_start_of_month") =>
      val groupby ="""[
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:00.000Z",
                     |    "event" : {
                     |      "Pricing Type" : 11,
                     |      "Keyword ID": "10",
                     |      "Average Bid": 9,
                     |     "Max Bid": 163,
                     |     "Impressions":175,
                     |     "Conversions":15.0,
                     |     "Min Bid":184,
                     |     "Average Position":205,
                     |     "Month":"2017-06-01",
                     |     "show_sov_flag": "0",
                     |     "Impression Share": 0.4567
                     |    }
                     |  },
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:12.000Z",
                     |    "event" : {
                     |    "Pricing Type" : 13,
                     |     "Keyword ID": "14",
                     |     "Average Bid": 15,
                     |     "Max Bid": 16,
                     |     "Impressions":17,
                     |     "Conversions":2.3,
                     |     "Min Bid":18,
                     |     "Average Position":20,
                     |     "Month":"2017-07-01",
                     |     "show_sov_flag": "1",
                     |     "Impression Share": 0.0123
                     |    }
                     |  }
                     |  ]""".stripMargin
      Ok(groupby)

    case POST -> Root /("3rowsgroupby") =>
      val groupby ="""[
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:00.000Z",
                     |    "event" : {
                     |     "Country WOEID" : 1,
                     |     "Country Name" : "US",
                     |     "Pricing Type" : 11,
                     |     "Keyword ID": 1,
                     |     "Average Bid": 9,
                     |     "Max Bid": 163,
                     |     "Impressions":175,
                     |     "Min Bid":184,
                     |     "Keyword Value": 419,
                     |     "Average Position":205,
                     |     "Day":"20160101"
                     |    }
                     |  },
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:12.000Z",
                     |    "event" : {
                     |     "Country WOEID" : 1,
                     |     "Country Name" : "US",
                     |     "Pricing Type" : 13,
                     |     "Keyword ID": 2,
                     |     "Average Bid": 15,
                     |     "Max Bid": 16,
                     |     "Impressions":17,
                     |     "Min Bid":18,
                     |     "Keyword Value": 19,
                     |     "Average Position":20,
                     |     "Day":"20160101"
                     |    }
                     |  },
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:12.000Z",
                     |    "event" : {
                     |     "Country WOEID" : 1,
                     |     "Country Name" : "US",
                     |     "Pricing Type" : 13,
                     |     "Keyword ID": 3,
                     |     "Average Bid": 10,
                     |     "Max Bid": 160123456,
                     |     "Impressions":127,
                     |     "Min Bid":18,
                     |     "Keyword Value": 19,
                     |     "Average Position":20.12345,
                     |     "Day":"20160101"
                     |    }
                     |  }
                     |  ]""".stripMargin
      Ok(groupby)

    case POST -> Root / ("faultygroupby")=>

      val groupby="""{
                    |    "timestamp" : "2012-01-01T00:00:12.000Z",
                    |    "event" : {
                    |    "Pricing Type" : 13,
                    |     "Keyword ID": 14,
                    |     "Average Bid": 15,
                    |     "Max Bid": 16,
                    |     "Impressions":17.00,
                    |     "Min Bid":18,
                    |     "Keyword Value": 19,
                    |     "Average Position":20,
                    |     "Day":"20160101"
                    |    }
                    |  }""".stripMargin
      Ok(groupby)

    case POST -> Root / ("timeseries") =>

      val timeSeries =
        """[
          |  {
          |    "timestamp": "2012-01-01T00:00:00.000Z",
          |    "result": { "Impressions": 15,"Day": "20160111" }
          |  },
          |  {
          |   "timestamp": "2012-01-02T00:00:00.000Z",
          |   "result": { "Impressions": 16 ,"Day": "20160112"}
          |  },
          |  {
          |    "timestamp": "2012-01-03T00:00:00.000Z",
          |    "result": { "Impressions": 17,"Day":"20160113" }
          |  }
          |]""".stripMargin
      Ok(timeSeries)

    case POST -> Root/("faultytimeseries") =>

      val timeSeries =
        """[
          |  {
          |    "timestamp": "2012-01-01T00:00:00.000Z",
          |    "re": { "Impressions": 15,"Day": "20160111" }
          |  },
          |  {
          |   "timestamp": "2012-01-02T00:00:00.000Z",
          |   "res": { "Impressions": 16 ,"Day": "20160112"}
          |  },
          |  {
          |    "timestamp": "2012-01-03T00:00:00.000Z",
          |    "re": { "Impressions": 17,"Day":"20160113" }
          |  }
          |]""".stripMargin
      Ok(timeSeries)


    case POST -> Root /("topn") =>
      val topN = """[{
                   |	"timestamp": "2013-08-31T00:00:00.000Z",
                   |	"result": [{
                   |		"Keyword ID": 14,
                   |		"Impressions": 10669
                   |	},{
                   |		"Keyword ID": 13,
                   |		"Impressions": 106
                   |	}]
                   |}]""".stripMargin
      Ok(topN)

    case POST -> Root /("topnWithNull") =>
      val topN = """[{
                   |	"timestamp": "2013-08-31T00:00:00.000Z",
                   |	"result": [{
                   |		"Keyword ID": null,
                   |		"Impressions": 10669
                   |	},{
                   |		"Keyword ID": null,
                   |		"Impressions": 106
                   |	}]
                   |}]""".stripMargin
      Ok(topN)


    case POST -> Root /  ("druidPlusOraclegroupby") =>
      val groupby ="""[
                     |  {
                     |    "timestamp" : "2013-08-31T00:00:00.000Z",
                     |    "event" : {
                     |      "Keyword ID": 14,
                     |     "Impressions":10669
                     |    }
                     |  },
                     |  {
                     |    "timestamp" : "2013-08-31T00:00:00.000Z",
                     |    "event" : {
                     |     "Keyword ID": 13,
                     |     "Impressions":106
                     |    }
                     |  }
                     |  ]""".stripMargin
      Ok(groupby)

    case POST -> Root / ("faultytopn")=>
      val topN = """[{
                   |	"timestamp": "2013-08-31T00:00:00.000Z",
                   |	"result": [{
                   |		"Keyword ID": null,
                   |    "Keyword Value":1,
                   |		"Impressions": 10669
                   |	},{
                   |		"Keyword ID": [{
                   |	"timestamp": "2013-08-31T00:00:00.000Z",
                   |	"result": [{
                   |		"Keyword ID": 14,
                   |    "Keyword Value":1,
                   |		"Impressions": 10669
                   |	},{
                   |		"Keyword ID": 13,
                   |		"Impressions": 106,
                   |    "Keyword Value":30
                   |	}]
                   |}],
                   |		"Impressions": 106,
                   |    "Keyword Value":30
                   |	}]
                   |}]""".stripMargin
      Ok(topN)

    case GET -> Root /("topn") =>
      val topN = """[{
                   |	"timestamp": "2013-08-31T00:00:00.000Z",
                   |	"result": [{
                   |		"Keyword ID": 14,
                   |    "Keyword Value":1,
                   |		"Impressions": 10669
                   |	},{
                   |		"Keyword ID": 13,
                   |		"Impressions": 106,
                   |    "Keyword Value":30
                   |	}]
                   |}]""".stripMargin
      Ok(topN)

    case POST -> Root /("adjustmentStatsGroupBy") =>
      val groupby ="""[
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:00.000Z",
                     |    "event" : {
                     |     "Impressions":100,
                     |     "Spend":15.0,
                     |     "Advertiser ID":184,
                     |     "Day":"2012-01-01"
                     |    }
                     |  },
                     |  {
                     |    "timestamp" : "2012-01-01T00:00:12.000Z",
                     |    "event" : {
                     |     "Impressions":100,
                     |     "Spend":10.0,
                     |     "Advertiser ID":199,
                     |     "Day":"2012-01-01"
                     |    }
                     |  }
                     |  ]""".stripMargin
      Ok(groupby)

    case POST -> Root /("whiteGloveGroupBy") =>
      val groupby =
        """
          |[
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":100702.5269201994,
          |         "Day":"20170502",
          |         "Hour":"12"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":100105.47737568617,
          |         "Day":"20170501",
          |         "Hour":"10"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":99452.79603540897,
          |         "Day":"20170502",
          |         "Hour":"11"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":99278.75371289253,
          |         "Day":"20170501",
          |         "Hour":"12"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":97812.48666465282,
          |         "Day":"20170501",
          |         "Hour":"11"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":97590.620177567,
          |         "Day":"20170502",
          |         "Hour":"10"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":97567.92169524357,
          |         "Day":"20170501",
          |         "Hour":"13"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":96825.4939264059,
          |         "Day":"20170502",
          |         "Hour":"13"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":94937.976395607,
          |         "Day":"20170501",
          |         "Hour":"14"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":93734.20419290662,
          |         "Day":"20170502",
          |         "Hour":"15"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":93478.08122582361,
          |         "Day":"20170502",
          |         "Hour":"14"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":93372.62983252853,
          |         "Day":"20170501",
          |         "Hour":"15"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":92731.11964201927,
          |         "Day":"20170502",
          |         "Hour":"16"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":92407.92718720436,
          |         "Day":"20170502",
          |         "Hour":"09"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":92231.04085025191,
          |         "Day":"20170501",
          |         "Hour":"16"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":91216.1552691944,
          |         "Day":"20170501",
          |         "Hour":"09"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":86134.26635608822,
          |         "Day":"20170501",
          |         "Hour":"17"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":83792.81063309312,
          |         "Day":"20170502",
          |         "Hour":"17"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":81854.47675731778,
          |         "Day":"20170501",
          |         "Hour":"08"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":80682.35567551851,
          |         "Day":"20170502",
          |         "Hour":"08"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":78679.26031523943,
          |         "Day":"20170501",
          |         "Hour":"18"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":78148.13511765003,
          |         "Day":"20170502",
          |         "Hour":"18"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":74984.74295082316,
          |         "Day":"20170502",
          |         "Hour":"19"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":74374.6146068573,
          |         "Day":"20170501",
          |         "Hour":"19"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":70020.28937721252,
          |         "Day":"20170501",
          |         "Hour":"20"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":69916.24665260315,
          |         "Day":"20170502",
          |         "Hour":"20"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":68890.52107673883,
          |         "Day":"20170501",
          |         "Hour":"07"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":67616.82631015778,
          |         "Day":"20170502",
          |         "Hour":"07"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":64026.77501773834,
          |         "Day":"20170501",
          |         "Hour":"21"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":61437.951813697815,
          |         "Day":"20170502",
          |         "Hour":"21"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":54657.061321258545,
          |         "Day":"20170501",
          |         "Hour":"22"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":54380.0269228816,
          |         "Day":"20170501",
          |         "Hour":"00"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":52897.4322437644,
          |         "Day":"20170502",
          |         "Hour":"06"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":52755.80685228109,
          |         "Day":"20170502",
          |         "Hour":"00"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":52233.05034738779,
          |         "Day":"20170501",
          |         "Hour":"06"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":50291.44211769104,
          |         "Day":"20170502",
          |         "Hour":"22"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":41530.911318697035,
          |         "Day":"20170501",
          |         "Hour":"23"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":41114.491149783134,
          |         "Day":"20170502",
          |         "Hour":"01"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":40357.94918587804,
          |         "Day":"20170501",
          |         "Hour":"01"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":39912.34553909302,
          |         "Day":"20170502",
          |         "Hour":"05"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":39016.80207681656,
          |         "Day":"20170502",
          |         "Hour":"23"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":39015.84096670151,
          |         "Day":"20170501",
          |         "Hour":"05"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":35015.9358971715,
          |         "Day":"20170501",
          |         "Hour":"02"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":34740.93743869662,
          |         "Day":"20170502",
          |         "Hour":"02"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":33221.94006937742,
          |         "Day":"20170502",
          |         "Hour":"04"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":32514.756277024746,
          |         "Day":"20170501",
          |         "Hour":"04"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":31368.691083669662,
          |         "Day":"20170502",
          |         "Hour":"03"
          |      }
          |   },
          |   {
          |      "version":"v1",
          |      "timestamp":"2017-04-30T00:00:00.000Z",
          |      "event":{
          |         "Spend Usd":31223.54076075554,
          |         "Day":"20170501",
          |         "Hour":"03"
          |      }
          |   }
          |]
        """.stripMargin
      Ok(groupby)

    case POST -> Root /("failFirst") =>
      if(!failFirstBoolean.get()) {
        failFirstBoolean.set(true)
        InternalServerError("fail first try!")
      } else {
        Ok("not fail now")
      }
    case POST -> Root / "fail" =>
      InternalServerError("always fail!")
  }

}
