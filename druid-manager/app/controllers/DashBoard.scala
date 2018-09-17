// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package controllers

import java.text.SimpleDateFormat

import com.yahoo.maha.core.auth.{AuthValidator, DruidAuthHeaderProvider}
import com.yahoo.maha.jdbc.JdbcConnection
import controllers.bean.{DataSourceMetrics, SegmentMetrics}
import javax.inject.Inject
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import play.api.Logger
import play.api.libs.json._
import play.api.libs.ws._
import play.api.mvc._
import play.api.routing.JavaScriptReverseRouter

import scala.collection.immutable.{ListMap, Map}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class DashBoard  @Inject() (ws:WSClient, druidCoordinator: String,
                            druildIndexer: String,
                            druidBroker : String,
                            druidHistoricalsHttpScheme: String,
                            jdbcConnectionToGetLookupCount: Option[JdbcConnection],
                            lookupCountSql: Option[String],
                            jdbcConnectionToGetLookupTimeStamp: Option[JdbcConnection],
                            lookupTimestampSql: Option[String],
                            druidAuthHeaderProvider: DruidAuthHeaderProvider,
                            authValidator: AuthValidator) extends Controller {

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global
  private val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private val N_A = "N/A"

  def dataSourceDashboard = Action.async{request =>
    Logger.info("running segmentsDashboard");
    // parsing input parameters
    val params: Map[String, Seq[String]] = request.queryString;
    var dataSourceOption = params.get("dataSources").map {seq => seq.apply(0);}
    val startDateOption = params.get("startDate").map {seq => seq.apply(0);}
    val endDateOption = params.get("endDate").map {seq => seq.apply(0);}

    Logger.info("ws call started: " + druidCoordinator + "/druid/coordinator/v1/datasources");
    val headers = druidAuthHeaderProvider.getAuthHeaders
    val dataSourcesInfoRespFuture = ws.url(druidCoordinator + "/druid/coordinator/v1/datasources").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();

    // Async
    val resultFuture:Future[play.twirl.api.HtmlFormat.Appendable]= dataSourcesInfoRespFuture.flatMap {
      dataSourcesInfoResult =>  Logger.info("ws call processing rsp");
        // PREPARING DATA SOURCES LIST RESULT
        val dataSources: Seq[String] = dataSourcesInfoResult.json.as[JsArray].value.map(orgValue => orgValue.as[String]);

        // default selected data source to the first data source if not available
        if (!dataSourceOption.isDefined || dataSourceOption.get.isEmpty) {
          dataSourceOption = Option(dataSources.apply(0));
        }

        Logger.info("ws call started: " + druidCoordinator + "/druid/coordinator/v1/datasources/" + dataSourceOption.get + "/segments/?full");
        val segmentsInfoRespFuture = ws.url(druidCoordinator + "/druid/coordinator/v1/datasources/" + dataSourceOption.get + "/segments/?full").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();
        Logger.info("ws call started: " + druildIndexer + "/druid/indexer/v1/runningTasks");
        val runningTasksInfoRespFuture = ws.url(druildIndexer + "/druid/indexer/v1/runningTasks").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();
        Logger.info("ws call started: " + druildIndexer + "/druid/indexer/v1/pendingTasks");
        val pendingTasksInfoRespFuture = ws.url(druildIndexer + "/druid/indexer/v1/pendingTasks").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();
        Logger.info("ws call started: " + druildIndexer + "/druid/indexer/v1/waitingTasks");
        val waitingTasksInfoRespFuture = ws.url(druildIndexer + "/druid/indexer/v1/waitingTasks").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();
        Logger.info("ws call started: " + druildIndexer + "/druid/indexer/v1/completeTasks");
        val completeTasksInfoRespFuture = ws.url(druildIndexer + "/druid/indexer/v1/completeTasks").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();

        //Async
        val future =
          for {
            segmentsInfoResult <- segmentsInfoRespFuture
            runningTasksInfoResult <- runningTasksInfoRespFuture
            pendingTasksInfoResult <- pendingTasksInfoRespFuture
            waitingTasksInfoResult <- waitingTasksInfoRespFuture
            completeTasksInfoResult <- completeTasksInfoRespFuture
          } yield {
            Logger.info("ws call processing rsp");

            /** PROCESSING: segmentsInfoResult */
            // GENERATE SEGMENTS METRICS MAP
            val segmentsMetricsByDateByShard: Map[String, Map[Int, SegmentMetrics]] =
              segmentsInfoResult.json.as[JsArray].value
                // filter based on input time window
                .filter(json =>
                (!startDateOption.isDefined || startDateOption.get.isEmpty || DateTime.parse(startDateOption.get + "+00:00").getMillis <= DateTime.parse((json \ "interval").as[String].split("\\/")(0)).getMillis)
                  &&
                  (!endDateOption.isDefined || endDateOption.get.isEmpty || DateTime.parse(endDateOption.get + "+00:00").getMillis >= DateTime.parse((json \ "interval").as[String].split("\\/")(0)).getMillis)
                )
                // transform Seq[JsValue] to Map[String, Seq[JsValue]] where String = Date
                .groupBy(json => (json \ "interval").as[String].split("\\/")(0))
                // transform Map[String, Seq[JsValue]] to Map[(String, Map[Int, SegmentMetrics]] : Map[Date, Map[Shard#, SegmentMetrics]]
                .map(keyVal =>
                  keyVal._1 -> keyVal._2.map(json =>
                    (json \ "identifier").as[String].last match {
                      case 'Z' => 0 -> new SegmentMetrics((json \ "size").as[Long]);
                      case _ => (json \ "identifier").as[String].split("_").last.toInt -> new SegmentMetrics((json \ "size").as[Long]);
                    }
                  ).toMap
                );


            // PREPARING SEGMENTS RESULTS
            // sort segmentsMetricsByDateByShard by Date and by Shard#
            val segmentsMetrcisByDateByShardSorted = ListMap(segmentsMetricsByDateByShard.map(keyVal => keyVal._1 -> ListMap(keyVal._2.toSeq.sortBy(_._1): _*)).toSeq.sortBy(_._1): _*);

            // 1. TOTAL DATA SOURCE SIZE - FORMATTED
            val dataSourceMetrics: DataSourceMetrics = new DataSourceMetrics(segmentsMetrcisByDateByShardSorted.foldLeft(0L)((topAcc, topKV) => topAcc + topKV._2.foldLeft(0L)((acc, kv) => acc + kv._2.size)));
            val formattedTotalDataSourceSize =
              if (dataSourceMetrics.size < 1024L) {("" + dataSourceMetrics.size + " (Bytes)")}
              else if (dataSourceMetrics.size < (1024L * 1024L)) {("" + (math floor dataSourceMetrics.size.toDouble/1024.0*100)/100 + " (KB)")}
              else if (dataSourceMetrics.size < (1024L * 1024L * 1024L)) {("" + (math floor dataSourceMetrics.size.toDouble/(1024.0 * 1024.0)*100)/100 + " (MB)")}
              else {("" + (math floor dataSourceMetrics.size.toDouble/(1024.0 * 1024.0 * 1024.0)*100)/100 + " (GB)")}

            // 2. CVS FOR SEGMENTS METRICS BY DATES
            var segmentsMetricsByDatesCvs: String = "Date,Size";
            for (date <- segmentsMetrcisByDateByShardSorted.keySet) {
              segmentsMetricsByDatesCvs += "\\n" + date + "," + segmentsMetrcisByDateByShardSorted.get(date).get.foldLeft(0L)((acc, kv) => acc + kv._2.size);
            }

            // 3. CVS FOR SEGMENTS METRICS BY DATES BY SHARDS
            val maxShardCount: Int = segmentsMetrcisByDateByShardSorted.foldLeft(0)((maxCount, kv) => if (kv._2.size > maxCount) kv._2.size else maxCount);
            var segmentsMetricsByDatesByShardsCvs: String = "Date";
            for (i <- (maxShardCount - 1) to 0 by -1) {
              segmentsMetricsByDatesByShardsCvs += ",Shard_" + i.toString;
            }
            for (date <- segmentsMetrcisByDateByShardSorted.keySet) {
              segmentsMetricsByDatesByShardsCvs += "\\n" + date;
              for (i <- (maxShardCount - 1) to 0 by -1) {
                segmentsMetricsByDatesByShardsCvs += ",";
                if (segmentsMetrcisByDateByShardSorted.get(date).get.get(i).isDefined) {
                  segmentsMetricsByDatesByShardsCvs += segmentsMetrcisByDateByShardSorted.get(date).get.get(i).get.size.toString;
                } else {
                  segmentsMetricsByDatesByShardsCvs += "0";
                }
              }
            }

            /** PROCESSING: runningTasksInfoResult */
            val dataSourcePrefixPattern = "index_realtime_";
            val dataSourceSuffixPattern = "_\\d{4}(.*)"
            val datePrefixPattern = "index_realtime_" + dataSourceOption.get + "_";
            val dateSuffixPattern = "Z(.*)"
            // GENERATE RUNNING TASKS METRICS MAP
            val runningTasksCountByDate: Map[String, Int] =
              runningTasksInfoResult.json.as[JsArray].value
              // filter based on input data source and input time window
              .filter(json =>
                (json \ "id").as[String].replaceFirst(dataSourcePrefixPattern, "").replaceFirst(dataSourceSuffixPattern, "") == dataSourceOption.get
                &&
                (!startDateOption.isDefined || startDateOption.get.isEmpty || DateTime.parse(startDateOption.get + "+00:00").getMillis <= DateTime.parse((json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, "")).getMillis)
                &&
                (!endDateOption.isDefined || endDateOption.get.isEmpty || DateTime.parse(endDateOption.get + "+00:00").getMillis >= DateTime.parse((json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, "")).getMillis)
              )
              // group by the date extracted from id field
              .groupBy(json => (json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, ""))
                .map(keyVal => keyVal._1 ->  keyVal._2.size);

            /** PROCESSING: pendingTasksInfoResult */
            // GENERATE PENDING TASKS METRICS MAP
            val pendingTasksCountByDate: Map[String, Int] =
              pendingTasksInfoResult.json.as[JsArray].value
                // filter based on input data source and input time window
                .filter(json =>
                  (json \ "id").as[String].replaceFirst(dataSourcePrefixPattern, "").replaceFirst(dataSourceSuffixPattern, "") == dataSourceOption.get
                  &&
                  (!startDateOption.isDefined || startDateOption.get.isEmpty || DateTime.parse(startDateOption.get + "+00:00").getMillis <= DateTime.parse((json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, "")).getMillis)
                  &&
                  (!endDateOption.isDefined || endDateOption.get.isEmpty || DateTime.parse(endDateOption.get + "+00:00").getMillis >= DateTime.parse((json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, "")).getMillis)
                )
                // group by the date extracted from id field
                .groupBy(json => (json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, ""))
                .map(keyVal => keyVal._1 ->  keyVal._2.size);

            /** PROCESSING: waitingTasksInfoResult */
            // GENERATE WAITING TASKS METRICS MAP
            val waitingTasksCountByDate: Map[String, Int] =
              waitingTasksInfoResult.json.as[JsArray].value
                // filter based on input data source and input time window
                .filter(json =>
                  (json \ "id").as[String].replaceFirst(dataSourcePrefixPattern, "").replaceFirst(dataSourceSuffixPattern, "") == dataSourceOption.get
                  &&
                  (!startDateOption.isDefined || startDateOption.get.isEmpty || DateTime.parse(startDateOption.get + "+00:00").getMillis <= DateTime.parse((json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, "")).getMillis)
                  &&
                  (!endDateOption.isDefined || endDateOption.get.isEmpty || DateTime.parse(endDateOption.get + "+00:00").getMillis >= DateTime.parse((json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, "")).getMillis)
                )
                // group by the date extracted from id field
                .groupBy(json => (json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, ""))
                .map(keyVal => keyVal._1 ->  keyVal._2.size);

            /** PROCESSING: completeTasksInfoResult */
            // GENERATE SUCCESS TASKS METRICS MAP
            val successTasksCountByDate: Map[String, Int] =
              completeTasksInfoResult.json.as[JsArray].value
                // filter based on statusCode = "SUCCESS" and input data source and input time window
                .filter(json =>
                  (json \ "statusCode").as[String] == "SUCCESS"
                  &&
                  (json \ "id").as[String].replaceFirst(dataSourcePrefixPattern, "").replaceFirst(dataSourceSuffixPattern, "") == dataSourceOption.get
                  &&
                  (!startDateOption.isDefined || startDateOption.get.isEmpty || DateTime.parse(startDateOption.get + "+00:00").getMillis <= DateTime.parse((json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, "")).getMillis)
                  &&
                  (!endDateOption.isDefined || endDateOption.get.isEmpty || DateTime.parse(endDateOption.get + "+00:00").getMillis >= DateTime.parse((json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, "")).getMillis)
                )
                // group by the date extracted from id field
                .groupBy(json => (json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, ""))
                .map(keyVal => keyVal._1 ->  keyVal._2.size);

            // GENERATE FAILED TASKS METRICS MAP
            val failedTasksCountByDate: Map[String, Int] =
              completeTasksInfoResult.json.as[JsArray].value
                // filter based on statusCode != "SUCCESS" and input data source and input time window
                .filter(json =>
                (json \ "statusCode").as[String] != "SUCCESS"
                  &&
                  (json \ "id").as[String].replaceFirst(dataSourcePrefixPattern, "").replaceFirst(dataSourceSuffixPattern, "") == dataSourceOption.get
                  &&
                  (!startDateOption.isDefined || startDateOption.get.isEmpty || DateTime.parse(startDateOption.get + "+00:00").getMillis <= DateTime.parse((json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, "")).getMillis)
                  &&
                  (!endDateOption.isDefined || endDateOption.get.isEmpty || DateTime.parse(endDateOption.get + "+00:00").getMillis >= DateTime.parse((json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, "")).getMillis)
                )
                // group by the date extracted from id field
                .groupBy(json => (json \ "id").as[String].replaceFirst(datePrefixPattern, "").replaceFirst(dateSuffixPattern, ""))
                .map(keyVal => keyVal._1 ->  keyVal._2.size);

            // MERGING ALL TASKS METRICS MAPS
            // runningTasksCountByDate pendingTasksCountByDate waitingTasksCountByDate successTasksCountByDate failedTasksCountByDate
            val taskKeysUnion: List[String] =
              runningTasksCountByDate.keySet
                .union(pendingTasksCountByDate.keySet)
                .union(waitingTasksCountByDate.keySet)
                .union(successTasksCountByDate.keySet)
                .union(failedTasksCountByDate.keySet)
                .toList.sorted.reverse;

            // PREPARING TASKS METRICS MAP
            // sort by date
            val tasksCountByDateSorted = ListMap(runningTasksCountByDate.toSeq.sortBy(_._1): _*);

            /** PUSHING THE RESULT */
            Logger.info("pushing result");
            views.html.dataSourceDash(
              dataSourceOption
              , startDateOption
              , endDateOption
              , dataSources
              , formattedTotalDataSourceSize
              , segmentsMetricsByDatesCvs
              , segmentsMetricsByDatesByShardsCvs
              , taskKeysUnion
              , runningTasksCountByDate
              , pendingTasksCountByDate
              , waitingTasksCountByDate
              , successTasksCountByDate
              , failedTasksCountByDate
            );
          }
        future
    }
    resultFuture.map(i => Ok(i))
  }

  def getDataSource = Action.async{ request =>

    Logger.info("Getting all the data soruces")
    val headers = druidAuthHeaderProvider.getAuthHeaders
    val dataSourceFuture = ws.url(druidCoordinator + "/druid/coordinator/v1/datasources").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get()
    val futureResponse = dataSourceFuture.map(data => {
      val dataSources: Seq[String] = data.json.as[JsArray].value.map(orgValue => orgValue.as[String])
      views.html.querySegmentDash(dataSources)
    }
    )
    futureResponse.map(i => Ok(i))

  }

  def getDimensions(datasource : String ) = Action.async{ request =>
    Logger.info(s"Fetching dimensions for the data source $datasource ")
    val headers = druidAuthHeaderProvider.getAuthHeaders
    val dimensionFuture = ws.url(s"$druidBroker/druid/v2/datasources/$datasource/dimensions").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get()
    val futureResponse = dimensionFuture.map(data =>{
      val dimensions = data.body
      dimensions
    })
    futureResponse.map( i => Ok(i))

  }

  def getMetrics(datasource : String ) = Action.async{ request =>
    Logger.info(s"Fetching dimensions for the data source $datasource ")
    val headers = druidAuthHeaderProvider.getAuthHeaders
    val dimensionFuture = ws.url(s"$druidBroker/druid/v2/datasources/$datasource/metrics").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get()
    val futureResponse = dimensionFuture.map(data =>{
      val metrics = data.body
      metrics
    })
    futureResponse.map( i => Ok(i))
  }

  def getSegments() = Action.async(BodyParsers.parse.json){request =>
    val postBody = request.body
    Logger.info(s"Requested query : $postBody")
    val headers = druidAuthHeaderProvider.getAuthHeaders
    val segFuture = ws.url(s"$druidBroker/druid/v2/?pretty").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).post(postBody)
    val futureResponse = segFuture.map(data =>{
      val metrics = data.body
      metrics
    })
    futureResponse.map( i =>{
      Logger.info(s"Response for segments $i")
      Ok(i)
    })
  }

  def metaDataDashboard() = Action.async{ request =>
  val futureResultTry:Try[Future[Seq[(String,String)]]] =Try{
    val headers = druidAuthHeaderProvider.getAuthHeaders
    val dataSourcesInfoRespFuture = ws.url(druidCoordinator + "/druid/coordinator/v1/datasources").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();
    val futureResult = dataSourcesInfoRespFuture.flatMap(dataSourcesInfoResult =>
    {
      val dataSources: Seq[String] = dataSourcesInfoResult.json.as[JsArray].value.map(orgValue => orgValue.as[String]);
      Logger.info(s"Data sources : $dataSources")
      val futureResponses =  dataSources.map(datasource =>{
        val query = prepareMetaDataQuery(datasource)
        Logger.info(s"query :$query")
        val headers = druidAuthHeaderProvider.getAuthHeaders
        val responseFuture = ws.url(s"$druidBroker/druid/v2/?pretty").withFollowRedirects(true).withHeaders("Content-Type"->"application/json", headers.head._1 -> headers.head._2).post(query)
        val tuple =  responseFuture.map(response =>{
          val ingestedTime:String = if(response.json.as[JsArray].value.size!=0){
            val array =(response.json.as[JsArray].value(0).as[JsObject] \ "result" \"maxIngestedEventTime").getOrElse(JsString("undefined"))
            Logger.info(s"ingested time valye :${array.as[String]}")
            array.as[String]
          }else{
            "undefined"
          }
          (datasource,ingestedTime)
        }
        )
        tuple
      })
      val seq =Future.sequence(futureResponses)
      seq
    }
    )
    futureResult
  }

     futureResultTry match {
      case Success(futureResult) =>futureResult.map(i=>Ok(views.html.metaData(i)))
      case Failure(e) =>{
        Logger.info(s"Unable to generate metadata dashboard $e")
        val future = scala.concurrent.Future(views.html.unauthorized("Unable to generate metadata Dashboard"))
        future.map(i => Ok(i))
      }
    }


  }

  def killSegments() = Action.async{ request =>
    val futureResultTry:Try[Future[Seq[String]]] = Try {
      val headers = druidAuthHeaderProvider.getAuthHeaders
      val dataSourcesInfoRespFuture = ws.url(druidCoordinator + "/druid/coordinator/v1/datasources").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();
      val futureResult = dataSourcesInfoRespFuture.map(
        dataSourcesInfoResult => {
          val dataSources: Seq[String] = dataSourcesInfoResult.json.as[JsArray].value.map(orgValue => orgValue.as[String]);
          Logger.info(s"Data sources : $dataSources")
          dataSources
        }
      )
      futureResult
    }

    futureResultTry match {
      case Success(futureResult) => futureResult.map(
        sources =>
          Ok(views.html.killSegments(sources)))
      case Failure(e) =>{
        Logger.info(s"Unable to get data sources $e")
        val future = scala.concurrent.Future(views.html.unauthorized("Unable to get data sources"))
        future.map(i => Ok(i))
      }
    }
  }

  def submitKillSegments() = Action.async(BodyParsers.parse.json) { request =>
    val postBody = request.body
    Logger.info(s"Kill Task Request : $postBody")
    val headers = druidAuthHeaderProvider.getAuthHeaders
    val segFuture = ws.url(s"$druidCoordinator/druid/indexer/v1/task").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).post(postBody)
    val futureResponse = segFuture.map(data =>{
      val response = data.body
      response
    })
    futureResponse.map( i =>{
      Logger.info(s"Response for killing task $i")
      Ok(i)
    })
  }

  def getWorkers() = Action.async{ request =>
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)
    Logger.debug(s"ws call started: ${druidCoordinator}/druid/indexer/v1/worker")

    val headers = druidAuthHeaderProvider.getAuthHeaders
    val workersConfigResponseFuture = ws.url(s"${druidCoordinator}/druid/indexer/v1/worker").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();

    val futureWorkerConfigResult = workersConfigResponseFuture.flatMap(response => {
      val jsWorkerConfig = response.json.as[JsObject]
      Logger.debug(s"${jsWorkerConfig}")
      Future.apply(jsWorkerConfig.toString())
    })

    Logger.debug(s"ws call started: ${druidCoordinator}/druid/indexer/v1/workers/")

    val workerResponseFuture = ws.url(s"${druidCoordinator}/druid/indexer/v1/workers/").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();
    val futureResult = workerResponseFuture.flatMap(response => {
      val jsArray = response.json.as[List[JsObject]]
      Logger.debug(s"${jsArray.mkString(" ")}")
      val futureSeq=
        for (jsVal <- jsArray)
          yield {
            val host = (jsVal \ "worker" \ "host").get.as[String]
            Logger.debug(s" worker is ${host}")
            val statusFuture = ws.url(s"http://${host}/druid/worker/v1/enabled").withHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).get();
             val entrySet = statusFuture.map(statusBody => {
              val status:Boolean = (statusBody.json.as[JsObject] \ host).get.as[Boolean]
              val hostAndStatusMap = Map("host" -> host, "status" -> status)
              hostAndStatusMap
            })
            entrySet
          }
      Future.sequence(futureSeq)
    })

   val finalConfig = for{
     response <- futureResult
     config <-   futureWorkerConfigResult
   }yield{
      (response,config)
    }
    finalConfig.map(i => Ok(views.html.taskManager(i._1,i._2)))
   // (futureResult.map(i => Ok(views.html.taskManager(i)) ),futureWorkerConfigResult.map(i => Ok(views.html.taskManager(i))))
  }


  def enableDisableMiddleManager( host:String,  action :String) = Action.async{ request =>

        Logger.info(s"Requested request for middle manager  : $host and action : ${action}")
        val headers = druidAuthHeaderProvider.getAuthHeaders
        val actionFuture = ws.url(s"http://${host}/druid/worker/v1/${action}").withHttpHeaders(headers.head._1 -> headers.head._2).withFollowRedirects(true).post(EmptyBody)
        val futureResponse = actionFuture.map(data =>{
          val metrics = data.body
          metrics
        })
        futureResponse.map( i =>{
          Logger.debug(s"Response => host : ${host} , Action : ${action}  ...  $i")
          Ok(i)
        })
  }

  def prepareMetaDataQuery(dataSource:String):String ={
    val query =s"""{"queryType" : "dataSourceMetadata\","dataSource": "$dataSource","context":{"timeout": 29000,"priority": -1}}"""
    query
  }

  case class OracleValue(lastUpdated: String, size: String)

  def getLookUps() = Action {
    request =>
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)
    val oracleCache = mutable.Map[String, OracleValue]()
      val headers = druidAuthHeaderProvider.getAuthHeaders
    val hostsFuture = ws.url(s"$druidCoordinator/druid/coordinator/v1/loadqueue?simple").withHeaders(headers.head._1 -> headers.head._2)
      .get()
    val mapFurture = hostsFuture.map {
      response => {
        val hosts = response.json.as[JsObject].keys
        Logger.debug(s"successfully got hosts: $hosts")
        val hostList = hosts.map {
          host =>
          val tupleFuture = ws.url(s"$druidHistoricalsHttpScheme://$host/druid/v1/namespaces?").withHeaders(headers.head._1 -> headers.head._2).get().map {
            hostResponse =>
              hostResponse.json.as[JsObject].fields.toList.map {
                field =>
                  val lookupName = field._1
                  val lookupSize = getLookupSize(host, field._2, lookupName)
                  val lastTime = getLastTime(host, lookupName)
                  getSingleLookup(oracleCache, lookupName, lookupSize, lastTime, host)
              }
          }
          val tupleAwait = Try {
            Await.result(tupleFuture, 120 second)
          }
          tupleAwait match {
            case Success(tupleList) => {
              Logger.debug(s"successfully got tuplelist for $host")
              (host -> tupleList)
            }
            case Failure(e) =>
              Logger.error(s"unable to get tupleList for host - $host: ${e.printStackTrace}")
              throw new UnsupportedOperationException("exception occurred while getting tupleList")
          }
        }
        hostList.toMap
      }
    }
    val mapAwait = Try {
      Await.result(mapFurture, 120 second)
    }
    mapAwait match {
      case Success(map) => {
        Logger.debug("successfully get all mappings for lookups")
        Ok(views.html.lookup(map))
      }
      case Failure(e) =>
        Logger.error(s"unable to get mapping for lookups: ${e.printStackTrace}")
        BadRequest(Serialization.write(Map("Error" -> e.getMessage)))
    }
  }

  private def getLastTime(host: String, lookupName: String) = {

    val headers = druidAuthHeaderProvider.getAuthHeaders
    val tupleFuture = ws.url(s"$druidHistoricalsHttpScheme://$host/druid/coordinator/v1/lookups/config/historicalLookupTier/$lookupName").withHeaders(headers.head._1 -> headers.head._2).get().map {
      hostResponse =>
        val lastTimeGet = (hostResponse.json \ "lookupExtractorFactory" \ "extractionNamespace" \ "type").as[String] match {
          case "mahajdbc" => ws.url(s"$druidHistoricalsHttpScheme://$host/druid/v1/namespaces/$lookupName/lastUpdatedTime?namespaceclass=com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace").withHeaders(headers.head._1 -> headers.head._2).get()
          case "mahainmemorydb" => ws.url(s"$druidHistoricalsHttpScheme://$host/druid/v1/namespaces/$lookupName/lastUpdatedTime?namespaceclass=com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.InMemoryDBExtractionNamespace").withHeaders(headers.head._1 -> headers.head._2).get()
        }
        val lastTimeFuture = lastTimeGet.map {
          nameResponse => new DateTime(nameResponse.body.toLong)
        }
        val lastTimeAwait = Try {
          Await.result(lastTimeFuture, 120 second)
        }
        lastTimeAwait match {
          case Success(lastTime) => {
            lastTime
          }
          case Failure(e) =>
            Logger.error(s"unable to get lookup last updated time : ${e.printStackTrace}")
            new DateTime(0L)
        }
    }
    val tupleAwait = Try {
      Await.result(tupleFuture, 120 second)
    }
    tupleAwait match {
      case Success(tupleList) => {
        Logger.debug(s"successfully got lookup for $host")
        (host -> tupleList)
      }
      case Failure(e) =>
        Logger.error(s"unable to get lookup for host - $host: ${e.printStackTrace}")
        throw new UnsupportedOperationException("exception occurred while getting tupleList")
    }
  }

  private def getLookupSize(host: String, value: JsValue, lookupName: String) = {
    val headers = druidAuthHeaderProvider.getAuthHeaders
    val tupleFuture = ws.url(s"$druidHistoricalsHttpScheme://$host/druid/coordinator/v1/lookups/config/historicalLookupTier/$lookupName").withHeaders(headers.head._1 -> headers.head._2).get().map {
      hostResponse =>
        val lookupFuture = (hostResponse.json \ "lookupExtractorFactory" \ "extractionNamespace" \ "type").as[String] match {
          case "mahainmemorydb" =>
            ws.url(s"$druidHistoricalsHttpScheme://$host/druid/v1/namespaces/$lookupName?namespaceclass=com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.InMemoryDBExtractionNamespace").withHeaders(headers.head._1 -> headers.head._2).get()
            .map(_.body.toString)
            val lookupSizeAwait = Try {
              Await.result(lookupFuture, 120 second)
            }
            lookupSizeAwait match {
              case Success(size) => {
                size
              }
              case Failure(e) =>
                Logger.error(s"unable to get size for $host: ${e.printStackTrace}")
                N_A
            }
          case _ => Json.stringify(value)
        }
    }
    val tupleAwait = Try {
      Await.result(tupleFuture, 120 second)
    }
    tupleAwait match {
      case Success(tupleList) => {
        Logger.debug(s"successfully got lookup for $host")
        (host -> tupleList)
      }
      case Failure(e) =>
        Logger.error(s"unable to get lookup for host - $host: ${e.printStackTrace}")
        throw new UnsupportedOperationException("exception occurred while getting tupleList")
    }
  }

  private def getSingleLookup(oracleCache: mutable.Map[String, OracleValue], lookupName: String, lookupSize: String, lastTime: DateTime, host: String) = {
    var (oracleLastUpdated: String, oracleSize: String) = (N_A, N_A)
    val headers = druidAuthHeaderProvider.getAuthHeaders
    val lookupTierURL = s"$druidCoordinator/druid/coordinator/v1/lookups/config/historicalLookupTier/$lookupName"

    def getOracleLookup(table: Option[String]) = {

      if (jdbcConnectionToGetLookupCount.isDefined &&
        jdbcConnectionToGetLookupTimeStamp.isDefined &&
        table.isDefined &&
        lookupCountSql.isDefined &&
        lookupTimestampSql.isDefined) {

        if (!oracleCache.contains(table.get)) {
          val tableSplit = table.get.split("\\.")
          val tableName = if (tableSplit.length > 1) tableSplit(1) else tableSplit(0)
          val countQuery = lookupCountSql.get.replace("$tableName", tableName)
          Logger.debug(s" Running query for getting oracle size: ${countQuery}")
          val countTry = jdbcConnectionToGetLookupCount.get.queryForObject(countQuery) { rs =>
            while (rs.next()) {
              oracleSize = Try(rs.getString(1)) match {
                case Success(s) => s
                case Failure(e) =>
                  Logger.error(s"unable to get size : $e")
                  N_A
              }
            }
          }
          val getMaxLastUpdatedQuery = lookupTimestampSql.get.replace("$tableName", tableName)
          Logger.debug(s" Running query for getting last updated time: $getMaxLastUpdatedQuery")
          val getMaxLastUpdatedTry = jdbcConnectionToGetLookupTimeStamp.get.queryForObject(getMaxLastUpdatedQuery) { rs =>
            while (rs.next()) {
              oracleLastUpdated = Try(rs.getTimestamp(1)) match {
                case Success(t) => dateFormat.format(t)
                case Failure(e) =>
                  Logger.error(s"unable to get lastUpdated : $e")
                  N_A
              }
            }
          }
          (countTry, getMaxLastUpdatedTry) match {
            case (Success(_), Success(_)) => {
              oracleCache += (table.get -> OracleValue(oracleLastUpdated, oracleSize))
              Logger.debug(s"Got last updated time and size: $oracleLastUpdated, $oracleSize")
            }
            case (Failure(e), _) => throw e
            case (_, Failure(e)) => throw e
          }
        } else {
          oracleLastUpdated = oracleCache(table.get).lastUpdated
          oracleSize = oracleCache(table.get).size
        }
      }
    }

    val lookupTierFuture = ws.url(lookupTierURL).withHeaders(headers.head._1 -> headers.head._2).get().map {
      tierReponse =>
        val table = if (lookupName == "ad_lookup" || lookupName == "adgroup_lookup"){
          Option(lookupName.split("_lookup")(0))
        } else {
          (tierReponse.json \ "lookupExtractorFactory" \ "extractionNamespace" \ "table") match {
            case JsDefined(tableName) => {
              Logger.debug(s"Table name: $tableName")
              Option(tableName.as[String])
            }
            case undefined: JsUndefined => {
              Logger.debug(s"Cannot find table name on $lookupTierURL ")
              None
            }
          }
        }
        val tsColumn = (tierReponse.json \ "lookupExtractorFactory" \ "extractionNamespace" \ "tsColumn") match {
          case JsDefined(tsColumnName) => {
            Logger.debug(s"tsColumn name: $tsColumnName")
            Option(tsColumnName.as[String])
          }
          case undefined: JsUndefined => {
            Logger.debug(s"Cannot find tsColumn on $lookupTierURL ")
            None
          }
        }
        getOracleLookup(table)
    }
    val lookupTierAwait = Try {
      Await.result(lookupTierFuture, 120 second)
    }
    lookupTierAwait match {
      case Success(tupleList) => {
        Logger.debug(s"successfully got tuple ($lookupName, $lookupSize, $lastTime, $oracleLastUpdated, $oracleSize)")
        (lookupName, lookupSize, oracleSize, lastTime.toString, oracleLastUpdated)
      }
      case Failure(e) =>
        Logger.error(s"unable to get lookupTier for host - $host: ${e.printStackTrace}")
        (N_A, N_A, N_A, N_A, N_A)
    }
  }

  def jsRouter = Action{ implicit  request =>
    Ok(
      JavaScriptReverseRouter("jsRoutes")(
        controllers.routes.javascript.DashBoard.getMetrics,
        controllers.routes.javascript.DashBoard.getDimensions,
        controllers.routes.javascript.DashBoard.getSegments,
        controllers.routes.javascript.DashBoard.enableDisableMiddleManager,
        controllers.routes.javascript.DashBoard.submitKillSegments
      )
    ).as("text/javascript")
  }

  def statusCheck() = Action{request =>
  Ok("""{"status":""}""")
  }

  def authcallback() = Action{ request =>

    authValidator.handleAuthCallback(request)

  }

}
