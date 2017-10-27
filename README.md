# Maha API
Yahoo's centralised API Engine library on top of multiple data stores to exploit them for what they do best.

>  Yahoo runs millions of queries on the multiple data sources for analytics everyday, some of them includes hive, oracle, druid etc. We needed a way to utilize the data stores in our architecture to exploit them for what they do best. This meant we needed to easily tune and identify sets of use cases where each data store fits the best. Our goal became to build a centralized system which was able to make these decisions on the fly at query time and also take care of the end to end query execution. The system needed to take in all the heuristics available, applying any constraints already defined in the system and select the best data store to run the query.  It then would need to generate the underlying queries and pass on all available information to the query execution layer in order to facilitate further optimization at that layer. 

# Key Features!
  - One config api service on multiple data sources (oracle, druid, hive)
  - Hosts high performance real time analytics api
  - Dynamic selection of query data source based on query cost, grain, weight,
  - Dynamic query generation with support of filter and ordering on every column, pagination, star schema joins, query type etc
  - One time fact/dim definition api cube config
  - Can host multiple schemas to maintain isolation of different endpoints
  - Fault tolerant apis: fall back option to other datasource if configured
  - Supports customising and tweaking data source specific executor's config 
  - MahaRequestLog : Kafka logging of API Statistics


### Modules in maha
  - maha-core : responsible for creating Reporting Request, Request Model (Query Metadata) , Query Generation, Query Pipeline (Engine selection)
  - maha-druid : Druid Query Executor
  - maha-oracle : Oracle Query Executor 
  - maha-druid-lookups: Druid Lookup extension for lookup join
  - maha-par-request: Library for Parallel Execution, Blocking and Non Blocking Callables
  - maha-service : One json config for creating different registries using the fact and dim definitions. 
  - maha-api-jersey : easy war file helper for exposing the api using maha-service module
 
### Getting Started 

- Maha-Service Examples  
    - Druid Wiki Ticker Example   
        - as pre-requisite you need to follow [Druid.io Getting Started Guide](http://druid.io/docs/latest/tutorials/quickstart.html) and need local running druid instance with wikiticker indexed)
    - H2 Database Student Course Example 
        - you can run in the local as unit test

#### Druid Wiki Ticker Example  
  For this example, you need druid instance running in local and wikitikcer dataset indexed into druid, please take look at http://druid.io/docs/latest/tutorials/quickstart.html

##### Creating Fact Definition for Druid Wikiticker
#
```
          ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Fact.newFact(
          "wikiticker_stats_datasource", DailyGrain, DruidEngine, Set(WikiSchema),
          Set(
            DimCol("channel", StrType())
            , DimCol("cityName", StrType())
            , DimCol("comment", StrType(), annotations = Set(EscapingRequired))
            , DimCol("countryIsoCode", StrType(10))
            , DimCol("countryName", StrType(100))
            , DimCol("isAnonymous", StrType(5))
            , DimCol("isMinor", StrType(5))
            , DimCol("isNew", StrType(5))
            , DimCol("isRobot", StrType(5))
            , DimCol("isUnpatrolled", StrType(5))
            , DimCol("metroCode", StrType(100))
            , DimCol("namespace", StrType(100, (Map("Main" -> "Main Namespace", "User" -> "User Namespace", "Category" -> "Category Namespace", "User Talk"-> "User Talk Namespace"), "Unknown Namespace")))
            , DimCol("page", StrType(100))
            , DimCol("regionIsoCode", StrType(10))
            , DimCol("regionName", StrType(200))
            , DimCol("user", StrType(200))
          ),
          Set(
          FactCol("count", IntType())
          ,FactCol("added", IntType())
          ,FactCol("deleted", IntType())
          ,FactCol("delta", IntType())
          ,FactCol("user_unique", IntType())
          ,DruidDerFactCol("Delta Percentage", DecType(10, 8), "{delta} * 100 / {count} ")
          )
        )
      }
        .toPublicFact("wikiticker_stats",
          Set(
            PubCol("channel", "Wiki Channel", InNotInEquality),
            PubCol("cityName", "City Name", InNotInEqualityLike),
            PubCol("countryIsoCode", "Country ISO Code", InNotInEqualityLike),
            PubCol("countryName", "Country Name", InNotInEqualityLike),
            PubCol("isAnonymous", "Is Anonymous", InNotInEquality),
            PubCol("isMinor", "Is Minor", InNotInEquality),
            PubCol("isNew", "Is New", InNotInEquality),
            PubCol("isRobot", "Is Robot", InNotInEquality),
            PubCol("isUnpatrolled", "Is Unpatrolled", InNotInEquality),
            PubCol("metroCode", "Metro Code", InNotInEquality),
            PubCol("namespace", "Namespace", InNotInEquality),
            PubCol("page", "Page", InNotInEquality),
            PubCol("regionIsoCode", "Region Iso Code", InNotInEquality),
            PubCol("regionName", "Region Name", InNotInEqualityLike),
            PubCol("user", "User", InNotInEquality)
          ),
          Set(
            PublicFactCol("count", "Total Count", InBetweenEquality),
            PublicFactCol("added", "Added Count", InBetweenEquality),
            PublicFactCol("deleted", "Deleted Count", InBetweenEquality),
            PublicFactCol("delta", "Delta Count", InBetweenEquality),
            PublicFactCol("user_unique", "Unique User Count", InBetweenEquality),
            PublicFactCol("Delta Percentage", "Delta Percentage", InBetweenEquality)
          ),
          Set.empty,
          getMaxDaysWindow, getMaxDaysLookBack
        )

```

Fact definition is the static object specification for the [facts](https://en.wikipedia.org/wiki/Measure_%28data_warehouse%29) and [dimension](https://en.wikipedia.org/wiki/Dimension_%28data_warehouse%29) columns present in the table in the data-source, you can say it is object image of the table. DimCol has the base name, data-types, annotation. Annotations are the configurations stating the primary key/foreign key configuration, special character escaping in the query generation, static value mapping ie ```StrType(100, (Map("Main" -> "Main Namespace", "User" -> "User Namespace", "Category" -> "Category Namespace", "User Talk"-> "User Talk Namespace"), "Unknown Namespace")) ```.  Fact definition can have derived columns, maha supports most common arithmetic derived expression.

Public Fact : Public fact contains the base name to public name mapping. Public Names can be directly used in the Request Json. Public fact are identified by the name called cube name ie 'wikiticker_stats'. Maha supports versioning on the cubes, you have multiple versions of the same cube. 

Fact/Dimension Registration Factory: Facts and dimensions are registered under the derived static class object of FactRegistrationFactory or DimensionRegistration Factory. Factory Classes used in the maha-service-json-config.

##### maha-service-config.json 
 Maha Service Config json contains one place config for launching maha-apis which includes the following.
 *  Set of Public Facts registered under Registry Name ie wikiticker_stats cube is registered under the registry name called wiki
 *  Set of Registries
 *  Set of Query of generator and their config
 *  Set of Query Executors and their config
 *  Bucketing configurations containing the cube version based routing of the reporting requests
 *  UTC Time provider Maps , if the date /time is local date then you can have utc time provider to convert it to utc in query generation phase.
 *  Parallel Service Executor Maps for serving the reporting request utilising the thread-pool config. 
 *  Maha Request Logging Config, kafka configuration for logging the maha request debug logs to kafka queue.
 
We have created ```api-jersey/src/test/resources/maha-service-config.json``` configuration to start with, this is maha api configuration for student and wiki registry. 

Debugging maha-service-config json: For the configuration syntax of this json, you can take look at JsonModels/Factories in the service module. Once Maha Service loads this configuration, if there are some failures in loading the configuration then mahaService will return the list of FailedToConstructFactory/ ServiceConfigurationError/ JsonParseError.

##### Exposing the endpoints with api-jersey
Api-jersey uses maha-service-config json and create MahaResource beans. All you need to do is to create the following three beans 'mahaService', 'baseRequest', 'exceptionHandler' etc. 
 
``` 
    <bean id="mahaService" class="com.yahoo.maha.service.example.ExampleMahaService" factory-method="getMahaService"/>
    <bean id="baseRequest" class="com.yahoo.maha.service.example.ExampleRequest" factory-method="getRequest"/>
    <bean id="exceptionHandler" class="com.yahoo.maha.api.jersey.GenericExceptionMapper" scope="singleton" />
    <import resource="classpath:maha-jersey-context.xml" />
```

Once your application context is ready, you are good to launch the war file on the web server. You can take look at the test application context that we have created for running local demo and unit test ```api-jersey/src/test/resources/testapplicationContext.xml```
  
#### Launch the maha api demo in local 
##### prerequisites 
  * druid.io getting started guide in local for wikitiker demo
  * Postman (optional) 

##### Run demo : 
  * Step 1: checkout yahoo/maha repository and go to ``` cd api-example ``` module
  * Step 2: run ```mvn jerry:run```, you can run it with -X for debug logs. 
  * Step 3: Step 2 will launch jetty server in local and will deploy maha-api example war and you are good to play with it!

##### Playing with demo :
  * GET Domain request: Dimension and Facts
  You can fetch wiki registry domain  using  ``` curl http://localhost:8080/mahademo/registry/wiki/domain ``` 
  Domain tells you lit of cubes and their corresponding list of fields that you can request for particular registry. Here wiki is the registry name.  
  
  * GET Flatten Domain request : Flatten dimension and facts fields 
  You can get flatten domain using ``` curl http://localhost:8080/mahademo/registry/wiki/flattenDomain ```
  
  * POST Maha Reporting Request for example student schema
  MahaRequest will look like following, you need to pass cube name, list of fields you want to fetch, filters, sorting columns etc. 

  ```
  {
     "cube": "student_performance",
     "selectFields": [
        {
           "field": "Student ID"
        },
        {
           "field": "Class ID"
        },
        {
           "field": "Section ID"
        },
        {
           "field": "Total Marks"
        }
     ],
     "filterExpressions": [
        {
           "field": "Day",
           "operator": "between",
           "from": "2017-10-20",
           "to": "2017-10-25"
        },
        {
           "field": "Student ID",
           "operator": "=",
           "value": "213"
        }
     ]
  } 
  ```
  you can find ```student.json``` in the api-example module, ```**make sure you change the dates to latest date range in YYYY-MM-dd to avoid max look back window error. ```
    
  Curl command : 
  ``` 
  curl -H "Content-Type: application/json" -H "Accept: application/json" -X POST -d @student.json http://localhost:8080/mahademo/registry/student/schemas/student/query?debug=true 
  ```

  Sync Output : 

  ``` 
{
   "header": {
      "cube": "student_performance",
      "fields": [
         {
            "fieldName": "Student ID",
            "fieldType": "DIM"
         },
         {
            "fieldName": "Class ID",
            "fieldType": "DIM"
         },
         {
            "fieldName": "Section ID",
            "fieldType": "FACT"
         },
         {
            "fieldName": "Total Marks",
            "fieldType": "FACT"
         }
      ],
      "maxRows": 200
   },
   "rows": [
      [
         213,
         200,
         100,
         125
      ]
   ]
}
```
  
  * POST Maha Reporting Request for example wiki schema
  
  Request : 
``` 
  {
        "cube": "wikiticker_stats",
        "selectFields": [
           {
              "field": "Wiki Channel"
           },
           {
              "field": "City Name"
           },
           {
              "field": "Country ISO Code"
           },
           {
              "field": "Total Count"
           },
           {
              "field": "Added Count"
           },
           {
              "field": "Deleted Count"
           }
        ],
        "filterExpressions": [
           {
              "field": "Day",
              "operator": "between",
              "from": "2017-10-20",
              "to": "2017-10-25"
           }
        ]
     }     
```


  Curl : 
```
      curl -H "Content-Type: application/json" -H "Accept: application/json" -X POST -d @wikiticker.json http://localhost:8080/mahademo/registry/wiki/schemas/wiki/query?debug=true  
```
  
