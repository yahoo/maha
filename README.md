

[![Pipeline Status](https://cd.screwdriver.cd/pipelines/7538/badge)](https://cd.screwdriver.cd/pipelines/7538)

[Google Group: Maha-Users](https://groups.google.com/forum/#!forum/maha-users) 

[Maha Release Notes](RELEASE_NOTES.md)

[Maha Release Pipeline](https://cd.screwdriver.cd/pipelines/7538/events/636686)

# Maha
A centralised library for building reporting APIs on top of multiple data stores to exploit them for what they do best.

>  We run millions of queries on multiple data sources for analytics every day. They run on hive, oracle, druid etc.  We needed a way to utilize the data stores in our architecture to exploit them for what they do best. This meant we needed to easily tune and identify sets of use cases where each data store fits the best. Our goal became to build a centralized system which was able to make these decisions on the fly at query time and also take care of the end to end query execution. The system needed to take in all the heuristics available, applying any constraints already defined in the system and select the best data store to run the query.  It then would need to generate the underlying queries and pass on all available information to the query execution layer in order to facilitate further optimization at that layer. 

# Key Features!
  - Configuration driven API making it easy to address multiple reporting use cases
  - Define cubes across multiple data sources (oracle, druid, hive)
  - Dynamic selection of query data source based on query cost, grain, weight
  - Dynamic query generation with support for filter and ordering on every column, pagination, star schema joins, query type etc
  - Pluggable partitioning scheme, and time providers
  - Access control based on schema/labeling in cube definitions
  - Define constraints on max lookback, max days window in cube definitions
  - Provide easily aliasing of physical column names across tables / engines
  - Query execution for Oracle, Druid out-of-the-box
  - Support for dim driven queries for entity management alongside metrics
  - API side joins between Oracle/Druid for fact driven or dim driven queries
  - Fault tolerant apis: fall back option to other datasource if configured
  - Supports customising and tweaking data source specific executor's config 
  - MahaRequestLog : Kafka logging of API Statistics
  - Support for high cardinality dimension druid lookups
  - Standard JDBC driver to query maha (With Maha Dialect) powered by Avatica and Calcite. 

### Maha Architecture

![Maha Architecture](https://user-images.githubusercontent.com/4935454/67800990-8c987580-fa45-11e9-8ba4-d78c1f31be8f.jpeg)

### Modules in maha
  - maha-core : responsible for creating Reporting Request, Request Model (Query Metadata) , Query Generation, Query Pipeline (Engine selection)
  - maha-druid-executor : Druid Query Executor
  - maha-oracle-executor : Oracle Query Executor 
  - maha-presto-executor : Presto Query Executor 
  - maha-postgres-executor : Postgres Query Executor 
  - maha-druid-lookups: Druid Lookup extension for high cardinality dimension druid lookups
  - maha-par-request: Library for Parallel Execution, Blocking and Non Blocking Callables using Java utils
  - maha-service : One json config for creating different registries using the fact and dim definitions. 
  - maha-api-jersey : Easy war file helper library for exposing the api using maha-service module
  - maha-api-example : End to end example implementation of maha apis
  - maha-par-request-2: Library for Parallel Execution, Blocking and Non Blocking Callables using Scala utils
  - maha-request-log: Kafka Events writer about the api usage request stats for given registry in maha

### Getting Started 

#### Installing Maha API Library 

- We have published the packages in Maven Central distribution, you can have look at the latest version at https://mvnrepository.com/artifact/com.yahoo.maha/maha-api-jersey

```
<dependency>
  <groupId>com.yahoo.maha</groupId>
  <artifactId>maha-api-jersey</artifactId>
  <version>6.53</version>
</dependency>
```

- maha-api-jersey includes all the dependencies of other modules 

#### Example Implementation of Maha Apis 

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
          "wikipedia", DailyGrain, DruidEngine, Set(WikiSchema),
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
  * Step 1: Checkout yahoo/maha repository 
  * Step 2: Run ``` mvn clean install ``` in maha
  * Step 3: Go to ``` cd api-example ``` module and run ```mvn jetty:run```, you can run it with -X for debug logs. 
  * Step 4: Step 2 will launch jetty server in local and will deploy maha-api example war and you are good to play with it!

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
		"fields": [{
				"fieldName": "Student ID",
				"fieldType": "DIM"
			},
			{
				"fieldName": "Class ID",
				"fieldType": "DIM"
			},
			{
				"fieldName": "Section ID",
				"fieldType": "DIM"
			},
			{
				"fieldName": "Total Marks",
				"fieldType": "FACT"
			}
		],
		"maxRows": 200
	},
	"rows": [
		[213, 200, 100, 125],
		[213, 198, 100, 120]
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
         "from": "2015-09-11",
         "to": "2015-09-13"
      }
   ]
}     
```


  Curl : 
```
      curl -H "Content-Type: application/json" -H "Accept: application/json" -X POST -d @wikiticker.json http://localhost:8080/mahademo/registry/wiki/schemas/wiki/query?debug=true  
```
  
  Output : 
```
{"header":{"cube":"wikiticker_stats","fields":[{"fieldName":"Wiki Channel","fieldType":"DIM"},{"fieldName":"Total Count","fieldType":"FACT"},{"fieldName":"Added Count","fieldType":"FACT"},{"fieldName":"Deleted Count","fieldType":"FACT"}],"maxRows":200},"rows":[["#ar.wikipedia",423,153605,2727],["#be.wikipedia",33,46815,1235],["#bg.wikipedia",75,41674,528],["#ca.wikipedia",478,112482,1651],["#ce.wikipedia",60,83925,135],["#cs.wikipedia",222,132768,1443],["#da.wikipedia",96,44879,1097],["#de.wikipedia",2523,522625,35407],["#el.wikipedia",251,31400,9530],["#en.wikipedia",11549,3045299,176483],["#eo.wikipedia",22,13539,2],["#es.wikipedia",1256,634670,15983],["#et.wikipedia",52,2758,483],["#eu.wikipedia",13,6690,43],["#fa.wikipedia",219,74733,2798],["#fi.wikipedia",244,54810,2590],["#fr.wikipedia",2099,642555,22487],["#gl.wikipedia",65,12483,526],["#he.wikipedia",246,51302,3533],["#hi.wikipedia",19,34977,60],["#hr.wikipedia",22,25956,204],["#hu.wikipedia",289,166101,2077],["#hy.wikipedia",153,39099,4230],["#id.wikipedia",110,119317,2245],["#it.wikipedia",1383,711011,12579],["#ja.wikipedia",749,317242,21380],["#kk.wikipedia",9,1316,31],["#ko.wikipedia",533,66075,6281],["#la.wikipedia",33,4478,1542],["#lt.wikipedia",20,14866,242],["#min.wikipedia",1,2,0],["#ms.wikipedia",11,21686,556],["#nl.wikipedia",445,145634,6557],["#nn.wikipedia",26,33745,0],["#no.wikipedia",169,51385,1146],["#pl.wikipedia",565,138931,8459],["#pt.wikipedia",472,229144,8444],["#ro.wikipedia",76,28892,1224],["#ru.wikipedia",1386,640698,19612],["#sh.wikipedia",14,6935,2],["#simple.wikipedia",39,43018,546],["#sk.wikipedia",33,12188,72],["#sl.wikipedia",21,3624,266],["#sr.wikipedia",168,72992,2349],["#sv.wikipedia",244,42145,3116],["#tr.wikipedia",208,67193,1126],["#uk.wikipedia",263,137420,1959],["#uz.wikipedia",983,13486,8],["#vi.wikipedia",9747,295972,1388],["#war.wikipedia",1,0,0],["#zh.wikipedia",1126,191033,7916]]}
```

  * POST Maha Reporting Request for example student schema with TimeShift Curator
  MahaRequest will look like following, you need to pass cube name, list of fields you want to fetch, filters, sorting columns in the base request and timeshift curator configs (daysOffset is an day offset for requesting previous period's to and from dates)

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
         "from": "2019-10-20",
         "to": "2019-10-29"
      },
      {
         "field": "Student ID",
         "operator": "=",
         "value": "213"
      }
   ],
  "curators": {
    "timeshift": {
      "config" : {
        "daysOffset": 0 
      }
    }
  }
}    
  ```
   please note that we have loaded the test data for demo in current day and day before. For timeshift curator demo, we have loaded data for 11 days back of current date. Please make sure that you update the requested to and from dates according to current dates. 

    
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
                "fieldType": "DIM"
            },
            {
                "fieldName": "Total Marks",
                "fieldType": "FACT"
            },
            {
                "fieldName": "Total Marks Prev",
                "fieldType": "FACT"
            },
            {
                "fieldName": "Total Marks Pct Change",
                "fieldType": "FACT"
            }
        ],
        "maxRows": 200,
        "debug": {}
    },
    "rows": [
        [
            213,
            198,
            100,
            120,
            98,
            22.45
        ],
        [
            213,
            200,
            100,
            125,
            110,
            13.64
        ]
    ]
}
```

  * POST Maha Reporting Request for example wiki schema with Total metrics curator
  
  Request : 
``` 
{
   "cube": "wikiticker_stats",
   "selectFields": [
      {
         "field": "Wiki Channel"
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
         "from": "2015-09-11",
         "to": "2015-09-13"
      }
   ],
   "curators": {
      "totalmetrics": {
         "config": {}
      }
   }
}
```
 In druid quick-start tutorial, wikipedia data is loaded for 
2015-09-12, thus no change in the requested dates here.  

  Curl : 
```
      curl -H "Content-Type: application/json" -H "Accept: application/json" -X POST -d @wikiticker.json http://localhost:8080/mahademo/registry/wiki/schemas/wiki/query?debug=true  
```
  
  Output : 
```
{
    "header": {
        "cube": "wikiticker_stats",
        "fields": [
            {
                "fieldName": "Wiki Channel",
                "fieldType": "DIM"
            },
            {
                "fieldName": "Total Count",
                "fieldType": "FACT"
            },
            {
                "fieldName": "Added Count",
                "fieldType": "FACT"
            },
            {
                "fieldName": "Deleted Count",
                "fieldType": "FACT"
            }
        ],
        "maxRows": 200,
        "debug": {}
    },
    "rows": [
        [
            "#ar.wikipedia",
            0,
            153605,
            2727
        ],
        [
            "#be.wikipedia",
            0,
            46815,
            1235
        ],
        [
            "#bg.wikipedia",
            0,
            41674,
            528
        ],
        [
            "#ca.wikipedia",
            0,
            112482,
            1651
        ],
        ... trimming other rows 
    ],
    "curators": {
        "totalmetrics": {
            "result": {
                "header": {
                    "cube": "wikiticker_stats",
                    "fields": [
                        {
                            "fieldName": "Total Count",
                            "fieldType": "FACT"
                        },
                        {
                            "fieldName": "Added Count",
                            "fieldType": "FACT"
                        },
                        {
                            "fieldName": "Deleted Count",
                            "fieldType": "FACT"
                        }
                    ],
                    "maxRows": -1,
                    "debug": {}
                },
                "rows": [
                    [
                        0,
                        9385573,
                        394298
                    ]
                ]
            }
        }
    }
}
```
#### Maha JDBC Query Layer (Example DB Ever configuration)
Maha is currently queryable by json REST APIs. 
We have exposed the standard JDBC interface to query maha so that users can use any other tool like SQL Labs/ DbEver/Any other Database Explorer that you like to query maha.  
Users will be agnostic about which engine maha sql query will be fetching the data from and able to get the data back seamlessly without any code change from client side.  
This feature is powered by Apache Calcite for sql parsing and Avatica JDBC for exposing the JDBC server.
You can follow the below steps to configure your local explorer and query maha jdbc. 
1. Please follow the above steps and keep your api-example server running. It exposes this endpoint `http://localhost:8080/mahademo/registry/student/schemas/student/sql-avatica` to be used by avatica jdbc connection.
2. Download the community version of DBEver from https://dbeaver.io/ 
3. Go to Driver Manager and Coonfigure Avatica Jar with the following settings as shown in the screenshot. 
4. Mostly Avatica driver is backward compatible, we used the https://mvnrepository.com/artifact/org.apache.calcite.avatica/avatica-core/1.17.0 for demo.
5. 



#### Presentation of 'Maha' at Bay Area Hadoop Meetup held on 29th Oct 2019:

[!['Maha' at Bay Area Hadoop Meetup held on 29th Oct 2019](https://img.youtube.com/vi/5YpAWE-qxac/0.jpg)](https://www.youtube.com/watch?v=5YpAWE-qxac)

### Contributions 
  - Hiral Patel
  - Pavan Arakere Badarinath
  - Pranav Anil Bhole
  - Shravana Krishnamurthy
  - Jian Shen
  - Shengyao Qian
  - Ryan Wagner
  - Raghu Kumar
  - Hao Wang
  - Surabhi Pandit
  - Parveen Kumar
  - Santhosh Joshi
  - Vivek Chauhan
  - Ravi Chotrani
  - Huiliang Zhang
  - Abhishek Sarangan
  - Jay Yang
  - Ritvik Jaiswal 
  - Ashwin Tumma
  - Ann Therese Babu
  - Kevin Chen
  - Priyanka Dadlani
 
### Acknowledgements
  - Oracle Query Optimizations
    - Remesh Balakrishnan
    - Vikas Khanna
  - Druid Query Optimizations
    - Eric Tschetter
    - Himanshu Gupta
    - Gian Merlino
    - Fangjin Yang
  - Hive Query Optimizations
    - Seshasai Kuchimanchi

