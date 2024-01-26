// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.api.jersey.example.student

import com.yahoo.maha.api.jersey.example.ExampleSchema
import com.yahoo.maha.api.jersey.example.ExampleSchema.StudentSchema
import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.registry.{DimensionRegistrationFactory, FactRegistrationFactory, RegistryBuilder}
import com.yahoo.maha.core.request.{AsyncRequest, RequestType, SyncRequest}

class SampleFactSchemaRegistrationFactory extends FactRegistrationFactory {
  protected[this] def getMaxDaysWindow: Map[(RequestType, Grain), Int] = {
    val result = 20
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result,
      (SyncRequest, HourlyGrain) -> result, (AsyncRequest, HourlyGrain) -> result
    )
  }

  protected[this] def getMaxDaysLookBack: Map[(RequestType, Grain), Int] = {
    val result = 30
    Map(
      (SyncRequest, DailyGrain) -> result, (AsyncRequest, DailyGrain) -> result
    )
  }


  override def register(registry: RegistryBuilder): Unit = {
    ExampleSchema.register()

    def pubfact: PublicFact = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        import com.yahoo.maha.core.OracleExpression._
        Fact.newFact(
          "student_grade_sheet", DailyGrain, OracleEngine, Set(StudentSchema),
          Set(
            DimCol("class_id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
            , DimCol("section_id", IntType(3))
            , DimCol("myyear", IntType(3, (Map(1 -> "Freshman", 2 -> "Sophomore", 3 -> "Junior", 4 -> "Senior"), "Other")))
            , DimCol("mycomment", StrType(), annotations = Set(EscapingRequired))
            , DimCol("mydate", DateType())
            ),
          Set(
             FactCol("total_marks", IntType())
            ,FactCol("obtained_marks", IntType())
            ,OracleDerFactCol("Performance Factor", DecType(10,2), "{obtained_marks}" /- "{total_marks}")
          )
        )
      }
        .toPublicFact("student_performance",
          Set(
            PubCol("class_id", "Class ID", Equality),
            PubCol("student_id", "Student ID", Equality),
            PubCol("section_id", "Section ID", Equality),
            PubCol("mydate", "Day", Equality),
            PubCol("myyear", "Year", Equality),
            PubCol("mycomment", "Remarks", InEqualityLike)
          ),
          Set(
            PublicFactCol("total_marks", "Total Marks", InBetweenEquality),
            PublicFactCol("obtained_marks", "Marks Obtained", InBetweenEquality),
            PublicFactCol("Performance Factor", "Performance Factor", InBetweenEquality)
          ),
          Set.empty,
          getMaxDaysWindow, getMaxDaysLookBack
        )
    }
    registry.register(pubfact)
  }
}

class SampleDimensionSchemaRegistrationFactory extends DimensionRegistrationFactory {
  override def register(registry: RegistryBuilder): Unit = {
    val student_dim: PublicDimension = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension("student", OracleEngine, LevelOne, Set(StudentSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("department_id", IntType())
            , DimCol("admitted_year", IntType())
            , DimCol("status", StrType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , schemaColMap = Map(StudentSchema -> "id")
          , annotations = Set(OracleHashPartitioning)
        ).toPublicDimension("student","student",
          Set(
            PubCol("id", "Student ID", Equality)
            , PubCol("name", "Student Name", Equality)
            , PubCol("admitted_year", "Admitted Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Student Status", InEquality)
          ), highCardinalityFilters = Set(NotInFilter("Student Status", List("DELETED")))
        )
      }
    }
    registry.register(student_dim)
  }
}
