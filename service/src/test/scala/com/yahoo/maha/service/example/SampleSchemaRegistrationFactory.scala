// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.service.example

import com.yahoo.maha.core.FilterOperation._
import com.yahoo.maha.core._
import com.yahoo.maha.core.dimension._
import com.yahoo.maha.core.fact._
import com.yahoo.maha.core.registry.{DimensionRegistrationFactory, FactRegistrationFactory, RegistryBuilder}
import com.yahoo.maha.core.request.{AsyncRequest, RequestType, SyncRequest}
import com.yahoo.maha.service.example.ExampleSchema.StudentSchema
import javax.annotation.Nullable

/**
 * Created by pranavbhole on 09/06/17.
 */
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

    def pubfactOracle: PublicFact = {
     val builder = ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        import com.yahoo.maha.core.OracleExpression._
        Fact.newFact(
          "student_grade_sheet", DailyGrain, OracleEngine, Set(StudentSchema),
          Set(
            DimCol("class_id", IntType(), annotations = Set(ForeignKey("class")))
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
            , DimCol("section_id", IntType(3))
            , DimCol("year", IntType(3, (Map(1 -> "Freshman", 2 -> "Sophomore", 3 -> "Junior", 4 -> "Senior"), "Other")))
            , DimCol("comment", StrType(), annotations = Set(EscapingRequired))
            , DimCol("date", DateType())
            , DimCol("month", DateType())
            , DimCol("top_student_id", IntType())
            ),
          Set(
             FactCol("total_marks", IntType())
            ,FactCol("obtained_marks", IntType())
            ,OracleDerFactCol("Performance Factor", DecType(10,2), "{obtained_marks}" /- "{total_marks}")
          )
        )
      }
      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
        builder.withAlternativeEngine("student_hive","student_grade_sheet", HiveEngine,
          discarding = Set("total_marks", "Performance Factor"))
      }


      builder.toPublicFact("student_performance",
          Set(
            PubCol("class_id", "Class ID", InEquality),
            PubCol("student_id", "Student ID", InBetweenEqualityFieldEquality),
            PubCol("section_id", "Section ID", InEquality),
            PubCol("date", "Day", Equality),
            PubCol("month", "Month", InEquality),
            PubCol("year", "Year", Equality),
            PubCol("comment", "Remarks", InEqualityLike),
            PubCol("top_student_id", "Top Student ID", FieldEquality)
          ),
          Set(
            PublicFactCol("total_marks", "Total Marks", InBetweenEqualityFieldEquality),
            PublicFactCol("obtained_marks", "Marks Obtained", InBetweenEqualityFieldEquality),
            PublicFactCol("Performance Factor", "Performance Factor", InBetweenEquality)
          ),
          Set.empty,
          getMaxDaysWindow, getMaxDaysLookBack
        )
    }
    registry.register(pubfactOracle)

    /*
     student_performance with new revision, designed to test multi engine query
     Fact is stored in the druid and dimensions are stored in oracle (local h2-mode)
     */
    def pubfactDruid: PublicFact = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        import com.yahoo.maha.core.DruidExpression._
        Fact.newFact(
          "student_grade_sheet", DailyGrain, DruidEngine, Set(StudentSchema),
          Set(
            DimCol("class_id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
            , DimCol("section_id", IntType(3))
            , DimCol("year", IntType(3, (Map(1 -> "Freshman", 2 -> "Sophomore", 3 -> "Junior", 4 -> "Senior"), "Other")))
            , DimCol("comment", StrType(), annotations = Set(EscapingRequired))
            , DimCol("date", DateType())
            , DimCol("month", DateType())
            , DimCol("top_student_id", IntType())
          ),
          Set(
            FactCol("total_marks", IntType())
            ,FactCol("obtained_marks", IntType())
            ,DruidDerFactCol("Performance Factor", DecType(10,2), "{obtained_marks}" /- "{total_marks}")
          )
        )
      }
        .toPublicFact("student_performance",
          Set(
            PubCol("class_id", "Class ID", InEquality),
            PubCol("student_id", "Student ID", InEqualityFieldEquality),
            PubCol("section_id", "Section ID", InEquality),
            PubCol("date", "Day", Equality),
            PubCol("month", "Month", InEquality),
            PubCol("year", "Year", Equality),
            PubCol("comment", "Remarks", InEqualityLike),
            PubCol("top_student_id", "Top Student ID", FieldEquality)
          ),
          Set(
            PublicFactCol("total_marks", "Total Marks", InBetweenEquality),
            PublicFactCol("obtained_marks", "Marks Obtained", InBetweenEquality),
            PublicFactCol("Performance Factor", "Performance Factor", InBetweenEquality)
          ),
          Set.empty,
          getMaxDaysWindow, getMaxDaysLookBack, revision = 1, dimRevision = 0
        )
    }
    registry.register(pubfactDruid)

    def pubfactOracleReduced: PublicFact = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        import com.yahoo.maha.core.OracleExpression._
        Fact.newFact(
          "student_grade_sheet_again", DailyGrain, OracleEngine, Set(StudentSchema),
          Set(
            DimCol("class_id", IntType(), annotations = Set(ForeignKey("class")))
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
            , DimCol("section_id", IntType(3), annotations = Set(ForeignKey("section")))
            , DimCol("year", IntType(3, (Map(1 -> "Freshman", 2 -> "Sophomore", 3 -> "Junior", 4 -> "Senior"), "Other")))
            , DimCol("comment", StrType(), annotations = Set(EscapingRequired))
            , DimCol("date", DateType())
            , DimCol("month", DateType())
            , DimCol("top_student_id", IntType())
          ),
          Set(
            FactCol("total_marks", IntType())
            , FactCol("obtained_marks", IntType())
            , OracleDerFactCol("Performance Factor", DecType(10, 2), "{obtained_marks}" /- "{total_marks}")
          )
        )
      }
        .toPublicFact("student_performance2",
          Set(
            PubCol("class_id", "Class ID", InEquality),
            PubCol("student_id", "Student ID", InEqualityFieldEquality),
            PubCol("section_id", "Section ID", InNotInEquality),
            PubCol("date", "Day", Equality),
            PubCol("month", "Month", InEquality),
            PubCol("year", "Year", Equality),
            PubCol("top_student_id", "Top Student ID", FieldEquality)
          ),
          Set(
            PublicFactCol("total_marks", "Total Marks", InBetweenEquality),
            PublicFactCol("obtained_marks", "Marks Obtained", InBetweenEquality)
          ),
          Set.empty,
          getMaxDaysWindow, getMaxDaysLookBack
        )
    }

    registry.register(pubfactOracleReduced)
  }

}

class SampleDimensionSchemaRegistrationFactory extends DimensionRegistrationFactory {
  override def register(registry: RegistryBuilder): Unit = {
    val student_dim: PublicDimension = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension("student", OracleEngine, LevelTwo, Set(StudentSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("department_id", IntType())
            , DimCol("admitted_year", IntType())
            , DimCol("status", StrType())
            , DimCol("profile_url", StrType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , schemaColMap = Map(StudentSchema -> "id")
          , annotations = Set(OracleHashPartitioning)
        ).toPublicDimension("student","student",
          Set(
            PubCol("id", "Student ID", InBetweenEqualityFieldEquality)
            , PubCol("name", "Student Name", EqualityFieldEquality)
            , PubCol("admitted_year", "Admitted Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Student Status", InEqualityFieldEquality)
            , PubCol("profile_url", "Profile URL", InEqualityLike, isImageColumn = true)
          ), highCardinalityFilters = Set(NotInFilter("Student Status", List("DELETED")))
        )
      }
    }
    // TODO: fix class, should be level to 2 and should contain foreign keys to student

    val class_dim: PublicDimension = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension("class", OracleEngine, LevelOne, Set(StudentSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("department_id", IntType())
            , DimCol("start_year", IntType())
            , DimCol("status", StrType())
            , DimCol("professor", StrType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(OracleHashPartitioning)
        ).toPublicDimension("class","class",
          Set(
            PubCol("id", "Class ID", Equality)
            , PubCol("name", "Class Name", Equality)
            , PubCol("start_year", "Start Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Class Status", InEquality)
            , PubCol("professor", "Professor Name", Equality)
          ), highCardinalityFilters = Set(NotInFilter("Class Status", List("DELETED")))
        )
      }
    }

    /*
     Section Dimension: although class and student foreign keys in the section dim violates the 2NF and 3NF, it is just example
     describing LevelThree dim. LevelThree dim always contains level one and level two foreign keys.
     */
    val section_dim: PublicDimension = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension("section", OracleEngine, LevelThree, Set(StudentSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
            , DimCol("class_id", IntType(), annotations = Set(ForeignKey("class")))
            , DimCol("start_year", IntType())
            , DimCol("status", StrType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(OracleHashPartitioning)
        ).toPublicDimension("section","section",
          Set(
            PubCol("id", "Section ID", InNotInEquality)
            , PubCol("student_id", "Student ID", Equality)
            , PubCol("class_id", "Class ID", Equality)
            , PubCol("name", "Section Name", Equality)
            , PubCol("start_year", "Section Start Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Section Status", InEquality)
          ), highCardinalityFilters = Set(NotInFilter("Section Status", List("DELETED")))
        )
      }
    }

    registry.register(section_dim)
    registry.register(class_dim)
    registry.register(student_dim)
  }
}
