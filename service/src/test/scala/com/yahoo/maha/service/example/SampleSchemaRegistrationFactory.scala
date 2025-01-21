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
            , DimCol("batch_id", IntType(), annotations = Set(ForeignKey("batch")))
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
            , DimCol("group_id", IntType(), annotations = Set(ForeignKey("grp")))
            , DimCol("section_id", IntType(3), annotations = Set(PrimaryKey))
            , DimCol("myyear", IntType(3, (Map(1 -> "Freshman", 2 -> "Sophomore", 3 -> "Junior", 4 -> "Senior"), "Other")))
            , DimCol("mycomment", StrType(), annotations = Set(EscapingRequired))
            , DimCol("mydate", DateType())
            , DimCol("myhour", IntType(2))
            , DimCol("mymonth", DateType())
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
            PubCol("batch_id", "Batch ID", InEquality),
            PubCol("student_id", "Student ID", InBetweenEqualityFieldEquality),
            PubCol("group_id", "Group ID", InEquality),
            PubCol("section_id", "Section ID", InEquality),
            PubCol("mydate", "Day", Equality),
            PubCol("myhour", "Hour", Equality),
            PubCol("mymonth", "Month", InEquality),
            PubCol("myyear", "Year", Equality),
            PubCol("mycomment", "Remarks", InEqualityLike),
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
            , DimCol("myyear", IntType(3, (Map(1 -> "Freshman", 2 -> "Sophomore", 3 -> "Junior", 4 -> "Senior"), "Other")))
            , DimCol("mycomment", StrType(), annotations = Set(EscapingRequired, ForeignKey("remarks")))
            , DimCol("mycomment2", StrType(), annotations = Set(EscapingRequired))
            , DimCol("mydate", DateType())
            , DimCol("mymonth", DateType())
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
            PubCol("mydate", "Day", Equality),
            PubCol("mymonth", "Month", InEquality),
            PubCol("myyear", "Year", Equality),
            PubCol("mycomment", "Remarks", InEqualityLike),
            PubCol("mycomment2", "Remarks2", InEqualityLike),
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
            , DimCol("batch_id", IntType(), annotations = Set(ForeignKey("batch")))
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
            , DimCol("group_id", IntType(3), annotations = Set(ForeignKey("grp"))) 
            , DimCol("section_id", IntType(3), annotations = Set(ForeignKey("section")))
            , DimCol("myyear", IntType(3, (Map(1 -> "Freshman", 2 -> "Sophomore", 3 -> "Junior", 4 -> "Senior"), "Other")))
            , DimCol("mycomment", StrType(), annotations = Set(EscapingRequired))
            , DimCol("mydate", DateType())
            , DimCol("mymonth", DateType())
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
            PubCol("batch_id", "Batch ID", InEquality),
            PubCol("student_id", "Student ID", InEqualityFieldEquality),
            PubCol("group_id", "Group ID", InNotInEquality),
            PubCol("section_id", "Section ID", InNotInEquality),
            PubCol("mydate", "Day", Equality),
            PubCol("mymonth", "Month", InEquality),
            PubCol("myyear", "Year", Equality),
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

    def pubfactStudentPerf: PublicFact = {
      val builder = ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        import com.yahoo.maha.core.OracleExpression._
        Fact.newFact(
          "student_performance", DailyGrain, OracleEngine, Set(StudentSchema),
          Set(
            DimCol("class_id", IntType(), annotations = Set(ForeignKey("class")))
            , DimCol("batch_id", IntType(), annotations = Set(ForeignKey("batch")))
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
            , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
            , DimCol("class_volunteer_id", IntType(), annotations = Set(ForeignKey("class_volunteers")))
            , DimCol("science_lab_volunteer_id", IntType(), annotations = Set(ForeignKey("science_lab_volunteers")))
            , DimCol("tutor_id", IntType(), annotations = Set(ForeignKey("tutors")))
            , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
            , DimCol("group_id", IntType(), annotations = Set(ForeignKey("grp")))
            , DimCol("section_id", IntType(3), annotations = Set(PrimaryKey))
            , DimCol("myyear", IntType(3, (Map(1 -> "Freshman", 2 -> "Sophomore", 3 -> "Junior", 4 -> "Senior"), "Other")))
            , DimCol("mycomment", StrType(), annotations = Set(EscapingRequired))
            , DimCol("mydate", DateType())
            , DimCol("mymonth", DateType())
            , DimCol("top_student_id", IntType())
          ),
          Set(
            FactCol("total_marks", IntType())
            , FactCol("obtained_marks", IntType())
            , OracleDerFactCol("Performance Factor", DecType(10, 2), "{obtained_marks}" /- "{total_marks}")
          )
        )
      }

      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          builder.withAlternativeEngine("hive_student_performance", "student_performance", HiveEngine)
      }

      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          builder.withAlternativeEngine("presto_student_performance", "student_performance", PrestoEngine)
      }

      ColumnContext.withColumnContext {
        implicit dc: ColumnContext =>
          builder.withAlternativeEngine("dr_student_performance", "student_performance", DruidEngine)
      }

      builder.toPublicFact("student_performance",
        Set(
          PubCol("class_id", "Class ID", InEquality),
          PubCol("batch_id", "Batch ID", InEquality),
          PubCol("student_id", "Student ID", InBetweenEqualityFieldEquality),
          PubCol("researcher_id", "Researcher ID", InBetweenEqualityFieldEquality),
          PubCol("class_volunteer_id", "Class Volunteer ID", InBetweenEqualityFieldEquality),
          PubCol("science_lab_volunteer_id", "Science Lab Volunteer ID", InBetweenEqualityFieldEquality),
          PubCol("tutor_id", "Tutor ID", InBetweenEqualityFieldEquality),
          PubCol("lab_id", "Lab ID", InBetweenEqualityFieldEquality),
          PubCol("section_id", "Section ID", InEquality),
          PubCol("group_id", "Group ID", InEquality),
          PubCol("mydate", "Day", Equality),
          PubCol("mymonth", "Month", InEquality),
          PubCol("myyear", "Year", Equality),
          PubCol("mycomment", "Remarks", InEqualityLike),
          PubCol("top_student_id", "Top Student ID", FieldEquality)
        ),
        Set(
          PublicFactCol("total_marks", "Total Marks", InBetweenEqualityFieldEquality),
          PublicFactCol("obtained_marks", "Marks Obtained", InBetweenEqualityFieldEquality),
          PublicFactCol("Performance Factor", "Performance Factor", InBetweenEquality)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack, revision = 3, dimRevision = 1
      )
    }

    registry.register(pubfactStudentPerf)

    def pubfactDruidOnly: PublicFact = {
      val builder = ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        import com.yahoo.maha.core.OracleExpression._
        Fact.newFact(
          "druid_perf", DailyGrain, DruidEngine, Set(StudentSchema),
          Set(
            DimCol("class_id", IntType(), annotations = Set(ForeignKey("class")))
            , DimCol("batch_id", IntType(), annotations = Set(ForeignKey("batch")))
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
            , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher_druid")))
            , DimCol("class_volunteer_id", IntType(), annotations = Set(ForeignKey("class_volunteers")))
            , DimCol("science_lab_volunteer_id", IntType(), annotations = Set(ForeignKey("science_lab_volunteers")))
            , DimCol("tutor_id", IntType(), annotations = Set(ForeignKey("tutors")))
            , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
            , DimCol("group_id", IntType(), annotations = Set(ForeignKey("researcher_druid")))
            , DimCol("section_id", IntType(3), annotations = Set(PrimaryKey))
            , DimCol("myyear", IntType(3, (Map(1 -> "Freshman", 2 -> "Sophomore", 3 -> "Junior", 4 -> "Senior"), "Other")))
            , DimCol("mycomment", StrType(), annotations = Set(EscapingRequired))
            , DimCol("mydate", DateType())
            , DimCol("mymonth", DateType())
            , DimCol("top_student_id", IntType())
          ),
          Set(
            FactCol("total_marks", IntType())
            , FactCol("obtained_marks", IntType())
          )
        )
      }

      builder.toPublicFact("druid_performance",
        Set(
          PubCol("class_id", "Class ID", InEquality),
          PubCol("batch_id", "Batch ID", InEquality),
          PubCol("student_id", "Student ID", InBetweenEqualityFieldEquality),
          PubCol("researcher_id", "Researcher ID", InBetweenEqualityFieldEquality),
          PubCol("class_volunteer_id", "Class Volunteer ID", InBetweenEqualityFieldEquality),
          PubCol("science_lab_volunteer_id", "Science Lab Volunteer ID", InBetweenEqualityFieldEquality),
          PubCol("tutor_id", "Tutor ID", InBetweenEqualityFieldEquality),
          PubCol("lab_id", "Lab ID", InBetweenEqualityFieldEquality),
          PubCol("section_id", "Section ID", InEquality),
          PubCol("group_id", "Group ID", InEquality),
          PubCol("mydate", "Day", Equality),
          PubCol("mymonth", "Month", InEquality),
          PubCol("myyear", "Year", Equality),
          PubCol("mycomment", "Remarks", InEqualityLike),
          PubCol("top_student_id", "Top Student ID", FieldEquality)
        ),
        Set(
          PublicFactCol("total_marks", "Total Marks", InBetweenEqualityFieldEquality),
          PublicFactCol("obtained_marks", "Marks Obtained", InBetweenEqualityFieldEquality)
        ),
        Set.empty,
        getMaxDaysWindow, getMaxDaysLookBack
      )
    }

    registry.register(pubfactDruidOnly)
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

    val student_dim_v1: PublicDimension = {
      val builder : DimensionBuilder = {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          Dimension.newDimension("student_v1", OracleEngine, LevelTwo, Set(StudentSchema),
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
              , DimCol("class_volunteer_id", IntType(), annotations = Set(ForeignKey("class_volunteers")))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
              , DimCol("profile_url", StrType())
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
            , schemaColMap = Map(StudentSchema -> "id")
            , annotations = Set(OracleHashPartitioning)
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "hive_student_v1",
            "student_v1",
            HiveEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
              , DimCol("class_volunteer_id", IntType(), annotations = Set(ForeignKey("class_volunteers")))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
              , DimCol("profile_url", StrType())
              , HivePartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "presto_student_v1",
            "student_v1",
            PrestoEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
              , DimCol("class_volunteer_id", IntType(), annotations = Set(ForeignKey("class_volunteers")))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
              , DimCol("profile_url", StrType())
              , PrestoPartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "dr_student_v1",
            "student_v1",
            DruidEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
              , DimCol("class_volunteer_id", IntType(), annotations = Set(ForeignKey("class_volunteers")))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
              , DimCol("profile_url", StrType())
            )
          )
        }
      }

      builder.toPublicDimension("student","student",
        Set(
          PubCol("id", "Student ID", InBetweenEqualityFieldEquality)
          , PubCol("researcher_id", "Researcher ID", InBetweenEqualityFieldEquality)
          , PubCol("class_volunteer_id", "Class Volunteer ID", InBetweenEqualityFieldEquality)
          , PubCol("name", "Student Name", EqualityFieldEquality)
          , PubCol("admitted_year", "Admitted Year", InEquality, hiddenFromJson = true)
          , PubCol("status", "Student Status", InEqualityFieldEquality)
          , PubCol("profile_url", "Profile URL", InEqualityLike, isImageColumn = true)
        ), highCardinalityFilters = Set(NotInFilter("Student Status", List("DELETED"))), revision = 1
      )
    }

    val researcher_dim: PublicDimension = {
      val builder : DimensionBuilder = {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          Dimension.newDimension("researcher", OracleEngine, LevelTwo, Set(StudentSchema),
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("science_lab_volunteer_id", IntType(), annotations = Set(ForeignKey("science_lab_volunteers")))
              , DimCol("tutor_id", IntType(), annotations = Set(ForeignKey("tutors")))
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
              , DimCol("profile_url", StrType())
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
            , annotations = Set(OracleHashPartitioning)
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "hive_researcher",
            "researcher",
            HiveEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("science_lab_volunteer_id", IntType(), annotations = Set(ForeignKey("science_lab_volunteers")))
              , DimCol("tutor_id", IntType(), annotations = Set(ForeignKey("tutors")))
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
              , DimCol("profile_url", StrType())
              , HivePartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "presto_researcher",
            "researcher",
            PrestoEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("science_lab_volunteer_id", IntType(), annotations = Set(ForeignKey("science_lab_volunteers")))
              , DimCol("tutor_id", IntType(), annotations = Set(ForeignKey("tutors")))
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
              , DimCol("profile_url", StrType())
              , PrestoPartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "dr_researcher",
            "researcher",
            DruidEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("science_lab_volunteer_id", IntType(), annotations = Set(ForeignKey("science_lab_volunteers")))
              , DimCol("tutor_id", IntType(), annotations = Set(ForeignKey("tutors")))
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
              , DimCol("profile_url", StrType())
            )
          )
        }
      }

      builder.toPublicDimension("researcher","researcher",
        Set(
          PubCol("id", "Researcher ID", InBetweenEqualityFieldEquality)
          , PubCol("science_lab_volunteer_id", "Science Lab Volunteer ID", InBetweenEqualityFieldEquality)
          , PubCol("tutor_id", "Tutor ID", InBetweenEqualityFieldEquality)
          , PubCol("name", "Researcher Name", EqualityFieldEquality)
          , PubCol("admitted_year", "Researcher Admitted Year", InEquality, hiddenFromJson = true)
          , PubCol("status", "Researcher Status", InEqualityFieldEquality)
          , PubCol("profile_url", "Researcher Profile URL", InEqualityLike, isImageColumn = true)
        ), highCardinalityFilters = Set(NotInFilter("Researcher Status", List("DELETED")))
      )
    }

    val remarks_dim: PublicDimension = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension("remarks", DruidEngine, LevelTwo, Set(StudentSchema),
          Set(
            DimCol("id", StrType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("department_id", IntType())
            , DimCol("admitted_year", IntType())
            , DimCol("status", StrType())
            , DimCol("profile_url", StrType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
        ).toPublicDimension("remarks","remarks",
          Set(
            PubCol("id", "Remarks", InBetweenEqualityFieldEquality)
            , PubCol("name", "Remark Name", EqualityFieldEquality)
            , PubCol("admitted_year", "Remark Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Remark Status", InEqualityFieldEquality)
            , PubCol("profile_url", "Remark URL", InEqualityLike, isImageColumn = true)
          )
        )
      }
    }
    // TODO: fix class, should be level to 2 and should contain foreign keys to student

    val class_dim: PublicDimension = {
      val builder : DimensionBuilder = {
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
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "hive_class",
            "class",
            HiveEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
              , DimCol("professor", StrType())
              , HivePartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "presto_class",
            "class",
            PrestoEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
              , DimCol("professor", StrType())
              , PrestoPartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "dr_class",
            "class",
            DruidEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
              , DimCol("professor", StrType())
            )
          )
        }
      }

      builder.toPublicDimension("class","class",
        Set(
          PubCol("id", "Class ID", Equality)
          , PubCol("name", "Class Name", Equality)
          , PubCol("start_year", "Start Year", InEquality, hiddenFromJson = true)
          , PubCol("status", "Class Status", InEquality)
          , PubCol("professor", "Professor Name", Equality)
        ), highCardinalityFilters = Set(NotInFilter("Class Status", List("DELETED")))
      )
    }

    val batch_dim: PublicDimension = {
      val builder: DimensionBuilder = {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          Dimension.newDimension("batch", OracleEngine, LevelOne, Set(StudentSchema),
            Set(DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("class_id", IntType(), annotations = Set(ForeignKey("class")))
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
            , annotations = Set(OracleHashPartitioning)
            , secondaryDimLevel = Some(0)
          )
        }
      }

      builder.toPublicDimension("batch","batch",
        Set(
          PubCol("id", "Batch ID", InNotInEquality)
          , PubCol("name", "Batch Name", InNotInEquality)
          , PubCol("class_id", "Class ID", Equality)
        )
      )
    }

    /*
     Section Dimension: although class and student foreign keys in the section dim violates the 2NF and 3NF, it is just example
     describing LevelThree dim. LevelThree dim always contains level one and level two foreign keys.
     */
    val section_dim: PublicDimension = {
      val builder : DimensionBuilder = {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          Dimension.newDimension("section", OracleEngine, LevelThree, Set(StudentSchema),
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
              , DimCol("class_id", IntType(), annotations = Set(ForeignKey("class")))
              , DimCol("batch_id", IntType(), annotations = Set(ForeignKey("batch")))
              , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
            , annotations = Set(OracleHashPartitioning)
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "hive_section",
            "section",
            HiveEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
              , DimCol("class_id", IntType(), annotations = Set(ForeignKey("class")))
              , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
              , HivePartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "presto_section",
            "section",
            PrestoEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
              , DimCol("class_id", IntType(), annotations = Set(ForeignKey("class")))
              , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
              , PrestoPartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "dr_section",
            "section",
            DruidEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
              , DimCol("class_id", IntType(), annotations = Set(ForeignKey("class")))
              , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
            )
          )
        }
      }

      builder.toPublicDimension("section","section",
        Set(
          PubCol("id", "Section ID", InNotInEquality)
          , PubCol("student_id", "Student ID", Equality)
          , PubCol("class_id", "Class ID", Equality)
          , PubCol("batch_id", "Batch ID", Equality)
          , PubCol("lab_id", "Lab ID", InBetweenEqualityFieldEquality)
          , PubCol("name", "Section Name", Equality)
          , PubCol("start_year", "Section Start Year", InEquality, hiddenFromJson = true)
          , PubCol("status", "Section Status", InEquality)
        ), highCardinalityFilters = Set(NotInFilter("Section Status", List("DELETED")))
      )
    }

    val lab_dim: PublicDimension = {
      val builder : DimensionBuilder = {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          Dimension.newDimension("lab", OracleEngine, LevelThree, Set(StudentSchema),
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
              , DimCol("name", StrType())
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
            , annotations = Set(OracleHashPartitioning)
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "hive_lab",
            "lab",
            HiveEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
              , DimCol("name", StrType())
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
              , HivePartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "presto_lab",
            "lab",
            PrestoEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
              , DimCol("name", StrType())
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
              , PrestoPartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "dr_lab",
            "lab",
            DruidEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
              , DimCol("name", StrType())
              , DimCol("start_year", IntType())
              , DimCol("status", StrType())
            )
          )
        }
      }

      builder.toPublicDimension("labs","lab",
        Set(
          PubCol("id", "Lab ID", InNotInEquality)
          , PubCol("researcher_id", "Researcher ID", InBetweenEqualityFieldEquality)
          , PubCol("name", "Lab Name", Equality)
          , PubCol("start_year", "Lab Start Year", InEquality, hiddenFromJson = true)
          , PubCol("status", "Lab Status", InEquality)
        )
      )
    }

    val class_volunteer_dim: PublicDimension = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension("class_volunteer", OracleEngine, LevelTwo, Set(StudentSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("department_id", IntType())
            , DimCol("admitted_year", IntType())
            , DimCol("status", StrType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(OracleHashPartitioning)
        ).toPublicDimension("class_volunteers","class_volunteer",
          Set(
            PubCol("id", "Class Volunteer ID", InBetweenEqualityFieldEquality)
            , PubCol("name", "Class Volunteer Name", EqualityFieldEquality)
            , PubCol("admitted_year", "Class Volunteer Admitted Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Class Volunteer Status", InEqualityFieldEquality)
          ), highCardinalityFilters = Set(NotInFilter("Class Volunteer Status", List("DELETED")))
        )
      }
    }

    val science_lab_volunteer_dim: PublicDimension = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension("science_lab_volunteer", OracleEngine, LevelTwo, Set(StudentSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("department_id", IntType())
            , DimCol("admitted_year", IntType())
            , DimCol("status", StrType())
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(OracleHashPartitioning)
        ).toPublicDimension("science_lab_volunteers","science_lab_volunteer",
          Set(
            PubCol("id", "Science Lab Volunteer ID", InBetweenEqualityFieldEquality)
            , PubCol("name", "Science Lab Volunteer Name", EqualityFieldEquality)
            , PubCol("admitted_year", "Science Lab Volunteer Admitted Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Science Lab Volunteer Status", InEqualityFieldEquality)
          ), highCardinalityFilters = Set(NotInFilter("Science Lab Volunteer Status", List("DELETED")))
        )
      }
    }

    val tutor_dim: PublicDimension = {
      val builder : DimensionBuilder = {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          Dimension.newDimension("tutor", OracleEngine, LevelTwo, Set(StudentSchema),
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
            )
            , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
            , annotations = Set(OracleHashPartitioning)
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "hive_tutor",
            "tutor",
            HiveEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
              , HivePartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "presto_tutor",
            "tutor",
            PrestoEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
              , PrestoPartDimCol("load_time", StrType(), annotations = Set.empty)
            )
          )
        }
      }

      {
        ColumnContext.withColumnContext { implicit dc: ColumnContext =>
          builder.withAlternateEngine(
            "dr_tutor",
            "tutor",
            DruidEngine,
            Set(
              DimCol("id", IntType(), annotations = Set(PrimaryKey))
              , DimCol("name", StrType())
              , DimCol("department_id", IntType())
              , DimCol("admitted_year", IntType())
              , DimCol("status", StrType())
            )
          )
        }
      }

      builder.toPublicDimension("tutors","tutor",
        Set(
          PubCol("id", "Tutor ID", InBetweenEqualityFieldEquality)
          , PubCol("name", "Tutor Name", EqualityFieldEquality)
          , PubCol("admitted_year", "Tutor Admitted Year", InEquality, hiddenFromJson = true)
          , PubCol("status", "Tutor Status", InEqualityFieldEquality)
        ), highCardinalityFilters = Set(NotInFilter("Tutor Status", List("DELETED")))
      )
    }

    val group_dim: PublicDimension = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension("grp", OracleEngine, LevelFour, Set(StudentSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(OracleHashPartitioning)
        ).toPublicDimension("grp","grp",
          Set(
            PubCol("id", "Group ID", InBetweenEqualityFieldEquality)
            , PubCol("name", "Group Name", EqualityFieldEquality)
            , PubCol("lab_id", "Lab ID", InEqualityFieldEquality)
            , PubCol("student_id", "Student ID", InEqualityFieldEquality)
          )
        )
      }
    }

    val druid_dim: PublicDimension = {
      ColumnContext.withColumnContext { implicit dc: ColumnContext =>
        Dimension.newDimension("researcher_druid", OracleEngine, LevelFour, Set(StudentSchema),
          Set(
            DimCol("id", IntType(), annotations = Set(PrimaryKey))
            , DimCol("name", StrType())
            , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
            , DimCol("student_id", IntType(), annotations = Set(ForeignKey("student")))
          )
          , Option(Map(AsyncRequest -> 400, SyncRequest -> 400))
          , annotations = Set(OracleHashPartitioning)
        ).toPublicDimension("researcher_druid", "researcher_druid",
          Set(
            PubCol("id", "Group ID", InBetweenEqualityFieldEquality)
            , PubCol("name", "Group Name", EqualityFieldEquality)
            , PubCol("lab_id", "Lab ID", InEqualityFieldEquality)
            , PubCol("student_id", "Student ID", InEqualityFieldEquality)
          )
        )
      }
    }

    registry.register(section_dim)
    registry.register(class_dim)
    registry.register(batch_dim)
    registry.register(student_dim)
    registry.register(student_dim_v1)
    registry.register(researcher_dim)
    registry.register(lab_dim)
    registry.register(remarks_dim)
    registry.register(class_volunteer_dim)
    registry.register(science_lab_volunteer_dim)
    registry.register(tutor_dim)
    registry.register(group_dim)
    registry.register(druid_dim)
  }
}
