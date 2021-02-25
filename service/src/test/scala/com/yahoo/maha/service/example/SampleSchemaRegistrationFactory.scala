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
            , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
            , DimCol("class_volunteer_id", IntType(), annotations = Set(ForeignKey("class_volunteers")))
            , DimCol("science_lab_volunteer_id", IntType(), annotations = Set(ForeignKey("science_lab_volunteers")))
            , DimCol("tutor_id", IntType(), annotations = Set(ForeignKey("tutors")))
            , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
            , DimCol("section_id", IntType(3), annotations = Set(PrimaryKey))
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
            PubCol("researcher_id", "Researcher ID", InBetweenEqualityFieldEquality),
            PubCol("class_volunteer_id", "Class Volunteer ID", InBetweenEqualityFieldEquality),
            PubCol("science_lab_volunteer_id", "Science Lab Volunteer ID", InBetweenEqualityFieldEquality),
            PubCol("tutor_id", "Tutor ID", InBetweenEqualityFieldEquality),
            PubCol("lab_id", "Lab ID", InBetweenEqualityFieldEquality),
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
            , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
            , DimCol("class_volunteer_id", IntType(), annotations = Set(ForeignKey("class_volunteers")))
            , DimCol("science_lab_volunteer_id", IntType(), annotations = Set(ForeignKey("science_lab_volunteers")))
            , DimCol("tutor_id", IntType(), annotations = Set(ForeignKey("tutors")))
            , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
            , DimCol("section_id", IntType(3))
            , DimCol("year", IntType(3, (Map(1 -> "Freshman", 2 -> "Sophomore", 3 -> "Junior", 4 -> "Senior"), "Other")))
            , DimCol("comment", StrType(), annotations = Set(EscapingRequired, ForeignKey("remarks")))
            , DimCol("comment2", StrType(), annotations = Set(EscapingRequired))
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
            PubCol("researcher_id", "Researcher ID", InBetweenEqualityFieldEquality),
            PubCol("class_volunteer_id", "Class Volunteer ID", InBetweenEqualityFieldEquality),
            PubCol("science_lab_volunteer_id", "Science Lab Volunteer ID", InBetweenEqualityFieldEquality),
            PubCol("tutor_id", "Tutor ID", InBetweenEqualityFieldEquality),
            PubCol("lab_id", "Lab ID", InBetweenEqualityFieldEquality),
            PubCol("section_id", "Section ID", InEquality),
            PubCol("date", "Day", Equality),
            PubCol("month", "Month", InEquality),
            PubCol("year", "Year", Equality),
            PubCol("comment", "Remarks", InEqualityLike),
            PubCol("comment2", "Remarks2", InEqualityLike),
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
            , DimCol("researcher_id", IntType(), annotations = Set(ForeignKey("researcher")))
            , DimCol("class_volunteer_id", IntType(), annotations = Set(ForeignKey("class_volunteers")))
            , DimCol("science_lab_volunteer_id", IntType(), annotations = Set(ForeignKey("science_lab_volunteers")))
            , DimCol("tutor_id", IntType(), annotations = Set(ForeignKey("tutors")))
            , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
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
            PubCol("researcher_id", "Researcher ID", InBetweenEqualityFieldEquality),
            PubCol("class_volunteer_id", "Class Volunteer ID", InBetweenEqualityFieldEquality),
            PubCol("science_lab_volunteer_id", "Science Lab Volunteer ID", InBetweenEqualityFieldEquality),
            PubCol("tutor_id", "Tutor ID", InBetweenEqualityFieldEquality),
            PubCol("lab_id", "Lab ID", InBetweenEqualityFieldEquality),
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
        ).toPublicDimension("student","student",
          Set(
            PubCol("id", "Student ID", InBetweenEqualityFieldEquality)
            , PubCol("researcher_id", "Researcher ID", InBetweenEqualityFieldEquality)
            , PubCol("class_volunteer_id", "Class Volunteer ID", InBetweenEqualityFieldEquality)
            , PubCol("name", "Student Name", EqualityFieldEquality)
            , PubCol("admitted_year", "Admitted Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Student Status", InEqualityFieldEquality)
            , PubCol("profile_url", "Profile URL", InEqualityLike, isImageColumn = true)
          ), highCardinalityFilters = Set(NotInFilter("Student Status", List("DELETED")))
        )
      }
    }

    val researcher_dim: PublicDimension = {
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
        ).toPublicDimension("researcher","researcher",
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
            , DimCol("lab_id", IntType(), annotations = Set(ForeignKey("labs")))
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
            , PubCol("lab_id", "Lab ID", InBetweenEqualityFieldEquality)
            , PubCol("name", "Section Name", Equality)
            , PubCol("start_year", "Section Start Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Section Status", InEquality)
          ), highCardinalityFilters = Set(NotInFilter("Section Status", List("DELETED")))
        )
      }
    }

    val lab_dim: PublicDimension = {
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
        ).toPublicDimension("labs","lab",
          Set(
            PubCol("id", "Lab ID", InNotInEquality)
            , PubCol("researcher_id", "Researcher ID", InBetweenEqualityFieldEquality)
            , PubCol("name", "Lab Name", Equality)
            , PubCol("start_year", "Lab Start Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Lab Status", InEquality)
          )
        )
      }
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
        ).toPublicDimension("tutors","tutor",
          Set(
            PubCol("id", "Tutor ID", InBetweenEqualityFieldEquality)
            , PubCol("name", "Tutor Name", EqualityFieldEquality)
            , PubCol("admitted_year", "Tutor Admitted Year", InEquality, hiddenFromJson = true)
            , PubCol("status", "Tutor Status", InEqualityFieldEquality)
          ), highCardinalityFilters = Set(NotInFilter("Tutor Status", List("DELETED")))
        )
      }
    }

    registry.register(section_dim)
    registry.register(class_dim)
    registry.register(student_dim)
    registry.register(researcher_dim)
    registry.register(lab_dim)
    registry.register(remarks_dim)
    registry.register(class_volunteer_dim)
    registry.register(science_lab_volunteer_dim)
    registry.register(tutor_dim)
  }
}
