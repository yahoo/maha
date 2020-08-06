package com.yahoo.maha.core.helper.jdbc

import java.io.Writer

import com.yahoo.maha.jdbc.{JdbcConnection, List}
import grizzled.slf4j.Logging
import org.apache.commons.lang3.StringUtils

/**
 * Inspired by stack overflow answers and other sources
 * https://stackoverflow.com/questions/2593803/how-to-generate-the-create-table-sql-statement-for-an-existing-table-in-postgr
 * https://stackoverflow.com/questions/41495613/how-to-generate-a-list-of-ddl-objects-or-functions-that-are-deployed-to-or-missi
 * https://stackoverflow.com/questions/6898453/how-to-display-the-function-procedure-triggers-source-code-in-postgresql
 * https://rextester.com/FTOUG46985
 * https://alberton.info/postgresql_meta_info.html
 */
object PostgresDDLDumper extends Logging {

  import JdbcSchemaDumper._

  val createProc: String =
    """
      |CREATE OR REPLACE FUNCTION public.generate_create_table_statement(p_table_name character varying)
      |  RETURNS SETOF text AS
      |$BODY$
      |DECLARE
      |    v_table_ddl   text;
      |    column_record record;
      |    table_rec record;
      |    constraint_rec record;
      |    firstrec boolean;
      |BEGIN
      |    FOR table_rec IN
      |        SELECT c.relname FROM pg_catalog.pg_class c
      |            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      |                WHERE relkind = 'r'
      |                AND relname~ ('^('||p_table_name||')$')
      |                AND n.nspname <> 'pg_catalog'
      |                AND n.nspname <> 'information_schema'
      |                AND n.nspname !~ '^pg_toast'
      |                AND pg_catalog.pg_table_is_visible(c.oid)
      |          ORDER BY c.relname
      |    LOOP
      |
      |        FOR column_record IN
      |            SELECT
      |                b.nspname as schema_name,
      |                b.relname as table_name,
      |                a.attname as column_name,
      |                pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,
      |                CASE WHEN
      |                    (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
      |                     FROM pg_catalog.pg_attrdef d
      |                     WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) IS NOT NULL THEN
      |                    'DEFAULT '|| (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
      |                                  FROM pg_catalog.pg_attrdef d
      |                                  WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
      |                ELSE
      |                    ''
      |                END as column_default_value,
      |                CASE WHEN a.attnotnull = true THEN
      |                    'NOT NULL'
      |                ELSE
      |                    'NULL'
      |                END as column_not_null,
      |                a.attnum as attnum,
      |                e.max_attnum as max_attnum
      |            FROM
      |                pg_catalog.pg_attribute a
      |                INNER JOIN
      |                 (SELECT c.oid,
      |                    n.nspname,
      |                    c.relname
      |                  FROM pg_catalog.pg_class c
      |                       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      |                  WHERE c.relname = table_rec.relname
      |                    AND pg_catalog.pg_table_is_visible(c.oid)
      |                  ORDER BY 2, 3) b
      |                ON a.attrelid = b.oid
      |                INNER JOIN
      |                 (SELECT
      |                      a.attrelid,
      |                      max(a.attnum) as max_attnum
      |                  FROM pg_catalog.pg_attribute a
      |                  WHERE a.attnum > 0
      |                    AND NOT a.attisdropped
      |                  GROUP BY a.attrelid) e
      |                ON a.attrelid=e.attrelid
      |            WHERE a.attnum > 0
      |              AND NOT a.attisdropped
      |            ORDER BY a.attnum
      |        LOOP
      |            IF column_record.attnum = 1 THEN
      |                v_table_ddl:='CREATE TABLE '||column_record.schema_name||'.'||column_record.table_name||' (';
      |            ELSE
      |                v_table_ddl:=v_table_ddl||',';
      |            END IF;
      |
      |            IF column_record.attnum <= column_record.max_attnum THEN
      |                v_table_ddl:=v_table_ddl||chr(10)||
      |                         '    '||column_record.column_name||' '||column_record.column_type||' '||column_record.column_default_value||' '||column_record.column_not_null;
      |            END IF;
      |        END LOOP;
      |
      |        firstrec := TRUE;
      |        FOR constraint_rec IN
      |            SELECT conname, pg_get_constraintdef(c.oid) as constrainddef
      |                FROM pg_constraint c
      |                    WHERE conrelid=(
      |                        SELECT attrelid FROM pg_attribute
      |                        WHERE attrelid = (
      |                            SELECT oid FROM pg_class WHERE relname = table_rec.relname
      |                        ) AND attname='tableoid'
      |                    )
      |        LOOP
      |            v_table_ddl:=v_table_ddl||','||chr(10);
      |            v_table_ddl:=v_table_ddl||'CONSTRAINT '||constraint_rec.conname;
      |            v_table_ddl:=v_table_ddl||chr(10)||'    '||constraint_rec.constrainddef;
      |            firstrec := FALSE;
      |        END LOOP;
      |        v_table_ddl:=v_table_ddl||');';
      |        RETURN NEXT v_table_ddl;
      |    END LOOP;
      |END;
      |$BODY$
      |  LANGUAGE plpgsql VOLATILE
      |  COST 100;
      |ALTER FUNCTION public.generate_create_table_statement(character varying)
      |  OWNER TO postgres;
    """.stripMargin

  val dropProc: String =
    """
      |DROP FUNCTION generate_create_table_statement(p_table_name varchar);
    """.stripMargin

  def dump(jdbcConnection: JdbcConnection, schemaDump: SchemaDump, writer: Writer, config: DDLDumpConfig): Unit = {
    val tryCreateProc = jdbcConnection.execute(createProc)
    require(tryCreateProc.isSuccess, "Failed to create generate function : " + tryCreateProc.failed.get.getMessage)
    val tables: List[String] = schemaDump.tableMetadata.tables.map(t => t -> schemaDump.tableLevels(t)).toList.sortBy(_._2.level).map(_._1)
    info(s"Processing tables in order : $tables")
    tables.foreach {
      table =>
        val selectSql = s"SELECT generate_create_table_statement('$table')"
        jdbcConnection.queryForObject(selectSql) {
          resultSet =>
            var count = 0
            while (resultSet.next()) {
              if (count == 0) {
                info(resultSet.getMetaData.extractResultSetMetaData)
              }
              val ddl = resultSet.getString("generate_create_table_statement") + "\n"
              if (StringUtils.isNoneBlank(ddl)) {
                writer.write(ddl)
                info(ddl)
              }
              count += 1
            }
        }
    }
    val tryDropProc = jdbcConnection.execute(dropProc)
    require(tryDropProc.isSuccess, "Failed to drop generate procedure : " + tryDropProc.failed.get.getMessage)

    config.procsLike.foreach {
      case LikeCriteria(schemaLike, procLike) =>
        val procSql =
          s"""SELECT
             |    routine_name,
             |    routine_definition
             |FROM
             |    information_schema.routines
             |WHERE
             |    specific_schema LIKE '$schemaLike'
             |    AND routine_name LIKE '$procLike';""".stripMargin
        jdbcConnection.queryForObject(procSql) {
          rs =>
            var count = 0
            while (rs.next()) {
              if (count == 0) {
                info(rs.getMetaData.extractResultSetMetaData)
              }
              val name = rs.getString("routine_name")
              val sql = rs.getString("routine_definition")
              if (StringUtils.isNoneBlank(sql)) {
                val ddl =
                  s"""\nCREATE OR REPLACE FUNCTION ${name}() RETURNS TRIGGER AS $$BODY$$$sql$$BODY$$ LANGUAGE plpgsql;"""
                writer.write(ddl)
                info(ddl)
              }
              count += 1
            }
        }
    }

    config.viewsLike.foreach {
      case LikeCriteria(schemaLike, viewLike) =>
        val viewSql =
          s"""SELECT
             |    table_name,
             |    view_definition
             |FROM
             |    information_schema.views
             |WHERE
             |    table_schema LIKE '$schemaLike'
             |    AND table_name LIKE '$viewLike';""".stripMargin
        jdbcConnection.queryForObject(viewSql) {
          rs =>
            var count = 0
            while (rs.next()) {
              if (count == 0) {
                info(rs.getMetaData.extractResultSetMetaData)
              }
              val name = rs.getString("table_name")

              val sql = rs.getString("view_definition")
              if (StringUtils.isNoneBlank(sql)) {
                val ddl =
                  s"""\nDROP VIEW IF EXISTS $name;
                     |CREATE VIEW $name AS
                     |$sql
                     |""".
                    stripMargin
                writer.write(
                  ddl)
                info(ddl)
              }
              count += 1
            }
        }
    }

    config.triggersLike.foreach {
      case LikeCriteria(schemaLike, triggerLike) =>
        val triggerActionStatementSql =
          s"""SELECT
             |DISTINCT trigger_name,event_object_table as table_name,action_statement,action_orientation,action_timing
             |FROM
             |    information_schema.triggers
             |WHERE
             |    trigger_schema LIKE '$schemaLike'
             |    AND trigger_name LIKE '$triggerLike'""".stripMargin
        case class TriggerName(name: String, tableName: String, actionOrientation: String, actionTiming: String)
        val actionStatementMap: scala.collection.mutable.Map[TriggerName, String] = new scala.collection.mutable.HashMap[TriggerName, String]()
        jdbcConnection.queryForObject(triggerActionStatementSql) {
          rs =>
            var count = 0
            while (rs.next()) {
              if (count == 0) {
                info(rs.getMetaData.extractResultSetMetaData)
              }
              val name = rs.getString("trigger_name")
              val tableName = rs.getString("table_name")
              val actionTiming = rs.getString("action_timing")
              val actionOrientation = rs.getString("action_orientation")
              val actionStatement = rs.getString("action_statement")
              if (List(name, tableName, actionTiming, actionOrientation, actionStatement).forall(StringUtils.isNotBlank)) {
                actionStatementMap += TriggerName(name, tableName, actionOrientation, actionTiming) -> actionStatement.replaceAll("FUNCTION", "PROCEDURE")
              }
              count += 1
            }
        }

        val triggerDetailSql =
          s"""SELECT trg.tgname AS trigger_name,
             |       tbl.relname AS table_name,
             |       p.proname AS function_name,
             |       CASE trg.tgtype & cast(2 AS int2)
             |         WHEN 0 THEN 'AFTER'
             |         ELSE 'BEFORE'
             |       END AS action_timing,
             |       CASE trg.tgtype & cast(28 AS int2)
             |         WHEN 16 THEN 'UPDATE'
             |         WHEN  8 THEN 'DELETE'
             |         WHEN  4 THEN 'INSERT'
             |         WHEN 20 THEN 'INSERT,UPDATE'
             |         WHEN 28 THEN 'INSERT,UPDATE,DELETE'
             |         WHEN 24 THEN 'UPDATE,DELETE'
             |         WHEN 12 THEN 'INSERT,DELETE'
             |       END AS trigger_event,
             |       CASE trg.tgtype & cast(1 AS int2)
             |         WHEN 0 THEN 'STATEMENT'
             |         ELSE 'ROW'
             |       END AS action_orientation,
             |       trg.*
             |  FROM pg_trigger trg,
             |       pg_class tbl,
             |       pg_proc p
             | WHERE trg.tgrelid = tbl.oid
             |   AND trg.tgfoid = p.oid
             |   AND tbl.relname !~ '^pg_' AND trg.tgname LIKE '$triggerLike'""".stripMargin
        jdbcConnection.queryForObject(triggerDetailSql) {
          rs =>
            var count = 0
            while (rs.next()) {
              if (count == 0) {
                info(rs.getMetaData.extractResultSetMetaData)
              }
              val name = rs.getString("trigger_name")
              val tableName = rs.getString("table_name")
              val actionTiming = rs.getString("action_timing")
              val actionOrientation = rs.getString("action_orientation")
              val functionName = rs.getString("function_name")
              val triggerEvent = rs.getString("trigger_event")
              if (tables.contains(tableName) && List(name, tableName, actionTiming, actionOrientation, functionName, triggerEvent).forall(StringUtils.isNotBlank)) {
                val actionStatement = actionStatementMap(TriggerName(name, tableName, actionOrientation, actionTiming))
                val ddl =
                  s"""\nDROP TRIGGER IF EXISTS $name ON $tableName;
                     |CREATE TRIGGER $name $actionTiming ${triggerEvent.split(',').toList.mkString(" OR ")} ON $tableName
                     |    FOR EACH $actionOrientation $actionStatement;
                     |""".stripMargin
                writer.write(ddl)
                info(
                  ddl)
              }
              count += 1
            }
        }
    }
  }
}
