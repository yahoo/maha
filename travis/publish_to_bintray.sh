#!/usr/bin/env bash

set -ev

mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-parent --settings travis/settings-maha-parent.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-druid-lookups --settings travis/settings-maha-druid-lookups.xml
mvn deploy -Dscoverage.skip=true -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-core --settings travis/settings-maha-core.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-druid-executor --settings travis/settings-maha-druid-executor.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-oracle-executor --settings travis/settings-maha-oracle-executor.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-service --settings travis/settings-maha-service.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-api-jersey --settings travis/settings-maha-api-jersey.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-presto-executor --settings travis/settings-maha-presto-executor.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-par-request-2 --settings travis/settings-maha-par-request-2.xml

mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-db --settings travis/settings-maha-db.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-job-service --settings travis/settings-maha-job-service.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-worker --settings travis/settings-maha-worker.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-request-log --settings travis/settings-maha-request-log.xml

mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-postgres-executor --settings travis/settings-maha-postgres-executor.xml
