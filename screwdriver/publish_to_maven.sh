#!/usr/bin/env bash

set -ev
#openssl aes-256-cbc -K $encrypted_f094dd62560a_key -iv $encrypted_f094dd62560a_iv -in travis/pubring.gpg.enc -out travis/pubring.gpg -d
#gpg --fast-import travis/pubring.gpg

mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-parent --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-db --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-druid-lookups --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-par-request-2 --settings travis/maven-settings.xml
mvn deploy -Dscoverage.skip=true -Darguments="-Dscoverage.skip=true" --projects com.yahoo.maha:maha-core --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-bigquery-executor --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-request-log --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-job-service --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-druid-executor --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-oracle-executor --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-presto-executor --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-postgres-executor --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-service --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-worker --settings travis/maven-settings.xml
mvn deploy -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-api-jersey --settings travis/maven-settings.xml


