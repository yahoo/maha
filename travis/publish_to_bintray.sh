#!/usr/bin/env bash

set -ev

mvn deploy -Dmaven.test.skip=true -DskipTests -Dscoverage.skip --projects com.yahoo.maha:maha-parent --settings travis/settings-maha-parent.xml
mvn deploy -Dmaven.test.skip=true -DskipTests -Dscoverage.skip --projects com.yahoo.maha:maha-par-request --settings travis/settings-maha-par-request.xml
mvn deploy -Dmaven.test.skip=true -DskipTests -Dscoverage.skip --projects com.yahoo.maha:maha-druid-lookups --settings travis/settings-maha-druid-lookups.xml
mvn deploy -Dscoverage.skip --projects com.yahoo.maha:maha-core --settings travis/settings-maha-core.xml
mvn deploy -Dmaven.test.skip=true -DskipTests -Dscoverage.skip --projects com.yahoo.maha:maha-druid-executor --settings travis/settings-maha-druid-executor.xml
mvn deploy -Dmaven.test.skip=true -DskipTests -Dscoverage.skip --projects com.yahoo.maha:maha-oracle-executor --settings travis/settings-maha-oracle-executor.xml
mvn deploy -Dmaven.test.skip=true -DskipTests -Dscoverage.skip --projects com.yahoo.maha:maha-service --settings travis/settings-maha-service.xml
mvn deploy -Dmaven.test.skip=true -DskipTests -Dscoverage.skip --projects com.yahoo.maha:maha-api-jersey --settings travis/settings-maha-api-jersey.xml
mvn deploy -Dmaven.test.skip=true -DskipTests -Dscoverage.skip --projects com.yahoo.maha:maha-api-example --settings travis/settings-maha-api-example.xml
mvn deploy -Dmaven.test.skip=true -DskipTests -Dscoverage.skip --projects com.yahoo.maha:maha-presto-executor --settings travis/settings-maha-presto-executor.xml
mvn deploy -Dmaven.test.skip=true -DskipTests -Dscoverage.skip --projects com.yahoo.maha:maha-par-request-2 --settings travis/settings-maha-par-request-2.xml

#mvn release:update-versions -DautoVersionSubmodules=true -Djava.awt.headless=true
#git commit -m "preparing for next release" -a
#git push https://github.com/yahoo/maha.git master
