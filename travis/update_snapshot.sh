#!/usr/bin/env bash

set -ev

mvn release:update-versions -DautoVersionSubmodules=true -Djava.awt.headless=true

git add pom.xml core/pom.xml api-jersey/pom.xml druid/pom.xml par-request-2/pom.xml presto/pom.xml api-example/pom.xml druid-lookups/pom.xml par-request/pom.xml service/pom.xml oracle/pom.xml db/pom.xml job-service/pom.xml worker/pom.xml request-log/pom.xml postgres/pom.xml bigquery/pom.xml

git commit -m "releasing next revision"
