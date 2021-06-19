#!/usr/bin/env bash

set -ev
#openssl aes-256-cbc -K $encrypted_f094dd62560a_key -iv $encrypted_f094dd62560a_iv -in travis/pubring.gpg.enc -out travis/pubring.gpg -d
#gpg --fast-import travis/pubring.gpg

export GPG_TTY=$(tty)

mkdir -p screwdriver/deploy
chmod 0700 screwdriver/deploy

openssl aes-256-cbc -pass pass:$GPG_ENCPHRASE -in screwdriver/pubring.gpg.enc -out screwdriver/deploy/pubring.gpg -d
openssl aes-256-cbc -pass pass:$GPG_ENCPHRASE -in screwdriver/secring.gpg.enc -out screwdriver/deploy/secring.gpg -d

#mvn -B deploy -P ossrh -Dmaven.test.skip=true --settings screwdriver/settings/settings-publish.xml
ls -l screwdriver/deploy
export PASS=MahaReporting@_1

mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-parent --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-db --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-druid-lookups --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-par-request-2 --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dscoverage.skip=true -Darguments="-Dscoverage.skip=true" --projects com.yahoo.maha:maha-core --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-bigquery-executor --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-request-log --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-job-service --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-druid-executor --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-oracle-executor --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-presto-executor --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-postgres-executor --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-service --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-worker --settings screwdriver/maven-settings.xml
mvn deploy -Dgpg.passphrase=${PASS} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-api-jersey --settings screwdriver/maven-settings.xml


rm -rf screwdriver/deploy
