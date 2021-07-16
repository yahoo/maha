#!/usr/bin/env bash

set -ev

export GPG_TTY=$(tty)

mkdir -p screwdriver/deploy
chmod 0700 screwdriver/deploy

openssl aes-256-cbc -pass pass:$PASS_PHRASE -in screwdriver/sec.key.enc -out screwdriver/deploy/sec.key -pbkdf2 -d
gpg --batch --import screwdriver/deploy/sec.key

ls -l screwdriver/deploy

# Print deploy failure step, exit with failure.
error_exit()
{
	echo "Error, step failed to deploy: $1"
	exit 0
}


mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-parent --settings screwdriver/maven-settings.xml || error_exit "maha-parent"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-db --settings screwdriver/maven-settings.xml || error_exit "maha-db"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-druid-lookups --settings screwdriver/maven-settings.xml || error_exit "maha-druid-lookups"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-par-request-2 --settings screwdriver/maven-settings.xml || error_exit "maha-par-request-2"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dscoverage.skip=true -Darguments="-Dscoverage.skip=true" --projects com.yahoo.maha:maha-core --settings screwdriver/maven-settings.xml || error_exit "maha-core"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-bigquery-executor --settings screwdriver/maven-settings.xml || error_exit "maha-bigquery-executor"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-request-log --settings screwdriver/maven-settings.xml || error_exit "maha-request-log"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-job-service --settings screwdriver/maven-settings.xml || error_exit "maha-maha-job-service"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-druid-executor --settings screwdriver/maven-settings.xml || error_exit "maha-druid-executor"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-oracle-executor --settings screwdriver/maven-settings.xml || error_exit "maha-oracle-executor"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-presto-executor --settings screwdriver/maven-settings.xml || error_exit "maha-presto-executor"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-postgres-executor --settings screwdriver/maven-settings.xml || error_exit "maha-postgres-executor"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-service --settings screwdriver/maven-settings.xml || error_exit "maha-service"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-worker --settings screwdriver/maven-settings.xml || error_exit "maha-worker"
mvn deploy -Dgpg.passphrase=${PASS_PHRASE} -Dgpg.skip=false -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true" --projects com.yahoo.maha:maha-api-jersey --settings screwdriver/maven-settings.xml || error_exit "maha-api-jersey"

rm -rf screwdriver/deploy
