#!/bin/bash
set -ev

cd $TRAVIS_BUILD_DIR/druid-manager

if [ "${TRAVIS_PULL_REQUEST}" != "false" ] || [ "${TRAVIS_BRANCH}" != "master" ]; then
    sbt ++$TRAVIS_SCALA_VERSION package;
else 
    VERSION="1.1.6";
    echo "VERSION = ${VERSION}";
    sbt ++$TRAVIS_SCALA_VERSION -Dversion=$VERSION rpm:packageBin > sbt_package.out;
    curl -T $TRAVIS_BUILD_DIR/druid-manager/target/rpm/RPMS/noarch/druid-manager-$VERSION-1.noarch.rpm -u$BINTRAY_USER:$BINTRAY_API_KEY https://api.bintray.com/content/yahoo/rpm/druid-manager/$VERSION/druid-manager-$VERSION-1.noarch.rpm;
    curl -X POST -u$BINTRAY_USER:$BINTRAY_API_KEY https://api.bintray.com/content/yahoo/rpm/druid-manager/$VERSION/publish;
fi

cd $TRAVIS_BUILD_DIR

if [ "${TRAVIS_PULL_REQUEST}" != "false" ]; then bash ./travis/pull_request.sh; else echo "it is master build"; fi
if [ "${TRAVIS_PULL_REQUEST}" = "false" ] && [ "${TRAVIS_BRANCH}" = "master" ]; then bash ./travis/release_prepare.sh; else echo "skipping release"; fi
echo "PR= ${TRAVIS_PULL_REQUEST} , and TAG= ${TRAVIS_TAG} , Branch= ${TRAVIS_BRANCH} , Event = ${TRAVIS_EVENT_TYPE}"
mvn -B -q install

