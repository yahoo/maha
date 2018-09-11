#!/bin/bash
set -ev
cd $TRAVIS_BUILD_DIR/druid-manager
sbt ++$TRAVIS_SCALA_VERSION package
cd $TRAVIS_BUILD_DIR
if [ "${TRAVIS_PULL_REQUEST}" != "false" ]; then bash ./travis/pull_request.sh; else echo "it is master build"; fi
if [ "${TRAVIS_PULL_REQUEST}" = "false" ] && [ "${TRAVIS_BRANCH}" = "master" ]; then bash ./travis/release_prepare.sh; else echo "skipping release"; fi
echo "PR= ${TRAVIS_PULL_REQUEST} , and TAG= ${TRAVIS_TAG} , Branch= ${TRAVIS_BRANCH} , Event = ${TRAVIS_EVENT_TYPE}"
mvn -B -q install
