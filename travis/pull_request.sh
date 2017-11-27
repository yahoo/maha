#!/usr/bin/env bash

set -ev

git checkout ${TRAVIS_BRANCH}

echo "PR ${TRAVIS_PULL_REQUEST} and TAG '${TRAVIS_TAG}' Branch ${TRAVIS_BRANCH}"