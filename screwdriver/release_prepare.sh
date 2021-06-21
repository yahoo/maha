#!/usr/bin/env bash

set -ev

mvn -q -B versions:set -DremoveSnapshot -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true"
git config --global user.name "Maha Headless"
git config --global user.email maha@yahoo-inc.com
# stays only in local
git commit -m "Preparing for release" -a
