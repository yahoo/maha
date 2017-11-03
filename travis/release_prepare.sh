#!/usr/bin/env bash

set -ev

git checkout master
git pull https://github.com/yahoo/maha.git master
mvn versions:set -DremoveSnapshot
git commit -m "Preparing for release" -a
#mvn -B release:clean release:prepare -Djava.awt.headless=true -Dmaven.test.skip=true -Dscoverage.skip=true -DskipTests -Darguments="-DskipTests -Dscoverage.skip=true"
