#!/usr/bin/env bash

set -ev

mvn release:update-versions -DautoVersionSubmodules=true -Djava.awt.headless=true