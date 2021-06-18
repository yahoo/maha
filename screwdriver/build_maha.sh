#!/bin/bash
set -ev
git clone https://github.com/yahoo/maha.git
cd maha
git checkout ${PR_BASE_BRANCH_NAME}
mvn clean install