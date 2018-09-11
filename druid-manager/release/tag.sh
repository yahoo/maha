#!/usr/local/bin/bash -x

VERSION=`git tag| sort -V| tail -1| awk -F"." {'print $1 "." $2 "." $3+1'}`
git tag -a "$VERSION" -m "Tag $VERSION"
git push --tags origin