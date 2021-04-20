#!/bin/bash
set -ev
wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 6B05F25D762E3157
sudo apt update
sudo apt-get update
sudo apt-get install rpm
