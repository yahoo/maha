#!/bin/bash
set -ev
wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
sudo apt update
sudo apt-get update
sudo apt-get install rpm
