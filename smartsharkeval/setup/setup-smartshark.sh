#!/bin/bash -l

large_mem_path=${1}
echo attempt to setup smartshark_2_2_small in $large_mem_path
cd $large_mem_path || exit 101
# download the following archive for the small version without code clones and code metrics, which only requires about 90 GB of free disk space
wget -O smartshark_2_2_small.agz https://data.goettingen-research-online.de/download/smartshark_2_2_small.agz
cd smartshark_2_2_small || exit 102
echo set-up... $PWD
###### for linux
#wget -qO - https://www.mongodb.org/static/pgp/server-4.0.asc | sudo apt-key add -
#echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.0.list
#sudo apt-get update
#sudo apt-get install -y mongodb-org
#sudo systemctl daemon-reload
#sudo systemctl start mongod
###### for MacOS
# install mongodb 5
brew install mongodb-community@5.0
# run it as a service
brew services start mongodb-community@5.0
# unpack smartshark
mongorestore --gzip --archive=smartshark_2_2_small.agz

