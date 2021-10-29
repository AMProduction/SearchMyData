#!/bin/bash
echo 'Building SearchMyData image...'
docker build -t searchmydata-v.1.7 .
echo 'Done'
echo 'Fetching MongoDB image...'
docker pull mongo
echo 'Done'
echo 'Creating a user-defined bridge network...'
docker network create searchmydata-net
echo 'Done'
echo 'Run MongoDB container...'
docker run --net searchmydata-net --name database -d mongo
echo 'Done'
echo 'Run The App container...'
sleep 1
docker run --net searchmydata-net --name client -it searchmydata-v.1.7