#!/bin/sh

redis-server --port 6381 --daemonize yes --logfile ./out.log
redis-server --port 6382 --daemonize yes --slaveof localhost 6381 --logfile ./out.log

redis-sentinel ./sentinel1.conf --port 26379 --daemonize yes --logfile ./out.log
redis-sentinel ./sentinel2.conf --port 26380 --daemonize yes --logfile ./out.log
redis-sentinel ./sentinel3.conf --port 26381 --daemonize yes --logfile ./out.log

tail -n 1000000000 -f ./out.log
