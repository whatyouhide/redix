#!/bin/sh

redis-server --port 6383 --daemonize yes --logfile ./out.log --requirepass main-password

redis-sentinel ./sentinel.conf --port 26383 --daemonize yes --logfile ./out.log

tail -n 1000000000 -f ./out.log
