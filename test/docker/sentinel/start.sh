redis-server --port 6381 --daemonize yes
redis-server --port 6382 --daemonize yes --slaveof localhost 6381

redis-sentinel ./sentinel.conf --port 26379 --daemonize yes
redis-sentinel ./sentinel.conf --port 26380 --daemonize yes
redis-sentinel ./sentinel.conf --port 26381 --daemonize yes

tail -f /dev/null
