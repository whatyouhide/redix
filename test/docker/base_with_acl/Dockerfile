FROM redis:6.0.0-alpine

COPY ./acl.conf acl.conf
COPY ./redis.conf redis.conf

CMD ["redis-server", "./redis.conf"]
