FROM redis:alpine

COPY ./sentinel.conf sentinel1.conf
COPY ./sentinel.conf sentinel2.conf
COPY ./sentinel.conf sentinel3.conf
COPY ./start.sh start.sh

CMD ["sh", "start.sh"]
