FROM redis:5.0.1-alpine

COPY ./sentinel.conf sentinel.conf
COPY ./start.sh start.sh

CMD ["sh", "start.sh"]
