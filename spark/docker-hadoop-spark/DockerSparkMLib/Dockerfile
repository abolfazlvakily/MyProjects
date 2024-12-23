FROM bde2020/spark-master:3.2.0-hadoop3.2

RUN apk add g++ python3-dev
RUN python3 -m pip install numpy pandas

CMD ["tail", "-f", "/dev/null"]