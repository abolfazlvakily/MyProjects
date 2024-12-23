FROM bde2020/spark-master:3.2.0-hadoop3.2
COPY app /home/app
CMD ["/spark/bin/spark-submit", "/home/app/main.py", "--master", "spark://spark-master:7077"]