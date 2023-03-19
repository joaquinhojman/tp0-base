FROM ubuntu:latest
RUN apt-get update && apt-get install -y netcat
COPY ./server_test/test_server.sh ./test_server.sh
RUN chmod a+x ./test_server.sh
CMD ./test_server.sh
