FROM tomcat

RUN apt-get update && apt-get install -y nano

VOLUME /usr/local/tomcat/webapps/test

WORKDIR /usr/local/tomcat/webapps/test

COPY ./docker/ /usr/local/tomcat/webapps/test/

EXPOSE 8080
