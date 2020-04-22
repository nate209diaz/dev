FROM tomcat

RUN apt-get update && apt-get install nano

VOLUME /usr/local/tomcat/webapps/test

WORKDIR /usr/local/tomcat/webapps/test

COPY ./ /usr/local/tomcat/webapps/test/

EXPOSE 8080
