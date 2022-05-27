FROM jetty:9.4.18-jre8-alpine

ADD target/maha-api-example-*.war /var/lib/jetty/webapps/mahademo.war
EXPOSE 8080
ENTRYPOINT java -jar $JETTY_HOME/start.jar
