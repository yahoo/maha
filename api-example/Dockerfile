FROM jetty:9.4.18-jre8-alpine

ADD target/maha-api-example-*.war /var/lib/jetty/webapps/root.war
ENTRYPOINT java -jar $JETTY_HOME/start.jar
