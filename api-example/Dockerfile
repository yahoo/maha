FROM jetty:12-jdk21

ADD target/maha-api-example-*.war /var/lib/jetty/webapps/ROOT.war
COPY jetty-logging.properties /var/lib/jetty/resources/
EXPOSE 8080
ENTRYPOINT java -jar $JETTY_HOME/start.jar
