FROM jetty:12-jdk21

COPY --chown=jetty:jetty target/maha-api-example-*.war /var/lib/jetty/webapps/ROOT.war
COPY jetty-logging.properties /var/lib/jetty/resources/
RUN java -jar /usr/local/jetty/start.jar --add-modules=server,http,ee10-deploy
