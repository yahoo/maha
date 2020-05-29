package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import io.druid.metadata.PasswordProvider;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public class MongoStorageConnectorConfig {

    /**
     * host1:port1,host2:port2,host3:port3 example usage: MongoClient mongoClient = new
     * MongoClient(Arrays.asList(new ServerAddress("host1", port1), new ServerAddress("host2", port2),
     * new ServerAddress("host3", port3)));
     */
    @JsonProperty
    private String hosts = "localhost";

    /**
     * Name of db in mongo, e.g. mydb example usage: DB db = mongoClient.getDB( "mydb" );
     */
    @JsonProperty
    private String dbName;

    /**
     * user name used for authentication example usage: MongoCredential credential =
     * MongoCredential.createCredential(userName, database, password); MongoClient mongoClient = new
     * MongoClient(new ServerAddress(), Arrays.asList(credential));
     */
    @JsonProperty
    private String user = null;

    /**
     * password provided by PasswordProvider example usage: MongoCredential credential =
     * MongoCredential.createCredential(userName, database, password); MongoClient mongoClient = new
     * MongoClient(new ServerAddress(), Arrays.asList(credential));
     */
    @JsonProperty("password")
    private PasswordProvider passwordProvider;

    /**
     * Map of client options to set example usage: MongoClient mongoClient = new MongoClient(new
     * ServerAddress(), Arrays.asList(credential), mongoClientOptions);
     */
    @JsonProperty("clientOptions")
    private Properties clientProperties;

    public String getHosts() {
        return hosts;
    }

    public String getDbName() {
        return dbName;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return passwordProvider == null ? null : passwordProvider.getPassword();
    }

    public String getConnectURI() {
        Preconditions.checkState(!Strings.isNullOrEmpty(this.getHosts()), "Hosts is null or empty");
        Preconditions.checkState(!Strings.isNullOrEmpty(this.getDbName()), "Db name is null or empty");
        return "mongodb://" + (Strings.isNullOrEmpty(this.getPassword()) ? this.getHosts()
                : this.getUser() + ":" + this.getPassword() + "@" + this.getHosts()) + "/" + this
                .getDbName();
    }

    public Properties getClientProperties() {
        return clientProperties;
    }

    static final String[] INT_PROPERTIES = new String[]{
            "connectionsPerHost"
            , "connectTimeout"
            , "heartbeatConnectTimeout"
            , "heartbeatFrequency"
            , "heartbeatSocketTimeout"
            , "localThreshold"
            , "maxConnectionIdleTime"
            , "maxConnectionLifeTime"
            , "maxWaitTime"
            , "minConnectionsPerHost"
            , "minHeartbeatFrequency"
            , "serverSelectionTimeout"
            , "socketTimeout"
            , "threadsAllowedToBlockForConnectionMultiplier"
    };

    static final String[] BOOL_PROPERTIES = new String[]{
            "alwaysUseMBeans"
            , "cursorFinalizerEnabled"
            , "socketKeepAlive"
            , "sslEnabled"
            , "sslInvalidHostNameAllowed"
    };

    @VisibleForTesting
    MongoClientOptions getMongoClientOptions() {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();

        if (getClientProperties() != null) {
            Class<MongoClientOptions.Builder> clazz = MongoClientOptions.Builder.class;
            Properties props = getClientProperties();
            for (String p : INT_PROPERTIES) {
                String value = props.getProperty(p);
                if (value != null) {
                    try {
                        int i = Integer.parseInt(value);
                        Method m = clazz.getMethod(p, int.class);
                        m.invoke(builder, i);
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format("Failed to parse int for property : %s=%s", p, value), e);
                    }
                }
            }
            for (String p : BOOL_PROPERTIES) {
                String value = props.getProperty(p);
                if (value != null) {
                    try {
                        boolean b = Boolean.parseBoolean(value);
                        Method m = clazz.getMethod(p, boolean.class);
                        m.invoke(builder, b);
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format("Failed to parse boolean for property : %s=%s", p, value), e);
                    }
                }
            }
        }

        return builder.build();
    }

    @Override
    public String toString() {
        return "MongoStorageConnectorConfig{" +
                "hosts='" + hosts + '\'' +
                ", dbName='" + dbName + '\'' +
                ", user='" + user + '\'' +
                ", passwordProvider=" + passwordProvider +
                ", clientProperties=" + clientProperties +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoStorageConnectorConfig that = (MongoStorageConnectorConfig) o;
        return Objects.equals(getHosts(), that.getHosts()) &&
                Objects.equals(getDbName(), that.getDbName()) &&
                Objects.equals(getUser(), that.getUser()) &&
                Objects.equals(passwordProvider, that.passwordProvider) &&
                Objects.equals(getClientProperties(), that.getClientProperties());
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(getHosts(), getDbName(), getUser(), passwordProvider, getClientProperties());
    }

    public List<ServerAddress> getServerAddressList() {
        return Arrays
                .stream(this.getHosts().split(",")).map(s -> {
                    String[] hostPort = s.split(":");
                    if (hostPort.length > 1) {
                        return new ServerAddress(hostPort[0], Integer.parseInt(hostPort[1]));
                    } else {
                        return new ServerAddress(hostPort[0]);
                    }
                }).collect(Collectors.toList());
    }

    public MongoClient getMongoClient() {
        MongoStorageConnectorConfig config = this;
        List<ServerAddress> serverAddressList = getServerAddressList();

        List<MongoCredential> mongoCredentialList = Collections.emptyList();
        if (getUser() != null && getPassword() != null) {
            mongoCredentialList = Lists.newArrayList(MongoCredential
                    .createCredential(config.getUser(), config.getDbName(), config.getPassword().toCharArray()));

        }
        MongoClientOptions options = config.getMongoClientOptions();
        return new MongoClient(serverAddressList, mongoCredentialList, options);
    }
}