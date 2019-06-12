package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testng.annotations.Test;

import java.util.UUID;

public class JdbcH2QueryTest {

    private HikariConfig config;
    private String h2dbId = UUID.randomUUID().toString().replace("-", "");
    private HikariDataSource ds;

    public void initJdbcToH2() {
        config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:" + h2dbId + ";MODE=Oracle;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("h2.test.database.password");
        config.setMaximumPoolSize(2);
        ds = new HikariDataSource(config);
        assert (ds.isRunning());
    }

    @Test
    public void testConnectJdbcToH2Lookup() {

    }
}
