package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

public interface PasswordProvider {

    default String getPassword(final String passwordKey) {
        return passwordKey;
    }
}
