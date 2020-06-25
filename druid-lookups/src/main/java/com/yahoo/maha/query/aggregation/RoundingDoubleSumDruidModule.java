package com.yahoo.maha.query.aggregation;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class RoundingDoubleSumDruidModule implements DruidModule {

    @Override
    public List<? extends Module> getJacksonModules() {
        return ImmutableList.of(
                new SimpleModule("RoundingDoubleSumModule").registerSubtypes(
                        new NamedType(RoundingDoubleSumAggregatorFactory.class, "roundingDoubleSum")
                )
        );
    }

    @Override
    public void configure(Binder binder) {
    }
}
