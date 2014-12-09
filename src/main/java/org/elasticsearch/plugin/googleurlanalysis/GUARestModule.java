package org.elasticsearch.plugin.googleurlanalysis;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class GUARestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(GUARiver.class).asEagerSingleton();
    }
}
