package org.elasticsearch.plugin.googleurlanalysis;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

public class GUAPlugin extends AbstractPlugin {

	public GUAPlugin() {
	}

	public String name() {
		return "river-google-url-analytic";
	}

	public String description() {
		return "GUA (Google Url Analytics) a plugin that retrieves the clickes analytics of shortend urls and put it into a specific index";
	}

	public void onModule(RiversModule module) {
		module.registerRiver("Shortner", GUARestModule.class);
	}

}
