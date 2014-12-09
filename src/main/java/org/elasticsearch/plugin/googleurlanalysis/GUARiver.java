package org.elasticsearch.plugin.googleurlanalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.threadpool.ThreadPool;
import org.json.JSONArray;
import org.json.JSONObject;

public class GUARiver extends AbstractRiverComponent implements River {
	private final Client client;
	private ThreadPool threadPool;
	private int bulkSize;
	private TimeValue bulkFlushInterval;
	private int maxConcurrentBulk;
	private BulkProcessor bulkProcessor;
	
	private Thread worker;
	
	private String sourceIndexName;
	private String sourceTypeName;
	private String urlFieldName;
	
	private String destIndexName;
	private String destTypeName;
	private String filePath;
	private File sourceFile;
	private String sourceType;
	
	@Inject
	public GUARiver(RiverName riverName, RiverSettings riverSettings,
			Client client, ThreadPool threadPool, Settings settings) {
		super(riverName, riverSettings);
		this.threadPool = threadPool;
		this.client = client;

		if (riverSettings.settings().containsKey("mUrlGoogleAnalyzer")) {
			Map<String, Object> mSettings = (Map<String, Object>) riverSettings
					.settings().get("mUrlGoogleAnalyzer");

			if (mSettings.containsKey("destIndex")) {
				Map<String, Object> destIndex = (Map<String, Object>) mSettings.get("destIndex");
				destIndexName = XContentMapValues.nodeStringValue(
						destIndex.get("index"), riverName.name());
				destTypeName = XContentMapValues.nodeStringValue(
						destIndex.get("type"), "doc");
				bulkSize = XContentMapValues.nodeIntegerValue(
						destIndex.get("bulk_size"), 100);
				bulkFlushInterval = TimeValue.parseTimeValue(
						XContentMapValues.nodeStringValue(
								destIndex.get("flush_interval"), "5s"),
						TimeValue.timeValueSeconds(5));
				maxConcurrentBulk = XContentMapValues.nodeIntegerValue(
						destIndex.get("max_concurrent_bulk"), 1);
			} else {
				destIndexName = riverName.name();
				destTypeName  = "doc";
				bulkSize      = 100;
				bulkFlushInterval = TimeValue.parseTimeValue("5s",TimeValue.timeValueSeconds(5));
				maxConcurrentBulk = 1;
			}
			
			//retrieving the source of shortened urls
			if(mSettings.containsKey("source")) {
				Map<String, Object> sourceSettings = (Map<String, Object>) mSettings.get("source");
				String inputType = XContentMapValues.nodeStringValue(sourceSettings.get("type"), "file");
				if(inputType.equals("index")) {
					sourceType = "index";
					sourceIndexName = XContentMapValues.nodeStringValue(sourceSettings.get("sourceIndexName"), "");
					sourceTypeName  = XContentMapValues.nodeStringValue(sourceSettings.get("sourceIndexType"), "");
					urlFieldName = XContentMapValues.nodeStringValue(sourceSettings.get("urlFieldName"), "");
					
					if(sourceIndexName.equals("") || sourceTypeName.equals("") || urlFieldName.equals("")) {
						logger.error("source index configuration is incomplete !!");
						return;
					}
					
				} else if(inputType.equals("file")) {
					sourceType = "file";
					filePath = XContentMapValues.nodeStringValue(sourceSettings.get("filePath"), "");
					sourceFile = new File(filePath);
					if(!sourceFile.isFile()) {
						logger.error("invalid path name !!");
						return;
					}
				} else {
					logger.error("unknown source configuration !!");
				}
				
			} else {
				logger.error("Creating plugin instance with incomplete configuration, source configuration missing !!");
				return;
			}

		} else {
			logger.error("Creating plugin instance with incomplete configuration !!");
			return;
		}

		// Creating bulk processor
		bulkProcessor = BulkProcessor
				.builder(client, new BulkProcessor.Listener() {
					public void beforeBulk(long executionId, BulkRequest request) {
						logger.debug(
								"Going to execute new bulk composed of {} actions",
								request.numberOfActions());
					}

					public void afterBulk(long executionId,
							BulkRequest request, BulkResponse response) {
						logger.debug("Executed bulk composed of {} actions",
								request.numberOfActions());
						if (response.hasFailures()) {
							logger.warn(
									"There was failures while executing bulk",
									response.buildFailureMessage());
							if (logger.isDebugEnabled()) {
								for (BulkItemResponse item : response
										.getItems()) {
									if (item.isFailed()) {
										logger.debug(
												"Error for {}/{}/{} for {} operation: {}",
												item.getIndex(),
												item.getType(), item.getId(),
												item.getOpType(),
												item.getFailureMessage());
									}
								}
							}
						}
					}

					public void afterBulk(long executionId,
							BulkRequest request, Throwable failure) {
						logger.warn("Error executing bulk", failure);
					}
				}).setBulkActions(bulkSize)
				.setConcurrentRequests(maxConcurrentBulk)
				.setFlushInterval(bulkFlushInterval).build();
	}

	public void start() {
		try {
			client.admin().indices().prepareCreate(destIndexName).execute()
					.actionGet();
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				logger.info("the index is already exists ,  no problem");
			} else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {

			} else {
				logger.warn("failed to create index [{}], disabling river...",
						e, destIndexName);
				return;
			}
		}

		try {
			String mapping = XContentFactory.jsonBuilder().startObject()
					.startObject(destTypeName).startObject("_routing")
					.field("required", true).field("path", "id").endObject()
					.startObject("properties").startObject("id")
					.field("type", "string").field("index", "not_analyzed")
					.endObject().startObject("status").field("type", "string")
					.endObject().startObject("created").field("type", "date")
					.endObject().startObject("clicks_time")
					.field("type", "date").endObject()
					.startObject("all_clicks").field("type", "integer")
					.endObject().startObject("countries")
					.startObject("properties").startObject("count")
					.field("type", "string").field("index", "not_analyzed")
					.endObject().startObject("id").field("type", "string")
					.field("index", "not_analyzed").endObject().endObject()
					.endObject().startObject("referrers")
					.startObject("properties").startObject("count")
					.field("type", "string").field("index", "not_analyzed")
					.endObject().startObject("id").field("type", "string")
					.field("index", "not_analyzed").endObject().endObject()
					.endObject().startObject("browsers")
					.startObject("properties").startObject("count")
					.field("type", "string").field("index", "not_analyzed")
					.endObject().startObject("id").field("type", "string")
					.field("index", "not_analyzed").endObject().endObject()
					.endObject().startObject("platforms")
					.startObject("properties").startObject("count")
					.field("type", "string").field("index", "not_analyzed")
					.endObject().startObject("id").field("type", "string")
					.field("index", "not_analyzed").endObject().endObject()
					.endObject().endObject().endObject().string();

			logger.debug("Applying default mapping for [{}]/[{}]: {}",
					destIndexName, destTypeName, mapping);
			client.admin().indices().preparePutMapping(destIndexName)
					.setType(destTypeName).setSource(mapping).execute().actionGet();
		} catch (Exception e) {
			logger.debug(
					"failed to apply default mapping [{}]/[{}], disabling river...",
					e, destIndexName, destTypeName);
		}

		startWorker();
	}

	private void startWorker() {
		if(sourceType.equals("file")) {
			startWorkerOnFile();
		} else { // Thread that runs on file
			startWorkerOnIndex();
		}

		
	}
	
	private void startWorkerOnIndex() {
		worker = new Thread(new Runnable() {
			public void run() {
			    while(true) {
					try {
						logger.info("scanning index : " + sourceIndexName + ", type: " + sourceTypeName);
						SearchResponse scrollResp = client.prepareSearch(sourceIndexName)
						        .setSearchType(SearchType.SCAN)
						        .setScroll(new TimeValue(60000))
						        .setSize(100).execute().actionGet();
						while (true) {
							scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
					                .setScroll(new TimeValue(60000)).execute().actionGet();
							 if (scrollResp.getHits().getHits().length == 0) {
						            logger.info("Closing the bulk processor");
						            break;
						    }
							 
						    for (SearchHit hit : scrollResp.getHits()) {
						    	String url = (String) hit.getSource().get(urlFieldName);
						    	if(url == null) {
						    		logger.info("field can't be found, skip document!!");
						    		continue;
						    	}
						    	//retrieving url analysis !!
						    	if(!url.contains("http")) {
						    		url = "http://" + url;
						    	}
						    	logger.info("Retrieving analysis of :" + url);
						    	JSONObject obj = retrieveAnalysis(url);
						    	
								// indexing the result into our sweet index
								XContentBuilder builder = createDocumentFromJson(obj);
								if(builder != null) {
									bulkProcessor.add(Requests.indexRequest(destIndexName).type(destTypeName).source(builder));
			                        logger.info("new document has been indexed!!");
								}
						    }
						}
					} catch(Exception ex) {
						ex.printStackTrace();
						logger.error("Exception in dealing with indexes");
					}
					
					try {
						Thread.sleep(7200000);
					} catch(Exception ex) {
						logger.info("exception in sleeping or retrieving");
					}
				}
			}
		});
		
		worker.start();
	}
	
	private void startWorkerOnFile() {
		worker = new Thread(new Runnable() {
			public void run() {
			    while(true) {
					try {
						logger.info("scanning file : " + filePath);
						BufferedReader br = new BufferedReader(new FileReader(sourceFile));
						String url;
						while ((url = br.readLine()) != null) {
							//retrieving url analysis !!
					    	if(!url.contains("http")) {
					    		url = "http://" + url;
					    	}
					    	logger.info("Retrieving analysis of: " + url);
					    	JSONObject obj = retrieveAnalysis(url.trim());
					    	
							// indexing the result into our sweet index
							XContentBuilder builder = createDocumentFromJson(obj);
							if(builder != null) {
								bulkProcessor.add(Requests.indexRequest(destIndexName).type(destTypeName).source(builder));
		                        logger.info("new document has been indexed!!");
							}
						}
						br.close();
					} catch(Exception ex) {
						ex.printStackTrace();
						logger.error("Exception in dealing with indexes");
					}
					
					try {
						Thread.sleep(7200000);
					} catch(Exception ex) {
						logger.info("exception in sleeping or retrieving");
					}
				}
			}
		});
		
		worker.start();

	}
	
	private XContentBuilder createDocumentFromJson(JSONObject obj) throws Exception {
		String id = obj.getString("id");
		String status = obj.getString("status");
		String creationDate = obj.getString("created");
		Integer allClickes = obj.getJSONObject("analytics").getJSONObject("twoHours").getInt("shortUrlClicks");
		if(allClickes == 0) {// No clicks in the past two hours , lets sleep an check after another two hours
			logger.info("No clicks found, lets sleep for the next two hours");
			return null;
		}
		
		JSONArray countries = new JSONArray();
		if(obj.getJSONObject("analytics").getJSONObject("twoHours").has("countries")) {
			countries = obj.getJSONObject("analytics").getJSONObject("twoHours").getJSONArray("countries");
		}
		
		JSONArray referrers = new JSONArray();
		if(obj.getJSONObject("analytics").getJSONObject("twoHours").has("referrers")) {
			referrers = obj.getJSONObject("analytics").getJSONObject("twoHours").getJSONArray("referrers");
		}
		
		JSONArray browsers = new JSONArray();
		if(obj.getJSONObject("analytics").getJSONObject("twoHours").has("browsers")) {
			browsers = obj.getJSONObject("analytics").getJSONObject("twoHours").getJSONArray("browsers");
		}
		
		JSONArray platforms = new JSONArray();
		if(obj.getJSONObject("analytics").getJSONObject("twoHours").has("platforms")) {
			platforms = obj.getJSONObject("analytics").getJSONObject("twoHours").getJSONArray("platforms");
		}
		
		DateTime timeNow = DateTime.now();
		XContentBuilder builder = XContentFactory.jsonBuilder();
		
        builder.startObject();
        builder.field("id", id);
        builder.field("status", status);
        builder.field("created", creationDate);
        builder.field("clicks_time", timeNow);
        builder.field("all_clicks", allClickes);
        builder.startArray("countries");
        for (int i = 0; i < countries.length(); i++) {
			JSONObject country = countries.getJSONObject(i);
			builder.startObject();
			builder.field("count", country.getInt("count"));
			builder.field("id", country.getString("id"));
			builder.endObject();
		}
        builder.endArray();
        
        builder.startArray("platforms");
        for (int i = 0; i < platforms.length(); i++) {
			JSONObject platform = platforms.getJSONObject(i);
			builder.startObject();
			builder.field("count", platform.getInt("count"));
			builder.field("id", platform.getString("id"));
			builder.endObject();
		}
        builder.endArray();
        
        builder.startArray("referrers");
        for (int i = 0; i < referrers.length(); i++) {
			JSONObject referrer = referrers.getJSONObject(i);
			builder.startObject();
			builder.field("count", referrer.getInt("count"));
			builder.field("id", referrer.getString("id"));
			builder.endObject();
		}
        builder.endArray();
        
        builder.startArray("browsers");
        for (int i = 0; i < browsers.length(); i++) {
			JSONObject browser = browsers.getJSONObject(i);
			builder.startObject();
			builder.field("count", browser.getInt("count"));
			builder.field("id", browser.getString("id"));
			builder.endObject();
		}
        builder.endArray();
		builder.endObject();
		return builder;
	}
	
	public JSONObject retrieveAnalysis(String shortend) {
		try {
			JSONObject obj = null;
			URL url = new URL(
					"https://www.googleapis.com/urlshortener/v1/url?projection=FULL&shortUrl="
							+ shortend);
			HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					con.getInputStream()));
			String input;
			String response = "";
			while ((input = br.readLine()) != null) {
				response += input;
			}
			br.close();
			obj = new JSONObject(response);
			return obj;
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	public void close() {
		bulkProcessor.close();
	}

}
