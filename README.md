GUA-Elastic-Plugin
==================
GUA (Google Url Analytics) elastic plugin that retrieves clicks analytics of shortened urls and puts the analytics into a specific index.

- Your shortened urls can be either in a file (url per line) or in an index under a specific field.  
- The plugin pulls the clicks from google api every 2 hours and saves them in an index you specify.


Installation :
----------
1- Build the project using maven:
Under GUA-Elastic-Plugin folder run the command:

    mvn package
To get the gua-plugin-1.0-SNAPSHOT.zip under releases folder 

2- Install the plugin into elastic using this command:

     ./bin/plugin -url file:/path_to_gua-plugin-1.0-SNAPSHOT.zip --install GUAPlugin

Usage:
----------
Creating the gua river can be done in two ways:

1-Creating a river from a file source (where your shortened urls are in a file):

    PUT _river/my_gua_river/_meta
    {
    	 "type" : "GUARiver",
    	 "mUrlGoogleAnalyzer" : {
    			"source" : {
    				"type" : "file",
    				"filePath" : "path to the file where the urls are"
    			},
    		    "destIndex" : {
    			      "index" : "index_name_where_to_put_the _analytics",
    			      "type"	: "json",
    			      "bulk_size" : 100,
    			      "flush_interval" : "5s",
    			      "max_concurrent_bulk" : 1
    		    }
    	  }
    }

2- Creating a river from an index:

        PUT _river/my_gua_river/_meta
    {
    	 "type" : "GUARiver",
    	 "mUrlGoogleAnalyzer" : {
    			"source" : {
    				"type" : "index",
    				"sourceIndexName" : "source index",
		  			"sourceIndexType" : "source type",
		  			"urlFieldName" : "url field"
	  			},
    		    "destIndex" : {
    			      "index" : "index_name_where_to_put_the _analytics",
    			      "type"	: "json",
    			      "bulk_size" : 100,
    			      "flush_interval" : "5s",
    			      "max_concurrent_bulk" : 1
    		    }
    	  }
    }

How does it work?
----------
- In case of file configuration, the plugin scans the file every 2 hours and takes each line (shortened url) and asks google api: https://googleapis.com/urlshortener/v1/url every two hours; if there are clicks, the plugin will push these clicks as a document into the specified index.

- Why two hours? Because the google api response is with clicks of the last two hours.

- In case of index configuration, the plugin scans the source index using scroll api, and for each document it takes the urlField and retrieves its clicks from google api.  


