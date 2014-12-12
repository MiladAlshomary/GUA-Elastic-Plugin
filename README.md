GUA-Elastic-Plugin
==================
GUA (Google Url Analytics) elastic plugin that retrieves clickes analytics of shortend urls and put the anaylitcs into a specific index.

- Your shorten urls can be either in a file ( url per line ) or in an index under specific field .  
- The plugin pulls the clicks from google api every 2 hours and save them in an index you specify .


Installation :
----------
1- Build the project using maven :
Under GUA-Elastic-Plugin folder run the command :

    mvn package
To get the gua-plugin-1.0-SNAPSHOT.zip under releases folder 

2- Install the plugin into elastic using this command :

     ./bin/plugin -url file:/path_to_gua-plugin-1.0-SNAPSHOT.zip --install GUAPlugin

Usage :
----------
Creating the gua river can be done in two ways:

1-Creating a river from a file source ( where your shorten urls are in a file ):

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

2- Creating a river from an index :

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

How does it work ?
----------
- In case of file configuration , the plugin scan the file every 2 hours and takes each line (shorten url) and ask google api : https://googleapis.com/urlshortner/v1/url every two hours and if there is clickes , the plugin will push these clicks as document into the specified index .

- why two hours ? because google api responses with clicks of the last two hours.

- In case of index configuration, the plugin scan the source index using scroll api and for each document it takes the urlField and retrieve its clicks from google api.  

