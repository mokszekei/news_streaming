
schema = {
    "settings": {
        "number_of_shards": 1, # the number of partitions that will keep the data of this Index. 
                               # If you are running a cluster of multiple Elastic nodes then entire 
                                # data is split across them. In simple words, if there are 5 shards then 
                                # entire data is available across 5 shards and ElasticSearch cluster can 
                                # serve requests from any of its node.
        "number_of_replicas": 0
    },
    "mappings": {
        "dynamic": "strict",  ## The automatic detection and addition of new fields is called dynamic mapping. 
                              ## The dynamic mapping rules can be customised to suit your purposes with: 
        "properties": {
            "source": {
                "type": "nested",
                "properties": {
                    "step": {"type": "text"}
                }
            },
            "author": {
                "type": "text"
            },
            "description": {
                "type": "text"
            },
            "url": {
                "type": "text"
            },
            "publishedAt": {
                "type": "date",   ## If date_detection is enabled (default), then new string fields are checked 
                                 ## to see whether their contents match any of the date patterns 
                                 ## specified in dynamic_date_formats
                "format": "yyyy-MM-dd'T'HH:mm:ssZ"   # if don't specify format, use defult format
            },
            "content": {
                "type": "text"
            },
        },
    }
}


schema_label = {
    "settings": {
        "number_of_shards": 1, # the number of partitions that will keep the data of this Index. 
                               # If you are running a cluster of multiple Elastic nodes then entire 
                                # data is split across them. In simple words, if there are 5 shards then 
                                # entire data is available across 5 shards and ElasticSearch cluster can 
                                # serve requests from any of its node.
        "number_of_replicas": 0
    },
    "mappings": {
        "dynamic": "strict",  ## The automatic detection and addition of new fields is called dynamic mapping. 
                              ## The dynamic mapping rules can be customised to suit your purposes with: 
        "properties": {
            "source": {
                "type": "nested",
                "properties": {
                    "step": {"type": "text"}
                }
            },
            "author": {
                "type": "text"
            },
            "description": {
                "type": "text"
            },
            "url": {
                "type": "text"
            },
            "publishedAt": {
                "type": "date",   ## If date_detection is enabled (default), then new string fields are checked 
                                 ## to see whether their contents match any of the date patterns 
                                 ## specified in dynamic_date_formats
                "format": "yyyy-MM-dd'T'HH:mm:ssZ"   # if don't specify format, use defult format
            },
            "content": {
                "type": "text"
            },
            "label": {
                "type": "text"
            },
        },
    }
}


schema_label2 = {
"label_news_test":{
    "mappings": {
     "data": {
        "properties": {
            "source": {
                "type": "nested",
                "properties": {
                    "step": {"type": "text"}
                }
            },
            "author": {
                "type": "text"
            },
            "description": {
                "type": "text"
            },
            "url": {
                "type": "text"
            },
            "publishedAt": {
                "type": "date",   ## If date_detection is enabled (default), then new string fields are checked 
                                 ## to see whether their contents match any of the date patterns 
                                 ## specified in dynamic_date_formats
                "format": "yyyy-MM-dd'T'HH:mm:ssZ"   # if don't specify format, use defult format
            },
            "content": {
                "type": "text"
            },
            "label": {
                "type": "text"
            },
        }
    }
}
}
   }