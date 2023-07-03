
#!/usr/bin/env bash

curl -XPUT 'http://127.0.0.1:9200/daily_pakistan/' -H 'Content-Type: application/json' -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 1
        }
    }
}'