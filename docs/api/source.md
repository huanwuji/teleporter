# Source
## Sources [GET /config/range]
+ Parameters:
    - key: /source/{namespace}/{task}/{stream}/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Source [GET /config]
+ Parameters:
    - key: /source/{namespace}/{task}/{stream}/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"key": "/source/test/test_task/kafka_receiver/kafka_source",
	"value": `{
		"id": 443,
		"key": "kafka_source",
		"name": "",
		"address": {
			"key": "/address/test/test_kafka_consumer",
			"bind": ""
		},
		"extraKeys": {},
		"errorRules": [".*:INFO => retry(delay=1.second)"],
		"ack": {
			"channelSize": 1,
			"batchCoordinateCommitNum": 25,
			"cacheSize": "60",
			"maxAge": "1.minutes"
		},
		"client": {
			"topics": "ppp:1"
		},
		"arguments": {},
		"category": "kafka"
	}`
}
```

## Save [POST /config]
+ Body
hdfs:
```javascript
{
	"key": "/source/test/test_task/kafka_receiver/kafka_source",
	"value": `{
              	"key": "test",
              	"name": "",
              	"address": {
              		"key": "",
              		"bind": "" //required:false
              	},
              	"extraKeys": {},
              	"errorRules": [],
              	"ack": {
              		"channelSize": 1,
              		"batchCoordinateCommitNum": 25,
              		"cacheSize": 100,
              		"maxAge": "1.minutes"
              	},
              	"client": {
              		"path": "",
              		"offset": "",
              		"len": "",
              		"bufferSize": ""
              	},
              	"arguments": {},
              	"category": "hdfs"
              }`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /source/{namespace}/{task}/{stream}/{key} (string)
+ Response 200 (application/json)