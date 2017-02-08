# Sink
## Sinks [GET /config/range]
+ Parameters:
    - key: /sink/{namespace}/{task}/{stream}/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Sink [GET /config]
+ Parameters:
    - key: /sink/{namespace}/{task}/{stream}/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"key": "/sink/test/test_task/kafka_send/send_sink",
	"value": `{
		"id": 439,
		"key": "send_sink",
		"name": "",
		"address": {
			"key": "/address/test/test_kafka_producer",
			"bind": ""
		},
		"extraKeys": {},
		"errorRules": [".*:INFO => stop()"],
		"client": {},
		"arguments": {},
		"category": "kafka"
	}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"key": "/sink/test/test_task/kafka_send/send_sink",
	"value": `{
		"id": 439,
		"key": "send_sink",
		"name": "",
		"address": {
			"key": "/address/test/test_kafka_producer",
			"bind": ""
		},
		"extraKeys": {},
		"errorRules": [".*:INFO => stop()"],
		"client": {},
		"arguments": {},
		"category": "kafka"
	}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /sink/{namespace}/{task}/{stream}/{key} (string)
+ Response 200 (application/json)