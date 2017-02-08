# Stream
## Streams [GET /config/range]
+ Parameters:
    - key: /stream/{namespace}/{task}/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Stream [GET /config]
+ Parameters:
    - key: /stream/{namespace}/{task}/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"key": "/stream/test/test_task/kafka_receiver",
	"value": `{
		"id": 442,
		"key": "kafka_receiver",
		"name": "",
		"cron": "",
		"status": "NORMAL",
		"extraKeys": {},
		"errorRules": [".* => restart(delay=10.seconds)"],
		"arguments": {},
		"template": "//scala stream code"
	}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"key": "/stream/test/test_task/kafka_receiver",
	"value": `{
		"key": "kafka_receiver",
		"name": "",
		"cron": "",
		"status": "NORMAL",
		"extraKeys": {},
		"errorRules": [".* => restart(delay=10.seconds)"],
		"arguments": {},
		"template": "//scala stream code"
	}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /stream/{namespace}/{task}/{key} (string)
+ Response 200 (application/json)