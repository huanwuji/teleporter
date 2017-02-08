# Address
## Addresses [GET /config/range]
+ Parameters:
    - key: /address/{namespace}/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Address [GET /config]
+ Parameters:
    - key: /address/{namespace}/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"_id": "/address/test/test_kafka_consumer",
	"value": `{
		"name": "",
		"key": "test_kafka_consumer",
		"id": 434,
		"client": {
			"auto.commit.interval.ms": 60000,
			"zookeeper.connection.timeout.ms": 60000,
			"auto.commit.enable": "false",
			"group.id": "test",
			"zookeeper.connect": "172.18.21.83:2181",
			"zookeeper.session.timeout.ms": 60000,
			"zookeeper.sync.time.ms": 30000
		},
		"category": "kafka_consumer",
		"arguments": {}
	}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"_id": "/address/test/test_kafka_consumer",
	"value": `{
		"name": "",
		"key": "test_kafka_consumer",
		"client": {
			"auto.commit.interval.ms": 60000,
			"zookeeper.connection.timeout.ms": 60000,
			"auto.commit.enable": "false",
			"group.id": "test",
			"zookeeper.connect": "172.18.21.83:2181",
			"zookeeper.session.timeout.ms": 60000,
			"zookeeper.sync.time.ms": 30000
		},
		"category": "kafka_consumer",
		"arguments": {}
	}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /address/{namespace}/{key} (string)
+ Response 200 (application/json)