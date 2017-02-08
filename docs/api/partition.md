# Partition
## Partitions [GET /config/range]
+ Parameters:
    - key: /partition/{namespace}/{task}/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Partition [GET /config]
+ Parameters:
    - key: /partition/{namespace}/{task}/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"key": "/partition/test/test_task/1",
	"value": `{
		"id": 436,
		"key": "1",
		"keys": ["/stream/test/test_task/.*"]
	}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"key": "/partition/test/test_task/1",
	"value": `{
		"key": "1",
		"keys": ["/stream/test/test_task/.*"]
	}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /partition/{namespace}/{task}/{key} (string)
+ Response 200 (application/json)