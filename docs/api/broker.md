# Broker
## Brokers [GET /config/range]
+ Parameters:
    - key: /broker/{namespace}/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Broker [GET /config]
+ Parameters:
    - key: /broker/{namespace}/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"key": "/broker/subscriber/1",
	"value": `{
			"ip": "localhost",
			"key": "1",
			"id": 8,
			"port": "9021",
			"tcpPort": "9022"
		}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"key": "/broker/subscriber/1",
	"value": `{
			"ip": "localhost",
			"key": "1",
			"port": "9021",
			"tcpPort": "9022"
		}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /broker/{namespace}/{key} (string)
+ Response 200 (application/json)