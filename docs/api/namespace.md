# Namespace
## Namespaces [GET /config/range]
+ Parameters:
    - key: /ns/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Namespace [GET /config]
+ Parameters:
    - key: /ns/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"key": "/ns/subscriber",
	"value": `{
		"id": 3,
		"key": "subscriber"
	}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"key": "/ns/subscriber",
	"value": `{
		"id": 3,
		"key": "subscriber"
	}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /ns/{key} (string)
+ Response 200 (application/json)