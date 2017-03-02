# Variable
## Variables [GET /config/range]
+ Parameters:
    - key: /ns/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Variable [GET /config]
+ Parameters:
    - key: /ns/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"key": "/variable/ns/env_pre",
	"value": `{
		"arguments": {
			"host": "www.baidu.com",
			"port": "80",
		},
		"key": "env_pre",
		"name": "",
		"id": 7
	}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"key": "/variable/ns/env_pre",
	"value": `{
		"arguments": {
			"host": "www.baidu.com",
			"port": "80",
		},
		"key": "env_pre",
		"name": "",
	}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /ns/{key} (string)
+ Response 200 (application/json)