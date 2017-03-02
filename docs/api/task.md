# Task
## Tasks [GET /config/range]
+ Parameters:
    - key: /task/{namespace}/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Task [GET /config]
+ Parameters:
    - key: /task/{namespace}/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"key": "/task/ns/key",
	"value": `{
		"id": 9,
		"key": "key",
		"name": "",
		"group": "/group/ns/group",
		"extraKeys": {},
		"arguments": {},
		"template": ""
	}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"key": "/task/ns/key",
	"value": `{
		"key": "key",
		"name": "",
		"group": "/group/ns/key",
		"extraKeys": {},
		"arguments": {},
		"template": ""
	}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /task/{namespace}/{key} (string)
+ Response 200 (application/json)