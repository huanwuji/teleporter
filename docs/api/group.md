# Group
## Groups [GET /config/range]
+ Parameters:
    - key: /group/{namespace}/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Group [GET /config]
+ Parameters:
    - key: /group/{namespace}/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"key": "/group/ns/group",
	"value": `{
		"id": 13,
		"key": "group",
		"tasks": ["/task/group/test1"],
		"instances": ["/instance/test/kuidai_pc"],
		"instanceOfflineReBalanceTime": "Inf"
	}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"key": "/group/ns/group",
	"value": `{
		"key": "key",
		"tasks": ["/task/ns/tasks"],
		"instances": ["/instance/ns/kuidai_pc"],
		"instanceOfflineReBalanceTime": "Inf"
	}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /group/{namespace}/{key} (string)
+ Response 200 (application/json)