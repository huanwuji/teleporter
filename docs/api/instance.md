# Instance
## Instances [GET /config/range]
+ Parameters:
    - key: /instance/{namespace}/{key} (string)
    - start: 1 (number)
    - end: 20 (number)
+ Response 200 (application/json)
    
    
## Instance [GET /config]
+ Parameters:
    - key: /instance/{namespace}/{key} (string)
+ Response 200 (application/json)
```javascript
{
	"key": "/instance/ns/kuidai_pc",
	"value": `{
		"id": 14,
		"key": "kuidai_pc",
		"group": "/group/ns/group"
	}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"key": "/instance/ns/kuidai_pc",
	"value": `{
		"key": "kuidai_pc",
		"group": "/group/ns/key"
	}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /instance/{namespace}/{key} (string)
+ Response 200 (application/json)