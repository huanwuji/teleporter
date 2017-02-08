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
	"key": "/group/subscriber/shopGroup",
	"value": `{
		"id": 13,
		"key": "shopGroup",
		"tasks": ["/task/subscriber/subscriberShop"],
		"instances": ["/instance/subscriber/kuidai_pc"],
		"instanceOfflineReBalanceTime": "Inf"
	}`
}
```

## Save [POST /config]
+ Body
```javascript
{
	"key": "/group/subscriber/shopGroup",
	"value": `{
		"key": "shopGroup",
		"tasks": ["/task/subscriber/subscriberShop"],
		"instances": ["/instance/subscriber/kuidai_pc"],
		"instanceOfflineReBalanceTime": "Inf"
	}`
}
```
+ Response 200 (application/json)

## Delete [DELETE /config]
+ Parameters:
    - key: /group/{namespace}/{key} (string)
+ Response 200 (application/json)