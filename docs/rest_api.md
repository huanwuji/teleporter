### Rest Api

+ [namespace](api/namespace.md)
    - [broker](api/broker.md)
    - [instance](api/instance.md)
    - [group](api/group.md)
    - [task](api/task.md)
        + [partition](api/partition.md)
        + [stream](api/stream.md)
            - [source](api/source.md)
            - [sink](api/sink.md)
            
Global Request
#### Notify [GET /config/notify]
Will refresh all related with this key
+ Parameters:
    - key: /{key} (string)
+ Response 200 (application/json)