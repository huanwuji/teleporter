# Teleporter

**Teleporter** is a realtime dynamic data process frameworks.

It's follow [reactive-streams](https://github.com/reactive-streams/reactive-streams-jvm) specification, and use [akka-streams](https://github.com/akka/akka) as data process engine.

Now support:
component|
---------|-
kafka    |
jdbc     |

### ==Quickstart==
 * broker
   - Is a config manager, task manager. It's contains a ui, default:(http://localhost:9021)
 * instance
   - The execute engine, for run data process task.

##### UI config
 * namespace
  - broker, define all broker, for master-master
  - instance, Every instance must have a unique key
  - group, many-to-many for task and instance relation
  - task, It contain many streams
    + partition, Split task use tail regex for task streams, like: `/stream/ns/task/stream11.*`
    + stream, the real task execute unit, default is a scala script write by akka-streams.
      - source, akka-steams source, or like jdbc,kafka source for data read
      - sink, akka-streams sink, for data write
  - address, data address, like database, kafka...

### ==Install==
for linux:
```bash
/bin/teleporter.sh start broker
```
It's will auto find config from broker and run it, All can config, You don't write any logic in this.
```bash
/bin/teleporter.sh start instance
```
or for local dev
```bash
/bin/teleporter.sh start local
```
dev ui:
[ui install](ui/README.md)
```bash
ng serve --proxy-config proxy.conf.json
```

##About
The idea from camel and spring integration,  and I will use akka-streaming, reactive-streams-jvm
Every stream has source, flow, sink. source sink is elso the publisher and subscribe.
So I will make every thing like this. **simple,flexibility and powerful**.
