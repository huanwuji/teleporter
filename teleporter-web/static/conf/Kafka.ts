import {Component,Input,EventEmitter} from 'angular2/core';
import {KafkaConsumerProps,KafkaProducerProps,KafkaSourceProps} from "../Types";
import {Config} from "./Config";

@Component({
    selector: 'kafka-consumer-address',
    template: `
        <div class="form-horizontal">
           <div class="form-group">
               <label for="zookeeper.connect" class="col-sm-3 control-label">zookeeper.connect</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps['zookeeperConnect']" placeholder="zookeeper.connect"/>
               </div>
           </div>
           <div class="form-group">
               <label for="group.id" class="col-sm-3 control-label">group.id</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps['groupId']" placeholder="group.id"/>
               </div>
           </div>
           <div class="form-group">
               <label for="zookeeper.connection.timeout.ms" class="col-sm-3 control-label">zookeeperConnectionTimeoutMs</label>
               <div class="col-sm-9">
                   <input type="number" class="form-control" [(ngModel)]="_inputProps['zookeeperConnectionTimeoutMs']" placeholder="zookeeper.connection.timeout.ms"/>
               </div>
           </div>
           <div class="form-group">
               <label for="zookeeper.session.timeout.ms" class="col-sm-3 control-label">zookeeper.session.timeout.ms</label>
               <div class="col-sm-9">
                   <input type="number" class="form-control" [(ngModel)]="_inputProps['zookeeperSessionTimeoutMs']" placeholder="zookeeper.session.timeout.ms"/>
               </div>
           </div>
           <div class="form-group">
               <label for="zookeeper.sync.time.ms" class="col-sm-3 control-label">zookeeper.sync.time.ms</label>
               <div class="col-sm-9">
                   <input type="number" class="form-control" [(ngModel)]="_inputProps['zookeeperSyncTimeMs']" placeholder="zookeeper.sync.time.ms"/>
               </div>
           </div>
           <div class="form-group">
               <label for="auto.commit.interval.ms" class="col-sm-3 control-label">auto.commit.interval.ms</label>
               <div class="col-sm-9">
                   <input type="number" class="form-control" [(ngModel)]="_inputProps['autoCommitIntervalMs']" placeholder="auto.commit.interval.ms"/>
               </div>
           </div>
           <div class="form-group">
               <label for="auto.commit.enable" class="col-sm-3 control-label">auto.commit.enable</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps['autoCommitEnable']" placeholder="auto.commit.enable"/>
               </div>
           </div>
        </div>
    `
})
export class KafkaConsumerForm extends Config<KafkaConsumerProps> {
    _inputProps:KafkaConsumerProps = {
        'zookeeperConnect': '',
        'groupId': '',
        'zookeeperConnectionTimeoutMs': 60000,
        'zookeeperSessionTimeoutMs': 60000,
        'zookeeperSyncTimeMs': 30000,
        'autoCommitIntervalMs': 60000,
        'autoCommitEnable': 'false'
    };

    constructor() {
        super();
    }
}

@Component({
    selector: 'kafka-producer-address',
    template: `
        <div class="form-horizontal">
           <div class="form-group">
               <label for="bootstrap.servers" class="col-sm-3 control-label">bootstrap.servers</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps['bootstrapServers']" placeholder="bootstrap.servers"/>
               </div>
           </div>
           <div class="form-group">
               <label for="acks" class="col-sm-3 control-label">acks</label>
               <div class="col-sm-9">
                   <input type="number" class="form-control" [(ngModel)]="_inputProps['acks']" placeholder="acks"/>
               </div>
           </div>
           <div class="form-group">
               <label for="key.serializer" class="col-sm-3 control-label">key.serializer</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps['keySerializer']" placeholder="key.serializer"/>
               </div>
           </div>
           <div class="form-group">
               <label for="value.serializer" class="col-sm-3 control-label">value.serializer</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps['valueSerializer']" placeholder="value.serializer"/>
               </div>
           </div>
           <div class="form-group">
               <label for="compression.type" class="col-sm-3 control-label">compression.type</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps['compressionType']" placeholder="compression.type"/>
               </div>
           </div>
        </div>
    `
})
export class KafkaProducerForm extends Config<KafkaProducerProps> {
    _inputProps:KafkaProducerProps = {
        'bootstrapServers': '',
        'acks': 1,
        'keySerializer': 'org.apache.kafka.common.serialization.ByteArraySerializer',
        'valueSerializer': 'org.apache.kafka.common.serialization.ByteArraySerializer',
        'compressionType': 'gzip'
    };

    constructor() {
        super();
    }
}

@Component({
    selector: 'kafka-source',
    template: `
    <div class="form-horizontal">
       <div class="form-group">
           <label for="topics" class="col-sm-3 control-label">topics</label>
           <div class="col-sm-9">
               <input type="text" class="form-control" [(ngModel)]="_inputProps.topics" placeholder="topics"/>
           </div>
       </div>
    </div>
    `
})
export class KafkaSourceForm extends Config<KafkaSourceProps> {
    _inputProps:KafkaSourceProps = {
        topics: ''
    };

    constructor() {
        super();
    }
}