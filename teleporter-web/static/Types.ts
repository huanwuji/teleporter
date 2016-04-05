export interface Task {
    id?:number;
    name:string;
    props?:any;
    streamScript?:string;
    desc?:string;
}
export interface Stream {
    id?:number;
    taskId?:number;
    name?:string;
    props?:any;
    desc?:any;
}
export interface GlobalProps {
    id?:number;
    name:string;
    props?:any;
    desc?:string;
}

export interface Address {
    id? :number;
    category?:string;
    name:string;
    props?:any;
}

export interface Source {
    id? :number;
    taskId?:number;
    streamId?:number;
    addressId?:number;
    category?:string;
    name:string;
    status?:string;
    props?:any;
}

export interface Sink {
    id? :number;
    taskId?:number;
    streamId?:number;
    addressId?:number;
    category?:string;
    name:string;
    props?:any;
}

export interface PageRollerProps {
    page?:string;
    pageSize?:string;
    maxPage?:string;
    offset?:string;
}

export interface TimeRollerProps {
    deadline?:string;
    start?:string;
    period?:string;
    maxPeriod?:string;
}

export interface KafkaConsumerProps {
    'zookeeperConnect'?: string;
    'groupId'?: string;
    'zookeeperConnectionTimeoutMs'?: number;
    'zookeeperSessionTimeoutMs'?: number;
    'zookeeperSyncTimeMs'?: number;
    'autoCommitIntervalMs'?: number;
    'autoCommitEnable'?: string;
}

export interface KafkaProducerProps {
    'bootstrapServers'?:string,
    'acks'?:number,
    'keySerializer'?:string,
    'valueSerializer'?:string,
    'compressionType'?:string
}

export interface Transaction {
    channelSize?:number;
    batchSize?:number;
    maxAge?:string;
    maxCacheSize?:number;
    commitDelay?:string;
    recoveryPointEnabled?:boolean;
    timeoutRetry?:boolean;
}

export interface KafkaSourceProps {
    topics:string;
}

export interface DataSourceSourceProps extends PageRollerProps,TimeRollerProps {
    sql:string;
    errorRules?:string;
}

export interface DataSourceSinkProps {
    errorRules?:string;
}

export interface KafkaProps extends Transaction,KafkaSourceProps {

}

export interface DataSourceProps {
    jdbcUrl?:string;
    username?:string;
    password?:string;
    maximumPoolSize?:string;
}

export interface MongoProps {
    url?:string;
}

export interface MongoSourceProps extends PageRollerProps,TimeRollerProps {
    database?:string;
    collection?:string;
    query?:string;
}

export interface InfluxdbProps {
    host?:string;
    port?:string;
    db?:string;
}