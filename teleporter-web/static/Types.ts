export interface Task {
    id?:number;
    name:string;
    props:any;
    desc:string;
}

export interface Address {
    id? :number;
    taskId?:number;
    category?:string;
    name:string;
    props?:any;
}

export interface Source {
    id? :number;
    taskId?:number;
    addressId?:number;
    category?:string;
    name:string;
    props?:any;
}

export interface Sink {
    id? :number;
    taskId?:number;
    addressId?:number;
    category?:string;
    name:string;
    props?:any;
}

export interface PageRollerProps {
    page?:string;
    pageSize?:string;
    maxPage?:string;
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
}

export interface KafkaSourceProps {
    topic:string;
}

export interface DataSourceSourceProps extends PageRollerProps,TimeRollerProps {
    sql:string;
}

export interface KafkaProps extends Transaction,KafkaSourceProps {

}

export interface DataSourceProps {
    jdbcUrl?:string;
    username?:string;
    password?:string;
    maximumPoolSize?:string;
}