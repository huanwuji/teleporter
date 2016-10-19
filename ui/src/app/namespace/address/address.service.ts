import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, RuntimeService, KeyBean, Identity} from "../../rest.servcie";
import {FormItemBase, TextboxFormItem, DynamicGroupFormItem, GroupFormItem} from "../../dynamic/form/form-item";

export interface Address extends Identity {
  ns?: string;
  key?: string;
  name?: string;
  category?: any;
  client?: any;
  arguments?: string;
  runtime?: RuntimeAddress;
}

export interface RuntimeAddress {
  key: Object;
  value: {keys: string[], timestamp: number};
}

@Injectable()
export class AddressService extends ConfigService<Address> {
  constructor(public http: Http) {
    super(http);
  }

  getFormItems(category: string): FormItemBase<any>[] {
    return [
      new TextboxFormItem({
        key: 'id',
        label: 'id',
        type: 'number',
        readonly: true,
        required: true
      }),
      new TextboxFormItem({
        key: 'key',
        label: 'key',
        required: true
      }),
      new TextboxFormItem({
        key: 'name',
        label: 'name',
        required: true
      }),
      new TextboxFormItem({
        key: 'share',
        label: 'share',
        required: true,
        placeholder: 'true|false'
      }),
      new GroupFormItem({
        key: 'client',
        label: 'client',
        value: this.getClientItems(category)
      }),
      new DynamicGroupFormItem({
        key: 'arguments',
        label: 'arguments'
      })
    ];
  }

  private getClientItems(category: string) {
    switch (category) {
      case 'kafka_consumer':
        return this.getKafkaConsumerItems();
      case 'kafka_producer':
        return this.getKafkaProducerItems();
      case 'dataSource':
        return this.getDataSourceItems();
      case 'mongo':
        return this.getMongoItems();
      case 'influxdb':
        return this.getInfluxdbItems();
      default:
        return this.getKafkaConsumerItems();
    }
  }

  private getKafkaConsumerItems() {
    return [
      new TextboxFormItem({key: 'zookeeper.connect', label: 'zookeeper.connect'}),
      new TextboxFormItem({key: 'group.id', label: 'group.id'}),
      new TextboxFormItem({
        key: 'zookeeper.connection.timeout.ms',
        label: 'zookeeper.connection.timeout.ms',
        type: 'number',
        value: 60000
      }),
      new TextboxFormItem({
        key: 'zookeeper.session.timeout.ms',
        label: 'zookeeper.session.timeout.ms',
        type: 'number',
        value: 60000
      }),
      new TextboxFormItem({
        key: 'zookeeper.sync.time.ms',
        label: 'zookeeper.sync.time.ms',
        type: 'number',
        value: 30000
      }),
      new TextboxFormItem({
        key: 'auto.commit.interval.ms',
        label: 'auto.commit.interval.ms',
        type: 'number',
        value: 60000
      }),
      new TextboxFormItem({key: 'auto.commit.enable', label: 'auto.commit.enable', value: 'false'})
    ]
  }

  private getKafkaProducerItems() {
    return [
      new TextboxFormItem({key: 'bootstrap.servers', label: 'bootstrap.servers'}),
      new TextboxFormItem({key: 'acks', label: 'acks', type: 'number', value: 1}),
      new TextboxFormItem({
        key: 'key.serializer',
        label: 'key.serializer',
        value: 'org.apache.kafka.common.serialization.ByteArraySerializer'
      }),
      new TextboxFormItem({
        key: 'value.serializer',
        label: 'value.serializer',
        value: 'org.apache.kafka.common.serialization.ByteArraySerializer'
      }),
      new TextboxFormItem({
        key: 'compression.type',
        label: 'compression.type',
        value: 'gzip'
      })
    ]
  }

  private getDataSourceItems() {
    return [
      new TextboxFormItem({key: 'jdbcUrl', label: 'jdbcUrl'}),
      new TextboxFormItem({key: 'username', label: 'username'}),
      new TextboxFormItem({key: 'password', label: 'password'}),
      new TextboxFormItem({key: 'maximumPoolSize', label: 'maximumPoolSize', type: 'number', value: 1})
    ];
  }

  private getMongoItems() {
    return [
      new TextboxFormItem({key: 'url', label: 'url'})
    ];
  }

  private getInfluxdbItems() {
    return [
      new TextboxFormItem({key: 'host', label: 'host'}),
      new TextboxFormItem({key: 'port', label: 'port'}),
      new TextboxFormItem({key: 'db', label: 'db'})
    ];
  }
}

@Injectable()
export class RuntimeAddressService extends RuntimeService<RuntimeAddress> {
  constructor(public http: Http) {
    super(http);
  }

  owners(ns: string, address: string): Promise<KeyBean<RuntimeAddress>[]> {
    let key = `/address/${ns}/${address}/owners`;
    return this.range(key, 0, 2000);
  }
}
