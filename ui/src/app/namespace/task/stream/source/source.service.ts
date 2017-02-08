import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, Identity} from "../../../../rest.servcie";
import {
  TextboxFormItem,
  FormItemBase,
  GroupFormItem,
  DynamicGroupFormItem,
  ArrayFormItem
} from "../../../../dynamic/form/form-item";

export interface Source extends Identity {
  key?: string;
  name?: string;
  address?: string;
  category?: string;
  errorRules?: string;
  client?: any;
  arguments?: string;
}

@Injectable()
export class SourceService extends ConfigService<Source> {
  constructor(public http: Http) {
    super(http);
  }

  getFormItems(category: string): FormItemBase<any>[] {
    let items: FormItemBase<any>[] = [
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
      new GroupFormItem({
        key: 'address',
        label: 'address',
        value: [
          new TextboxFormItem({key: 'key', label: 'key'}),
          new TextboxFormItem({key: 'bind', label: 'bind'}),
        ]
      }),
      new DynamicGroupFormItem({
        key: 'extraKeys',
        label: 'extraKeys'
      }),
      new ArrayFormItem({
        key: 'errorRules',
        label: 'errorRules',
      })];
    items = items.concat(this.getSourceComponentItems(category));
    items.push(new DynamicGroupFormItem({
      key: 'arguments',
      label: 'arguments'
    }));
    return items;
  }

  private getSourceComponentItems(category: string) {
    switch (category) {
      case 'kafka':
        return [
          this.getAckItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getKafkaItems()
          })
        ];
      case 'jdbc':
        return [
          this.getAckItems(),
          this.getRollerItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getJDBCItems()
          })
        ];
      case 'mongo':
        return [
          this.getAckItems(),
          this.getRollerItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getMongoItems()
          })
        ];
      case 'hdfs':
        return [
          this.getAckItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getHdfsItems()
          })
        ];
      default:
        window.alert("Category not found");
    }
  }

  private getAckItems() {
    return new GroupFormItem({
      key: 'ack',
      label: 'ack',
      value: [
        new TextboxFormItem({
          key: 'channelSize',
          label: 'channelSize',
          required: true,
          type: 'number',
          value: 1
        }),
        new TextboxFormItem({
          key: 'batchCoordinateCommitNum',
          label: 'batchCoordinateCommitNum',
          required: true,
          type: 'number',
          value: 25
        }),
        new TextboxFormItem({
          key: 'cacheSize',
          label: 'cacheSize',
          required: true,
          type: 'number',
          value: 100
        }),
        new TextboxFormItem({
          key: 'maxAge',
          label: 'maxAge',
          required: true,
          value: '1.minutes'
        })
      ]
    });
  }

  private getRollerItems() {
    return new GroupFormItem({
      key: 'roller',
      label: 'roller',
      value: [
        new TextboxFormItem({key: 'page', label: 'page', type: 'number'}),
        new TextboxFormItem({key: 'pageSize', label: 'pageSize', type: 'number'}),
        new TextboxFormItem({key: 'maxPage', label: 'maxPage', type: 'number'}),
        new TextboxFormItem({key: 'offset', label: 'offset', readonly: true, type: 'number'}),
        new TextboxFormItem({key: 'start', label: 'start'}),
        new TextboxFormItem({key: 'end', label: 'end', readonly: true}),
        new TextboxFormItem({key: 'deadline', label: 'deadline'}),
        new TextboxFormItem({key: 'period', label: 'period'}),
        new TextboxFormItem({key: 'maxPeriod', label: 'maxPeriod'}),
      ]
    });
  }

  private getKafkaItems() {
    return [
      new TextboxFormItem({key: 'topics', label: 'topics'}),
    ];
  }

  private getJDBCItems() {
    return [
      new TextboxFormItem({key: 'sql', label: 'sql'}),
    ];
  }

  private getMongoItems() {
    return [
      new TextboxFormItem({key: 'database', label: 'database'}),
      new TextboxFormItem({key: 'collection', label: 'collection'}),
      new TextboxFormItem({key: 'filter', label: 'filter'}),
    ];
  }

  private getHdfsItems() {
    return [
      new TextboxFormItem({key: 'path', label: 'path'}),
      new TextboxFormItem({key: 'offset', label: 'offset', type: 'number'}),
      new TextboxFormItem({key: 'len', label: 'len', type: 'number'}),
      new TextboxFormItem({key: 'bufferSize', label: 'bufferSize', type: '4096'}),
    ];
  }
}
