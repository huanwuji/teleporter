import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, Identity} from "../../../../rest.servcie";
import {
  ArrayFormItem,
  DynamicGroupFormItem,
  FormItemBase,
  GroupFormItem,
  TextboxFormItem
} from "../../../../dynamic/form/form-item";

export interface Sink extends Identity {
  key?: string;
  category?: string;
  address?: string;
  errorRules?: string;
  client?: any;
  arguments?: string;
}

@Injectable()
export class SinkService extends ConfigService<Sink> {
  constructor(public http: Http) {
    super(http);
  }

  getFormItems(category: string): FormItemBase<any>[] {
    let items = [
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
        placeholder: 'regex => reload|retry|resume|stop(delay = 1.seconds, retries = 1, next = stop)'
      })];
    items = items.concat(this.getSinkItems(category));
    items.push(new DynamicGroupFormItem({
      key: 'arguments',
      label: 'arguments'
    }));
    return items;
  }

  private getSinkItems(category: string) {
    switch (category) {
      case 'kafka_producer':
        return [
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getKafkaItems()
          })
        ];
      case 'jdbc':
        return [
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getJDBCItems()
          })
        ];
      case 'elasticsearch':
        return [
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getElasticserach()
          })
        ];
      case 'kudu':
        return [
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getKuduItems()
          })
        ];
      case 'hdfs':
        return [
          this.getRollerItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getHdfsItems()
          })
        ];
      case 'hbase':
        return [
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getHbaseItems()
          })
        ];
      case 'file':
        return [
          this.getRollerItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getFileItems()
          })
        ];
      default:
        throw new Error(`UnMatch sink category ${category}`)
    }
  }

  private getKafkaItems(): FormItemBase<any>[] {
    return [];
  }

  private getElasticserach(): FormItemBase<any>[] {
    return [
      new TextboxFormItem({key: 'parallelism', label: 'parallelism', type: 'number', required: true, value: 1})
    ];
  }

  private getKuduItems(): FormItemBase<any>[] {
    return [
      new TextboxFormItem({key: 'parallelism', label: 'parallelism', type: 'number', required: true, value: 1}),
      new TextboxFormItem({
        key: 'autoFit',
        label: 'autoFit',
        required: true,
        value: 'DEFAULT',
        placeholder: 'DEFAULT|SCHEMA'
      })
    ];
  }

  private getJDBCItems(): FormItemBase<any>[] {
    return [
      new TextboxFormItem({key: 'parallelism', label: 'parallelism', type: 'number', required: true, value: 1})
    ];
  }

  private getHdfsItems(): FormItemBase<any>[] {
    return [
      new TextboxFormItem({
        key: 'path',
        label: 'path',
        placeholder: 'If roller is config, support /tmp/file/test_{0,date,yyyy-MM-dd}_{1}.txt'
      }),
      new TextboxFormItem({key: 'overwrite', label: 'overwrite', value: true})
    ];
  }

  private getHbaseItems(): FormItemBase<any>[] {
    return [
      new TextboxFormItem({key: 'parallelism', label: 'parallelism', type: 'number', required: true, value: 1})
    ];
  }

  private getFileItems(): FormItemBase<any>[] {
    return [
      new TextboxFormItem({
        key: 'path',
        label: 'path',
        placeholder: 'If roller is config, support /tmp/file/test_{0,date,yyyy-MM-dd_HH:mm:ss}_{1}.txt'
      }),
      new TextboxFormItem({key: 'offset', label: 'offset'}),
      new TextboxFormItem({
        key: 'openOptions',
        label: 'openOptions',
        placeholder: 'READ, WRITE, APPEND, TRUNCATE_EXISTING, CREATE, CREATE_NEW, DELETE_ON_CLOSE, SPARSE, SYNC, DSYNC)',
        value: 'CREATE,WRITE,APPEND'
      })
    ];
  }

  private getRollerItems() {
    return new GroupFormItem({
      key: 'roller',
      label: 'roller',
      value: [
        new TextboxFormItem({
          key: 'cron',
          label: 'cron',
          required: true,
          placeholder: "* * * * *"
        }),
        new TextboxFormItem({
          key: 'size',
          label: 'size',
          required: true,
          placeholder: "500MB"
        })
      ]
    });
  }
}
