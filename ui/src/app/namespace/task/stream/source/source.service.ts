import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, Identity} from "../../../../rest.servcie";
import {TextboxFormItem, FormItemBase, GroupFormItem, DynamicGroupFormItem} from "../../../../dynamic/form/form-item";

export interface Source extends Identity {
  key?: string;
  name?: string;
  address?: string;
  category?: string;
  errorRules?: string;
  client?: any;
  shadow?: string;
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
      new TextboxFormItem({
        key: 'address',
        label: 'address',
        required: true
      }),
      new TextboxFormItem({
        key: 'shadow',
        label: 'shadow',
        required: true
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
          this.getTransactionItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getKafkaItems()
          })
        ];
      case 'dataSource':
        return [
          this.getTransactionItems(),
          this.getScheduleItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getDataSourceItems()
          })
        ];
      case 'mongo':
        return [
          this.getTransactionItems(),
          this.getScheduleItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getMongoItems()
          })
        ];
      default:
        window.alert("Category not found");
    }
  }

  private getTransactionItems() {
    return new GroupFormItem({
      key: 'transaction',
      label: 'transaction',
      value: [
        new TextboxFormItem({
          key: 'channelSize',
          label: 'channelSize',
          required: true,
          type: 'number',
          value: 1
        }),
        new TextboxFormItem({
          key: 'maxAge',
          label: 'maxAge',
          required: true,
          value: '1.minutes'
        }),
        new TextboxFormItem({
          key: 'maxBlockNum',
          label: 'maxBlockNum',
          required: true,
          type: 'number',
          value: 100000
        }),
        new TextboxFormItem({
          key: 'commitDelay',
          label: 'commitDelay',
          placeholder: '1.minutes',
          required: true,
        }),
        new TextboxFormItem({
          key: 'transaction',
          label: 'transaction',
          required: true,
          type: 'number',
          value: 10000
        })
      ]
    });
  }

  private getScheduleItems() {
    return new GroupFormItem({
      key: 'schedule',
      label: 'schedule',
      value: [
        new TextboxFormItem({key: 'page', label: 'page'}),
        new TextboxFormItem({key: 'pageSize', label: 'pageSize'}),
        new TextboxFormItem({key: 'maxPage', label: 'maxPage'}),
        new TextboxFormItem({key: 'offset', label: 'offset'}),
        new TextboxFormItem({key: 'deadline', label: 'deadline'}),
        new TextboxFormItem({key: 'start', label: 'start'}),
        new TextboxFormItem({key: 'period', label: 'period'}),
        new TextboxFormItem({key: 'maxPeriod', label: 'maxPeriod'}),
        new TextboxFormItem({key: 'cron', label: 'cron'})
      ]
    });
  }

  private getKafkaItems() {
    return [
      new TextboxFormItem({key: 'topics', label: 'topics'}),
    ];
  }

  private getDataSourceItems() {
    return [
      new TextboxFormItem({key: 'sql', label: 'sql'}),
    ];
  }

  private getMongoItems() {
    return [
      new TextboxFormItem({key: 'database', label: 'database'}),
      new TextboxFormItem({key: 'collection', label: 'collection'}),
      new TextboxFormItem({key: 'query', label: 'query'}),
    ];
  }
}
