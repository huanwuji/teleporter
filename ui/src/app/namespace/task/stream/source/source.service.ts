import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, Identity} from "../../../../rest.servcie";
import {
  TextboxFormItem,
  FormItemBase,
  GroupFormItem,
  DynamicGroupFormItem,
  ArrayFormItem,
  CheckboxFormItem
} from "../../../../dynamic/form/form-item";

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
      new CheckboxFormItem({
        key: 'shadow',
        label: 'shadow',
        required: true
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
          this.getTransactionItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getKafkaItems()
          })
        ];
      case 'jdbc':
        return [
          this.getTransactionItems(),
          this.getScheduleItems(),
          new GroupFormItem({
            key: 'client',
            label: 'client',
            value: this.getJDBCItems()
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
          key: 'batchSize',
          label: 'batchSize',
          required: true,
          type: 'number',
          value: 512
        }),
        new TextboxFormItem({
          key: 'cacheSize',
          label: 'cacheSize',
          required: true,
          type: 'number',
          value: 512
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

  private getScheduleItems() {
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
}
