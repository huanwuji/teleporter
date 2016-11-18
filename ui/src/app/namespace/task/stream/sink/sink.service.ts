import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, Identity} from "../../../../rest.servcie";
import {
  FormItemBase,
  TextboxFormItem,
  DynamicGroupFormItem,
  GroupFormItem,
  ArrayFormItem
} from "../../../../dynamic/form/form-item";

export interface Sink extends Identity {
  key?: string;
  name?: string;
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
        key: 'address',
        label: 'address',
        required: true
      }),
      new ArrayFormItem({
        key: 'extraKeys',
        label: 'extraKeys'
      }),
      new ArrayFormItem({
        key: 'errorRules',
        label: 'errorRules'
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
      case 'kafka':
        return this.getKafkaItems();
      case 'dataSource':
        return this.getDataSourceItems();
      default:
        return this.getKafkaItems();
    }
  }

  private getKafkaItems(): FormItemBase<any>[] {
    return [];
  }

  private getDataSourceItems(): FormItemBase<any>[] {
    return [
      new TextboxFormItem({
        key: 'parallelism',
        label: 'parallelism',
        type: 'number',
        required: true,
        value: 1
      }),
    ];
  }
}
