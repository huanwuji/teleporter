import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, Identity} from "../../rest.servcie";
import {FormItemBase, TextboxFormItem, ArrayFormItem} from "../../dynamic/form/form-item";

export interface Group extends Identity {
  key?: string;
  tasks?: string[];
  instances?: string[];
  instanceOfflineReBalanceTime?: string;
}

@Injectable()
export class GroupService extends ConfigService<Group> {
  constructor(public http: Http) {
    super(http);
  }

  getFormItems(): FormItemBase<any>[] {
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
      new ArrayFormItem({
        key: 'tasks',
        label: 'tasks'
      }),
      new ArrayFormItem({
        key: 'instances',
        label: 'instances'
      }),
      new TextboxFormItem({
        key: 'instanceOfflineReBalanceTime',
        label: 'instanceOfflineReBalanceTime',
        type: 'number',
        required: true
      }),
    ];
  }
}
