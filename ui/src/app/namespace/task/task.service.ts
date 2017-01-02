import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, Identity} from "../../rest.servcie";
import {FormItemBase, TextboxFormItem, DynamicGroupFormItem, TextareaFormItem} from "../../dynamic/form/form-item";

export interface Task extends Identity {
  ns?: string;
  key?: string;
  name?: string;
  group?: string;
  arguments?: string;
  template?: string;
}

@Injectable()
export class TaskService extends ConfigService<Task> {
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
      new TextboxFormItem({
        key: 'name',
        label: 'name',
        required: true
      }),
      new TextboxFormItem({
        key: 'group',
        label: 'group'
      }),
      new DynamicGroupFormItem({
        key: 'extraKeys',
        label: 'extraKeys'
      }),
      new DynamicGroupFormItem({
        key: 'arguments',
        label: 'arguments'
      }),
      new TextareaFormItem({
        key: 'template',
        label: 'template'
      })
    ];
  }
}
