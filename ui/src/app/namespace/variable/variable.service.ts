import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, RuntimeService, Identity} from "../../rest.servcie";
import {FormItemBase, TextboxFormItem, DynamicGroupFormItem} from "../../dynamic/form/form-item";

export interface Variable extends Identity {
  ns?: string;
  key?: string;
  name?: string;
  arguments?: string;
  runtime?: Object;
}

@Injectable()
export class VariableService extends ConfigService<Variable> {
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
      new DynamicGroupFormItem({
        key: 'arguments',
        label: 'arguments'
      })
    ];
  }
}
@Injectable()
export class VariableRuntimeService extends RuntimeService<Variable> {
  constructor(public http: Http) {
    super(http);
  }
}
