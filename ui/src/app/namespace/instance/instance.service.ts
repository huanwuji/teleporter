import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, Identity, RuntimeService} from "../../rest.servcie";
import {FormItemBase, TextboxFormItem} from "../../dynamic/form/form-item";

export interface RuntimeInstance {
  ip: string;
  port: number;
  status: string;
  broker: string;
  timestamp: number;
}

export interface Instance extends Identity {
  key?: string;
  group?: string;
  runtime?: RuntimeInstance;
}

@Injectable()
export class InstanceService extends ConfigService<Instance> {
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
        key: 'group',
        label: 'group'
      })
    ];
  }
}

@Injectable()
export class RuntimeInstanceService extends RuntimeService<RuntimeInstance> {
  constructor(public http: Http) {
    super(http);
  }
}
