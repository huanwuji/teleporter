import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, Identity} from "../../rest.servcie";
import {FormItemBase, TextboxFormItem} from "../../dynamic/form/form-item";

export interface Broker extends Identity {
  key?: string;
  ip?: string;
  port?: number;
  tcpPort?: number;
}

@Injectable()
export class BrokerService extends ConfigService<Broker> {
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
        key: 'ip',
        label: 'ip',
        value: 'localhost'
      }),
      new TextboxFormItem({
        key: 'port',
        label: 'port',
        type: 'number',
        value: '9021'
      }),
      new TextboxFormItem({
        key: 'tcpPort',
        label: 'tcpPort',
        type: 'number',
        value: '9022'
      })
    ];
  }
}
