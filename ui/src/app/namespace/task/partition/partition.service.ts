import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, RuntimeService, Identity} from "../../../rest.servcie";
import {TextboxFormItem, FormItemBase, ArrayFormItem} from "../../../dynamic/form/form-item";

export interface Partition extends Identity {
  key?: string;
  keys?: string;
  runtime?: RuntimePartition;
}

export interface RuntimePartition {
  instance?: string;
  timestamp: number;
}

@Injectable()
export class PartitionService extends ConfigService<Partition> {
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
        key: 'keys',
        label: 'keys'
      })
    ];
  }
}
@Injectable()
export class RuntimePartitionService extends RuntimeService<RuntimePartition> {
  constructor(public http: Http) {
    super(http);
  }
}
