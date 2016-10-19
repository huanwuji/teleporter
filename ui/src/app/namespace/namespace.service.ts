import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import "rxjs/add/operator/toPromise";
import {ConfigService, Identity} from "../rest.servcie";
import {FormItemBase, TextboxFormItem} from "../dynamic/form/form-item";

export interface Namespace extends Identity {
  key?: string;
}

@Injectable()
export class NamespaceService extends ConfigService<Namespace> {
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
      })
    ];
  }
}
