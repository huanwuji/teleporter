import {Injectable} from "@angular/core";
import {FormItemBase, GroupFormItem, DynamicGroupFormItem, ControlType} from "./form-item";
import {FormControl, FormArray, FormGroup, Validators} from "@angular/forms";

@Injectable()
export class FormItemService {
  toForm(formItems: FormItemBase<any>[], obj: any = {}): {formItems: FormItemBase<any>[], formGroup: FormGroup} {
    this.fillValue(formItems, obj);
    return {formItems: formItems, formGroup: this.toFormGroup(formItems)};
  }

  toFormGroup(formItems: FormItemBase<any>[]) {
    let group: any = {};
    formItems.forEach(formItem => {
      switch (formItem.controlType) {
        case ControlType.group:
          group[formItem.key] = this.toFormGroup(formItem.value);
          break;
        case ControlType.array:
          group[formItem.key] = new FormArray(formItem.value.map((v: string) => new FormControl(v)));
          break;
        case ControlType.dynamicGroup:
          if (formItem.value) {
            let tmpGroup = {};
            formItem.value.map((entry: {key: string, value: string}) => tmpGroup[entry.key] = new FormControl(entry.value));
            group[formItem.key] = new FormGroup(tmpGroup);
          }
          break;
        default:
          group[formItem.key] = formItem.required ? new FormControl(formItem.value || '', Validators.required)
            : new FormControl(formItem.value || '');
      }
    });
    return new FormGroup(group);
  }

  private fillValue(formItems: FormItemBase<any>[], obj: any) {
    for (let formItem of formItems) {
      let value = obj[formItem.key];
      if (value) {
        if (formItem instanceof DynamicGroupFormItem) {
          formItem.value = <[{key: string, value: string}]>Object.keys(value).map((key: string) => {
            return {key: key, value: value[key]};
          })
        } else if (formItem instanceof GroupFormItem) {
          this.fillValue(formItem.value, value);
        } else {
          formItem.value = value;
        }
      }
    }
  }
}
