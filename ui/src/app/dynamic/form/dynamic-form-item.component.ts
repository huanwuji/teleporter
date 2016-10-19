import {Component, Input} from "@angular/core";
import {FormGroup, FormControl, FormArray} from "@angular/forms";
import {FormItemBase, ControlType} from "./form-item";

@Component({
  selector: 'df-form-item',
  templateUrl: './dynamic-form-item.component.html'
})
export class DynamicFormItemComponent {
  @Input() formItem: FormItemBase<any>;
  @Input() form: FormGroup;
  private dynamicKey: string;

  dynamicItemAdd() {
    switch (this.formItem.controlType) {
      case ControlType.dynamicGroup :
        let dynamicGroup = <FormGroup>this.form.controls[this.formItem.key];
        dynamicGroup.addControl(this.dynamicKey, new FormControl());
        this.formItem.value.push({key: this.dynamicKey, value: ''});
        this.dynamicKey = '';
        break;
      case ControlType.array:
        this.formItem.value.push('');
        let formArray = <FormArray>this.form.controls[this.formItem.key];
        formArray.push(new FormControl());
        break;
    }
  }

  dynamicItemDel(i: number|string) {
    switch (this.formItem.controlType) {
      case ControlType.dynamicGroup :
        this.formItem.value = this.formItem.value.filter((v: any, ai: number, a: any)=>v.key != i);
        let dynamicGroup = <FormGroup>this.form.controls[this.formItem.key];
        dynamicGroup.removeControl(<string>i);
        break;
      case ControlType.array:
        this.formItem.value = this.formItem.value.filter((v: any, ai: number, a: any)=>ai != i);
        let formArray = <FormArray>this.form.controls[this.formItem.key];
        formArray.removeAt(<number>i);
        break;
    }
  }

  // get isValid() {
  //     return this.form.controls[this.formItem.key].valid;
  // }
}
