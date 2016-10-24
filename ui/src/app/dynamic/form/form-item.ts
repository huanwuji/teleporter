export class FormItemBase<T> {
  value: T;
  key: string;
  label: string;
  placeholder: string;
  required: boolean;
  controlType: string;
  readonly: boolean = false;

  constructor(options: {
    value?: T,
    key?: string,
    label?: string,
    placeholder?: string;
    required?: boolean,
    order?: number,
    controlType?: string,
    readonly?: boolean
  } = {}) {
    this.value = options.value;
    this.key = options.key || '';
    this.label = options.label || '';
    this.placeholder = options.placeholder || '';
    this.required = !!options.required;
    this.controlType = options.controlType;
    this.readonly = options.readonly;
  }
}

export class ControlType {
  static group = 'group';
  static dynamicGroup = 'dynamicGroup';
  static array = 'array';
  static dropdown = 'dropdown';
  static textbox = 'textbox';
  static textarea = 'textarea';
  static checkbox = 'checkbox';
}

export class GroupFormItem extends FormItemBase<FormItemBase<any>[]> {
  controlType = ControlType.group;

  constructor(options: {} = {}) {
    super(options);
  }
}

export class DynamicGroupFormItem extends FormItemBase<[{key: string, value: string}]> {
  controlType = ControlType.dynamicGroup;

  constructor(options: {} = {}) {
    super(options);
    this.value = options['value'] || [];
  }
}

export class ArrayFormItem extends FormItemBase<string[]> {
  controlType = ControlType.array;

  constructor(options: {} = {}) {
    super(options);
    this.value = options['value'] || [];
  }
}

export class DropdownFormItem extends FormItemBase<string> {
  controlType = ControlType.dropdown;
  options: {key: string, value: string}[] = [];

  constructor(options: {} = {}) {
    super(options);
    this.options = options['options'] || [];
  }
}

export class TextboxFormItem extends FormItemBase<string> {
  controlType = ControlType.textbox;
  type: string;

  constructor(options: {} = {}) {
    super(options);
    this.type = options['type'] || '';
  }
}

export class TextareaFormItem extends FormItemBase<string> {
  controlType = ControlType.textarea;
  type: string;
  rows: number;

  constructor(options: {} = {}) {
    super(options);
    this.type = options['type'] || '';
    this.rows = options['rows'] || 10;
  }
}

export class CheckboxFormItem extends FormItemBase<string> {
  controlType = ControlType.checkbox;
  type: string;

  constructor(options: {} = {}) {
    super(options);
    this.type = 'checkbox';
  }
}
