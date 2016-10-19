import {Component, OnInit} from "@angular/core";
import {ActivatedRoute} from "@angular/router";
import {SourceService, Source} from "./source.service";
import {Location} from "@angular/common";
import {KeyBean} from "../../../../rest.servcie";
import {FormControl, FormGroup} from "@angular/forms";
import {FormItemBase} from "../../../../dynamic/form/form-item";
import {FormItemService} from "../../../../dynamic/form/form-item.service";

@Component({
  selector: 'source-list',
  templateUrl: './source-list.component.html'
})
export class SourceListComponent implements OnInit {
  private kbs: KeyBean<Source>[] = [];
  private ns: string;
  private task: string;
  private stream: string;

  constructor(private sourceService: SourceService, private route: ActivatedRoute) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.task = params['task'];
      this.stream = params['stream'];
      if (this.task) {
        this.list();
      }
    })
  }

  private list() {
    this.sourceService.range(`/source/${this.ns}/${this.task}/${this.stream}`, 0, 2000)
      .then(kbs => this.kbs = kbs);
  }

  del(key: string) {
    if (window.confirm("Are you sure delete it !!!")) {
      this.sourceService.remove(key).then(() => this.list());
    }
  }

  refresh(key: string) {
    this.sourceService.refresh(key);
  }
}

@Component({
  selector: 'source-detail',
  templateUrl: './source-detail.component.html'
})
export class SourceDetailComponent implements OnInit {
  private formItems: FormItemBase<any>[];
  private formGroup: FormGroup = new FormGroup({"": new FormControl()});
  private payLoad: string;
  private ns: string = "";
  private task: string = "";
  private stream: string = "";
  private key: string;

  constructor(public route: ActivatedRoute, public location: Location,
              public sourceService: SourceService, private formItemService: FormItemService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.task = params['task'];
      this.stream = params['stream'];
      let source = params['source'];
      if (source) {
        this.sourceService.findOne(this.fullKey(source))
          .then((kb: KeyBean<Source>) => {
            this.key = kb.value.key;
            this.setForm(kb.value);
          });
      } else {
        this.setForm({});
      }
    })
  }

  categoryChange(event: any) {
    let value = this.formGroup.value;
    value.category = event.target.value;
    value.client = {};
    if (value.schedule) {
      value.schedule = {};
    }
    if (value.transaction) {
      value.transaction = {};
    }
    this.setForm(value);
  }

  setForm(value: any) {
    let category = value.category || 'kafka';
    let form = this.formItemService.toForm(this.sourceService.getFormItems(category), value);
    this.formItems = form.formItems;
    form.formGroup.addControl('category', new FormControl(category));
    this.formGroup = form.formGroup;
  }

  onSubmit() {
    let source = this.formGroup.value;
    this.payLoad = JSON.stringify(source);
    this.sourceService.save(this.fullKey(source.key), source)
      .then(v => this.location.back());
  }

  private fullKey(key: string) {
    return `/source/${this.ns}/${this.task}/${this.stream}/${key}`;
  }
}
