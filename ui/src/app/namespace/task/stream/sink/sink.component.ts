import {Component, OnInit} from "@angular/core";
import {ActivatedRoute, Router} from "@angular/router";
import {SinkService, Sink} from "./sink.service";
import {KeyBean} from "../../../../rest.servcie";
import {FormItemBase} from "../../../../dynamic/form/form-item";
import {FormGroup, FormControl} from "@angular/forms";
import {FormItemService} from "../../../../dynamic/form/form-item.service";

@Component({
  selector: 'sink-list',
  templateUrl: './sink-list.component.html'
})
export class SinkListComponent implements OnInit {
  private kbs: KeyBean<Sink>[] = [];
  private ns: string;
  private task: string;
  private stream: string;

  constructor(private sinkService: SinkService, private route: ActivatedRoute) {
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

  list() {
    this.sinkService.range(`/sink/${this.ns}/${this.task}/${this.stream}`)
      .then(kbs => this.kbs = kbs);
  }

  del(key: string) {
    if (window.confirm("Are you sure delete it !!!")) {
      this.sinkService.remove(key).then(() => this.list());
    }
  }

  refresh(key: string) {
    this.sinkService.refresh(key);
  }
}

@Component({
  selector: 'sink-detail',
  templateUrl: './sink-detail.component.html'
})
export class SinkDetailComponent implements OnInit {
  private formItems: FormItemBase<any>[];
  private formGroup: FormGroup = new FormGroup({"": new FormControl()});
  private payLoad: string;
  private ns: string;
  private task: string;
  private stream: string;
  private key: string;

  constructor(private route: ActivatedRoute, private router: Router,
              public sinkService: SinkService, private formItemService: FormItemService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.task = params['task'];
      this.stream = params['stream'];
      let sink = params['sink'];
      if (sink) {
        this.sinkService.findOne(this.fullKey(sink))
          .then((kb: KeyBean<Sink>) => {
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
    this.setForm(value);
  }

  setForm(value: any) {
    let category = value.category || 'kafka';
    let form = this.formItemService.toForm(this.sinkService.getFormItems(category), value);
    this.formItems = form.formItems;
    form.formGroup.addControl('category', new FormControl(category));
    this.formGroup = form.formGroup;
  }

  preview() {
    this.payLoad = JSON.stringify(this.formGroup.value, null, '\t');
  }

  onSubmit() {
    let sink = this.formGroup.value;
    this.sinkService.save(this.fullKey(sink.key), sink)
      .then(kb => this.router.navigate([`../${sink.key}`], {relativeTo: this.route}));
  }

  private fullKey(key: string) {
    return `/sink/${this.ns}/${this.task}/${this.stream}/${key}`;
  }
}
