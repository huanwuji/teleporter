import {Component, OnInit} from "@angular/core";
import {StreamService, Stream} from "./stream.service";
import {ActivatedRoute, Router} from "@angular/router";
import {KeyBean} from "../../../rest.servcie";
import {FormItemService} from "../../../dynamic/form/form-item.service";
import {FormItemBase} from "../../../dynamic/form/form-item";
import {FormGroup, FormControl} from "@angular/forms";

@Component({
  selector: 'stream-list',
  templateUrl: './stream-list.component.html'
})
export class StreamListComponent implements OnInit {
  private kbs: KeyBean<Stream>[] = [];
  private ns: string;
  private task: string;

  constructor(private streamService: StreamService, private route: ActivatedRoute) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.task = params['task'];
      if (this.task) {
        this.list()
      }
    })
  }

  private list() {
    this.streamService.range(`/stream/${this.ns}/${this.task}/`)
      .then(kbs => this.kbs = kbs);
  }

  del(key: string) {
    if (window.confirm("Are you sure delete it !!!")) {
      this.streamService.remove(key).then(() => this.list());
    }
  }

  refresh(key: string) {
    this.streamService.refresh(key);
  }
}

@Component({
  selector: 'stream-detail',
  templateUrl: './stream-detail.component.html'
})
export class StreamDetailComponent implements OnInit {
  private formItems: FormItemBase<any>[];
  private formGroup: FormGroup = new FormGroup({"": new FormControl()});
  private payLoad: string;
  private ns: string;
  private task: string;
  private key: string;

  constructor(private route: ActivatedRoute, private router: Router,
              private streamService: StreamService, private formItemService: FormItemService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.task = params['task'];
      let stream = params['stream'];
      if (stream) {
        this.streamService.findOne(this.fullKey(stream))
          .then((kb: KeyBean<Stream>) => {
            this.key = kb.value.key;
            this.setForm(kb.value);
          });
      } else {
        this.setForm({});
      }
    })
  }

  setForm(value: any) {
    let form = this.formItemService.toForm(this.streamService.getFormItems(), value);
    this.formItems = form.formItems;
    this.formGroup = form.formGroup;
  }

  preview() {
    this.payLoad = JSON.stringify(this.formGroup.value, null, '\t');
  }

  onSubmit() {
    let stream = this.formGroup.value;
    this.streamService.save(this.fullKey(stream.key), stream)
      .then(kb => this.router.navigate([`../${stream.key}`], {relativeTo: this.route}));
  }

  protected fullKey(key: string) {
    return `/stream/${this.ns}/${this.task}/${key}`;
  }
}
