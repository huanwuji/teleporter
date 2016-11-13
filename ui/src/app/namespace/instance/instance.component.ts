import {Component, OnInit} from "@angular/core";
import {ActivatedRoute} from "@angular/router";
import {Location} from "@angular/common";
import {InstanceService, Instance, RuntimeInstanceService} from "./instance.service";
import {KeyBean} from "../../rest.servcie";
import {FormItemService} from "../../dynamic/form/form-item.service";
import {FormControl, FormGroup} from "@angular/forms";
import {FormItemBase} from "../../dynamic/form/form-item";

@Component({
  selector: 'instance-list',
  templateUrl: './instance-list.component.html'
})
export class InstanceListComponent implements OnInit {
  private kbs: KeyBean<Instance>[] = [];
  private ns: string;

  constructor(private route: ActivatedRoute, private instanceService: InstanceService, private runtimeInstanceService: RuntimeInstanceService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.list();
    });
  }

  list() {
    this.instanceService.range(`/instance/${this.ns}`, 0, 2000)
      .then(kbs => this.kbs = kbs);
  }

  del(key: string) {
    if (window.confirm("Are you sure delete it !!!")) {
      this.instanceService.remove(key).then(() => this.list());
    }
  }

  refresh(key: string) {
    this.instanceService.refresh(key);
  }

  runtime(i: number, key: string) {
    this.runtimeInstanceService.findOne(key)
      .then(kb => this.kbs[i].value.runtime = kb.value);
  }

  executorCmd(instance: string) {
    var ws = new WebSocket(`ws://${window.location.host}/log?instance=${instance}`);
    let requests = 10;
    let requestLogs = () => {
      while (requests > 0) {
        ws.send("echo test");
        requests--;
      }
      setTimeout(requestLogs, 1000);
    };
    ws.onopen = (event: Event)=> {
      console.log("trace will open", event);
      requestLogs();
    };
    ws.onmessage = (message: MessageEvent) => {
      requests++;
      console.log(message.data);
    }
  }
}

@Component({
  selector: 'instance-detail',
  templateUrl: './instance-detail.component.html'
})
export class InstanceDetailComponent implements OnInit {
  private formItems: FormItemBase<any>[];
  private formGroup: FormGroup = new FormGroup({"": new FormControl()});
  private payLoad: string;
  private ns: string;
  private key: string;

  constructor(private route: ActivatedRoute, private location: Location,
              private instanceService: InstanceService, private formItemService: FormItemService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      let instance = params['instance'];
      if (instance) {
        this.instanceService.findOne(this.fullKey(instance))
          .then((kb: KeyBean<Instance>) => {
            this.key = kb.value.key;
            this.setForm(kb.value);
          });
      } else {
        this.setForm({});
      }
    })
  }

  setForm(value: any) {
    let form = this.formItemService.toForm(this.instanceService.getFormItems(), value);
    this.formItems = form.formItems;
    this.formGroup = form.formGroup;
  }

  preview() {
    this.payLoad = JSON.stringify(this.formGroup.value, null, '\t');
  }

  onSubmit() {
    let instance = this.formGroup.value;
    this.instanceService.save(this.fullKey(instance.key), instance)
      .then(v => this.location.back());
  }

  private fullKey(key: string) {
    return `/instance/${this.ns}/${key}`;
  }
}
