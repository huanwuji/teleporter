import {Component, OnInit} from "@angular/core";
import {ActivatedRoute} from "@angular/router";
import {Location} from "@angular/common";
import {PartitionService, Partition, RuntimePartitionService} from "./partition.service";
import {KeyBean} from "../../../rest.servcie";
import {FormItemBase} from "../../../dynamic/form/form-item";
import {FormControl, FormGroup} from "@angular/forms";
import {FormItemService} from "../../../dynamic/form/form-item.service";

@Component({
  selector: 'partition-list',
  templateUrl: './partition-list.component.html'
})
export class PartitionListComponent implements OnInit {
  private kbs: KeyBean<Partition>[] = [];
  private ns: string;
  private task: string;

  constructor(private partitionService: PartitionService, private runtimePartitionService: RuntimePartitionService, private route: ActivatedRoute) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.task = params['task'];
      if (this.task) {
        this.list();
      }
    })
  }

  list() {
    this.partitionService.range(`/partition/${this.ns}/${this.task}`, 0, 2000)
      .then(kbs =>this.kbs = kbs);
  }

  runtime(i: number, key: string) {
    this.runtimePartitionService.findOne(key)
      .then(kb => this.kbs[i].value.runtime = kb.value);
  }

  del(key: string) {
    if (window.confirm("Are you sure delete it !!!")) {
      this.partitionService.remove(key).then(() => this.list());
      this.runtimePartitionService.remove(key);
    }
  }

  refresh(key: string) {
    this.partitionService.refresh(key);
  }
}

@Component({
  selector: 'partition-detail',
  templateUrl: './partition-detail.component.html'
})
export class PartitionDetailComponent implements OnInit {
  private formItems: FormItemBase<any>[];
  private formGroup: FormGroup = new FormGroup({"": new FormControl()});
  private payLoad: string;
  private ns: string;
  private task: string;
  private key: string;

  constructor(private route: ActivatedRoute, private location: Location,
              private partitionService: PartitionService, private formItemService: FormItemService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.task = params['task'];
      let partition = params['partition'];
      if (partition) {
        this.partitionService.findOne(this.fullKey(partition))
          .then((kb: KeyBean<Partition>) => {
            this.key = kb.value.key;
            this.setForm(kb.value);
          });
      } else {
        this.setForm({});
      }
    })
  }

  setForm(value: any) {
    let form = this.formItemService.toForm(this.partitionService.getFormItems(), value);
    this.formItems = form.formItems;
    this.formGroup = form.formGroup;
  }

  onSubmit() {
    let partition = this.formGroup.value;
    this.payLoad = JSON.stringify(partition);
    this.partitionService.save(this.fullKey(partition), partition)
      .then(v => this.location.back());
  }

  protected fullKey(key: string) {
    return `/partition/${this.ns}/${this.task}/${key}`;
  }
}
