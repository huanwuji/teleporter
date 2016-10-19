import {Component, OnInit} from "@angular/core";
import {ActivatedRoute} from "@angular/router";
import {Location} from "@angular/common";
import {BrokerService, Broker} from "./broker.service";
import {KeyBean} from "../../rest.servcie";
import {FormGroup, FormControl} from "@angular/forms";
import {FormItemBase} from "../../dynamic/form/form-item";
import {FormItemService} from "../../dynamic/form/form-item.service";

@Component({
  selector: 'broker-list',
  templateUrl: './broker-list.component.html'
})
export class BrokerListComponent implements OnInit {
  private kbs: KeyBean<Broker>[] = [];
  private ns: string;

  constructor(private route: ActivatedRoute, private brokerService: BrokerService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.list();
    });
  }

  list() {
    this.brokerService.range(`/broker/${this.ns}`, 0, 2000)
      .then((kbs: KeyBean<Broker>[]) => this.kbs = kbs);
  }

  del(key: string) {
    if (window.confirm("Are you sure delete it !!!")) {
      this.brokerService.remove(key).then(() => this.list());
    }
  }

  refresh(key: string) {
    this.brokerService.refresh(key);
  }
}

@Component({
  selector: 'broker-detail',
  templateUrl: './broker-detail.component.html'
})
export class BrokerDetailComponent implements OnInit {
  private formItems: FormItemBase<any>[];
  private formGroup: FormGroup = new FormGroup({"": new FormControl()});
  private payLoad: string;
  private ns: string;
  private key: string;

  constructor(private route: ActivatedRoute, private location: Location,
              private brokerService: BrokerService, private formItemService: FormItemService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      let broker = params['broker'];
      if (broker) {
        this.brokerService.findOne(this.fullKey(broker))
          .then((kb: KeyBean<Broker>) => {
            this.key = kb.value.key;
            this.setForm(kb.value);
          });
      } else {
        this.setForm({});
      }
    })
  }

  setForm(value: any) {
    let form = this.formItemService.toForm(this.brokerService.getFormItems(), value);
    this.formItems = form.formItems;
    this.formGroup = form.formGroup;
  }

  save() {
    let broker = this.formGroup.value;
    this.payLoad = JSON.stringify(broker);
    this.brokerService.save(this.fullKey(broker.key), broker)
      .then(v => this.location.back());
  }

  private fullKey(key: string) {
    return `/broker/${this.ns}/${key}`;
  }
}
