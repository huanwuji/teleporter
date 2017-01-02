import {Component, OnInit} from "@angular/core";
import {ActivatedRoute, Router} from "@angular/router";
import {AddressService, Address, RuntimeAddressService, RuntimeAddress} from "./address.service";
import {KeyBean} from "../../rest.servcie";
import {FormItemService} from "../../dynamic/form/form-item.service";
import {FormItemBase} from "../../dynamic/form/form-item";
import {FormGroup, FormControl} from "@angular/forms";

@Component({
  selector: 'address-list',
  templateUrl: './address-list.component.html'
})
export class AddressListComponent implements OnInit {
  private kbs: KeyBean<Address>[] = [];
  private ns: string;
  private searchRegex: string = "";
  private page: number = 0;
  private pageSize: number = 20;

  constructor(private addressService: AddressService, private runtimeAddressService: RuntimeAddressService, private route: ActivatedRoute) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.page = parseInt(params['page'] || 0);
      this.list();
    })
  }

  list() {
    this.addressService.range(`/address/${this.ns}/${this.searchRegex}`, this.page, this.pageSize)
      .then((kbs: KeyBean<Address>[]) => this.kbs = kbs);
  }

  runtime(i: number, key: string) {
    this.runtimeAddressService.findOne(key)
      .then((kb: KeyBean<RuntimeAddress>) => this.kbs[i].value.runtime = kb.value);
  }

  del(key: string) {
    if (window.confirm("Are you sure delete it !!!")) {
      this.addressService.remove(key).then(() => this.list());
      this.runtimeAddressService.remove(key);
    }
  }

  refresh(key: string) {
    this.addressService.refresh(key);
  }
}

@Component({
  selector: 'address-detail',
  templateUrl: './address-detail.component.html'
})
export class AddressDetailComponent implements OnInit {
  private formItems: FormItemBase<any>[];
  private formGroup: FormGroup = new FormGroup({"": new FormControl()});
  private payLoad: string;
  private ns: string;
  private key: string;

  constructor(private route: ActivatedRoute, private router: Router,
              private addressService: AddressService, private formItemService: FormItemService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      let address = params['address'];
      if (address) {
        this.addressService.findOne(this.fullKey(address))
          .then((kb: KeyBean<Address>) => {
            this.key = kb.value.key;
            this.setForm(kb.value);
          });
      } else {
        this.setForm({});
      }
    });
  }

  categoryChange(event: any) {
    let value = this.formGroup.value;
    value.category = event.target.value;
    value.client = {};
    this.setForm(value);
  }

  setForm(value: any) {
    let category = value.category || 'kafka_consumer';
    let form = this.formItemService.toForm(this.addressService.getFormItems(category), value);
    this.formItems = form.formItems;
    form.formGroup.addControl('category', new FormControl(category));
    this.formGroup = form.formGroup;
  }

  preview() {
    this.payLoad = JSON.stringify(this.formGroup.value, null, '\t');
  }

  onSubmit() {
    let address = this.formGroup.value;
    this.addressService.save(this.fullKey(address.key), address)
      .then(kb => this.router.navigate([`../${address.key}`], {relativeTo: this.route}));
  }

  private fullKey(key: string) {
    return `/address/${this.ns}/${key}`;
  }
}
