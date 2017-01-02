import {Component, OnInit} from "@angular/core";
import {ActivatedRoute, Router} from "@angular/router";
import {NamespaceService, Namespace} from "./namespace.service";
import {KeyBean} from "../rest.servcie";
import {FormItemBase} from "../dynamic/form/form-item";
import {FormGroup, FormControl} from "@angular/forms";
import {FormItemService} from "../dynamic/form/form-item.service";

@Component({
  selector: 'namespace-list',
  templateUrl: './namespace-list.component.html'
})
export class NamespaceListComponent implements OnInit {
  private kbs: KeyBean<Namespace>[] = [];

  constructor(private namespaceService: NamespaceService) {
  }

  ngOnInit() {
    this.list();
  }

  list() {
    this.namespaceService.range('/ns')
      .then(kbs => this.kbs = kbs);
  }

  del(key: string) {
    this.namespaceService.remove(key).then(() => this.list());
  }
}

@Component({
  selector: 'namespace-detail',
  templateUrl: './namespace-detail.component.html'
})
export class NamespaceDetailComponent implements OnInit {
  private formItems: FormItemBase<any>[];
  private formGroup: FormGroup = new FormGroup({"": new FormControl()});
  private payLoad: string;
  private key: string;

  constructor(private route: ActivatedRoute, private router: Router,
              private namespaceService: NamespaceService, private formItemService: FormItemService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      let ns = params['ns'];
      if (ns) {
        this.namespaceService.findOne(this.fullKey(ns))
          .then((kb: KeyBean<Namespace>) => {
            this.key = kb.value.key;
            this.setForm(kb.value);
          });
      } else {
        this.setForm({});
      }
    });
  }

  setForm(value: any) {
    let form = this.formItemService.toForm(this.namespaceService.getFormItems(), value);
    this.formItems = form.formItems;
    this.formGroup = form.formGroup;
  }

  preview() {
    this.payLoad = JSON.stringify(this.formGroup.value, null, '\t');
  }

  onSubmit() {
    let namespace = this.formGroup.value;
    this.namespaceService.save(this.fullKey(namespace.key), namespace)
      .then(kb => this.router.navigate([`../${namespace.key}`], {relativeTo: this.route}));
  }

  private fullKey(key: string) {
    return `/ns/${key}`;
  }
}
