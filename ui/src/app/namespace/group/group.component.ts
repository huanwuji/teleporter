import {Component, OnInit} from "@angular/core";
import {ActivatedRoute, Router} from "@angular/router";
import {GroupService, Group} from "./group.service";
import {KeyBean} from "../../rest.servcie";
import {FormItemBase} from "../../dynamic/form/form-item";
import {FormGroup, FormControl} from "@angular/forms";
import {FormItemService} from "../../dynamic/form/form-item.service";

@Component({
  selector: 'group-list',
  templateUrl: './group-list.component.html'
})
export class GroupListComponent implements OnInit {
  private kbs: KeyBean<Group>[] = [];
  private ns: string;

  constructor(private route: ActivatedRoute, private groupService: GroupService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.list();
    });
  }

  list() {
    this.groupService.range(`/group/${this.ns}/`)
      .then((kbs: KeyBean<Group>[]) => this.kbs = kbs);
  }

  del(key: string) {
    if (window.confirm("Are you sure delete it !!!")) {
      this.groupService.remove(key).then(() => this.list());
    }
  }

  refresh(key: string) {
    this.groupService.refresh(key);
  }
}

@Component({
  selector: 'group-detail',
  templateUrl: './group-detail.component.html'
})
export class GroupDetailComponent implements OnInit {
  private formItems: FormItemBase<any>[];
  private formGroup: FormGroup = new FormGroup({"": new FormControl()});
  private payLoad: string;
  private ns: string;
  private key: string;

  constructor(private route: ActivatedRoute, private router: Router,
              public groupService: GroupService, private formItemService: FormItemService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      let group = params['group'];
      if (group) {
        this.groupService.findOne(this.fullKey(group))
          .then((kb: KeyBean<Group>) => {
            this.key = kb.value.key;
            this.setForm(kb.value);
          });
      } else {
        this.setForm({});
      }
    })
  }

  setForm(value: any) {
    let form = this.formItemService.toForm(this.groupService.getFormItems(), value);
    this.formItems = form.formItems;
    this.formGroup = form.formGroup;
  }

  preview() {
    this.payLoad = JSON.stringify(this.formGroup.value, null, '\t');
  }

  onSubmit() {
    let group = this.formGroup.value;
    this.groupService.save(this.fullKey(group.key), group)
      .then(kb => this.router.navigate([`../${group.key}`], {relativeTo: this.route}));
  }

  private fullKey(key: string) {
    return `/group/${this.ns}/${key}`;
  }
}
