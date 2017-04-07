import {Component, OnInit} from "@angular/core";
import {Task, TaskService} from "./task.service";
import {ActivatedRoute, Router} from "@angular/router";
import {KeyBean} from "../../rest.servcie";
import {FormItemBase} from "../../dynamic/form/form-item";
import {FormGroup, FormControl} from "@angular/forms";
import {FormItemService} from "../../dynamic/form/form-item.service";

@Component({
  selector: 'task-list',
  templateUrl: './task-list.component.html'
})
export class TaskListComponent implements OnInit {
  private kbs: KeyBean<Task>[] = [];
  private ns: string;

  constructor(private route: ActivatedRoute, private taskService: TaskService) {
  }

  ngOnInit(): any {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      this.list();
    });
  }

  list() {
    this.taskService.range(`/task/${this.ns}/`)
      .then((kbs: KeyBean<Task>[]) => this.kbs = kbs);
  }

  del(key: string) {
    if (window.confirm("Are you sure delete it !!!")) {
      this.taskService.remove(key).then(() => this.list());
    }
  }

  refresh(key: string) {
    this.taskService.refresh(key);
  }
}

@Component({
  selector: 'task-detail',
  templateUrl: './task-detail.component.html'
})
export class TaskDetailComponent implements OnInit {
  private formItems: FormItemBase<any>[];
  private formGroup: FormGroup = new FormGroup({"": new FormControl()});
  private payLoad: string;
  private ns: string;
  private key: string;

  constructor(private route: ActivatedRoute, private router: Router,
              private taskService: TaskService, private formItemService: FormItemService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.ns = params['ns'];
      let task = params['task'];
      if (task) {
        this.taskService.findOne(this.fullKey(task))
          .then((kb: KeyBean<Task>) => {
            this.key = kb.value.key;
            this.setForm(kb.value);
          });
      } else {
        this.setForm({});
      }
    });
  }

  setForm(value: any) {
    let form = this.formItemService.toForm(this.taskService.getFormItems(), value);
    this.formItems = form.formItems;
    this.formGroup = form.formGroup;
  }

  preview() {
    this.payLoad = JSON.stringify(this.formGroup.value, null, '\t');
  }

  onSubmit() {
    let task = this.formGroup.value;
    this.taskService.save(this.fullKey(task.key), task)
      .then(kb => this.router.navigate([`../${task.key}`], {relativeTo: this.route}));
  }

  private fullKey(key: string) {
    return `/task/${this.ns}/${key}`;
  }
}
