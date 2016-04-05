import {
    Component, Injectable, View,
    NgSwitch,NgSwitchWhen,NgFor,NgIf,
    Control, ControlGroup, ControlArray,
    CORE_DIRECTIVES, FORM_DIRECTIVES
} from 'angular2/angular2';
import {Http,Headers,Request,RequestOptions,RequestMethods} from 'angular2/http';
import {RouteParams,Location,ROUTER_DIRECTIVES} from 'angular2/router';
import * as Global from "../Global";
import {PropsCmp} from './PropsCmp';
import {Task} from "../Types";
@Component({
    selector: 'task-list'
})
@View ({
    template: `
    <table class="table table-striped table-hover">
        <thread>
            <tr>
                <th>id</th>
                <th>name</th>
                <th>props</th>
                <th><a class="btn btn-primary" [router-link]="['/TaskDetailCreate']">add</a></th>
            </tr>
        </thread>
        <tbody>
            <tr *ng-for="#task of tasks">
                <td>
                    <a [router-link]="['/TaskDetailEdit', {'id':task.id}]">{{task.id}}</a>
                </td>
                <td>{{task.name}}</td>
                <td title="{{task.props}}">{{task.props|json|slice:0:50}}...</td>
                <td><button class="btn btn-danger" (click)="delete(task.id)">del</button></td>
            </tr>
        </tbody>
    </table>
    <hr/>
    <div class="row">
        <div class="col-sm-1">
            <h4 class="pull-right"><a *ng-if="page>1" [router-link]="['/TaskList',{'page':page-1}]">pre</a></h4>
        </div>
        <div class="col-sm-10"></div>
        <div class="col-sm-1">
            <h4 class="pull-left"><a *ng-if="tasks.length==pageSize" [router-link]="['/TaskList',{'page':page+1}]">next</a></h4>
        </div>
    </div>
    `,
    directives: [
        NgSwitch, NgSwitchWhen, NgFor, NgIf,
        CORE_DIRECTIVES, FORM_DIRECTIVES, ROUTER_DIRECTIVES
    ]
})
export class TaskList {
    page:number = 1;
    pageSize:number = Global.PAGE_SIZE;
    tasks:Task[] = [];

    constructor(public http:Http, params:RouteParams) {
        let currPage = parseInt(params.get("page")) || 1;
        this.page = currPage;
        this.query(currPage);
    }

    query(page) {
        this.http.get(`${Global.TASK_PATH}?page=${page}&pageSize=${this.pageSize}`)
            .subscribe(res => {
                this.tasks = JSON.parse(res.text());
            }
        );
    }

    delete(id) {
        if (confirm('Are you sure you want to delete?')) {
            this.http.get(`${Global.TASK_PATH}/${id}?action=delete`)
                .subscribe(res => {
                    console.log("delete success", res);
                    this.query(this.page);
                });
        }
    }
}

@Component({
    selector: 'task-detail'
})
@View ({
    template: `
     <div class="form-horizontal">
        <div class="form-group">
            <label for="id" class="col-sm-3 control-label">id</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="id" [(ng-model)]="task.id" placeholder="id" readonly/>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">name</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ng-model)]="task.name" placeholder="name" (change)="nameChange()" *ng-if="edit" readonly/>
                <input type="text" class="form-control" id="name" [(ng-model)]="task.name" placeholder="name" (change)="nameChange()" *ng-if="!edit"/>
            </div>
        </div>
        <div class="form-group">
            <label for="desc" class="col-sm-3 control-label">desc</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="desc" [(ng-model)]="task.desc" placeholder="desc"/>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">props</label>
            <div class="col-sm-9">
                <props-config [(input-props)]="tmpProps" (output-props)="propsChanged($event)"></props-config>
            </div>
        </div>
        </br>
        <div class="col-sm-offset-3 col-sm-9">
            <div class="form-group">
                <button (click)="save()" class="btn btn-primary">save</button>
            </div>
        </div>
     </div>
     <pre>{{propsPre}}</pre>
    `,
    directives: [
        NgFor, NgIf,
        CORE_DIRECTIVES, FORM_DIRECTIVES, ROUTER_DIRECTIVES,
        PropsCmp
    ]
})
export class TaskDetail {
    task:Task = {name: '', props: {}, desc: ''};
    tmpProps:any = {};
    edit:boolean = false;

    constructor(public http:Http, public location:Location, params:RouteParams) {
        var id = params.get("id");
        if (id) {
            this.edit = true;
            this.http.get(`${Global.TASK_PATH}/${id}`)
                .subscribe(res => {
                    this.task = JSON.parse(res.text());
                    this.tmpProps = this.task.props;
                });
        }
    }

    save() {
        if (this.edit) {
            this.http.post(`${Global.TASK_PATH}/${this.task.id}`, JSON.stringify(this.task))
                .subscribe(res => console.log("update", res));
        } else {
            this.http.post(Global.TASK_PATH, JSON.stringify(this.task))
                .subscribe(res => {
                    console.log("save", res);
                    this.location.back();
                });
        }
    }

    nameChange():void {
        this.task.id = Global.hashCode(this.task.name);
    }

    get propsPre() {
        if (this.task.props) {
            return JSON.stringify(this.task.props, null, '\t');
        }
    }

    private propsChanged(event:any):void {
        this.task.props = event;
    }
}