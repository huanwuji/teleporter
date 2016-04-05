import {Component} from 'angular2/core';
import {Http,Headers,Request,RequestOptions,RequestMethod} from 'angular2/http';
import {RouteParams,Location,ROUTER_DIRECTIVES} from 'angular2/router';
import * as Global from "../Global";
import {Task} from "../Types";
import {Props} from "./Props";

@Component({
    selector: 'task-list',
    template: `
    <form class="form-inline">
        <div class="form-group">
            <label class="sr-only" for="search">search</label>
            <input type="search" class="form-control" id="search" placeholder="like {id:3434}" [(ngModel)]="searchText">
        </div>
        <button class="btn btn-default" [routerLink]="['/TaskList',{'page':page, 'search':searchText}]">search</button>
    </form>
    <table class="table table-striped table-hover">
        <tbody>
            <tr>
                <th>id</th>
                <th>name</th>
                <th>props</th>
                <th><a class="btn btn-primary" [routerLink]="['/TaskDetailCreate']">add</a></th>
            </tr>
            <tr *ngFor="#task of tasks">
                <td>
                    <a [routerLink]="['/TaskDetailEdit', {'id':task.id}]">{{task.id}}</a>
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
            <h4 class="pull-right"><a *ngIf="page>1" [routerLink]="['/TaskList',{'page':page-1}]">pre</a></h4>
        </div>
        <div class="col-sm-10"></div>
        <div class="col-sm-1">
            <h4 class="pull-left"><a *ngIf="tasks.length==pageSize" [routerLink]="['/TaskList',{'page':page+1}]">next</a></h4>
        </div>
    </div>
    `,
    directives: [ROUTER_DIRECTIVES]
})
export class TaskList {
    page:number = 1;
    pageSize:number = Global.PAGE_SIZE;
    tasks:Task[] = [];
    searchText:string;

    constructor(public http:Http, params:RouteParams) {
        let currPage = parseInt(params.get("page")) || 1;
        this.searchText = decodeURI(params.get("search") || "{}");
        this.page = currPage;
        this.query(currPage, this.searchText);
    }

    query(page, search = "{}") {
        this.http.get(`${Global.TASK_PATH()}?page=${page}&pageSize=${this.pageSize}&search=${search}`)
            .subscribe(res => {
                this.tasks = res.json();
            }
        );
    }

    delete(id) {
        if (confirm('Are you sure you want to delete?')) {
            this.http.get(`${Global.TASK_PATH()}/${id}?action=delete`)
                .subscribe(res => {
                    console.log("delete success", res);
                    this.query(this.page);
                });
        }
    }
}

@Component({
    selector: 'task-detail',
    template: `
     <div class="form-horizontal">
        <div class="form-group">
            <label for="id" class="col-sm-3 control-label">id</label>
            <div class="col-sm-9">
                <input type="number" class="form-control" id="id" [(ngModel)]="task.id" placeholder="id" readonly/>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">name</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="task.name" placeholder="name" *ngIf="edit" readonly/>
                <input type="text" class="form-control" id="name" [(ngModel)]="task.name" placeholder="name" *ngIf="!edit"/>
            </div>
        </div>
        <div class="form-group">
            <label for="desc" class="col-sm-3 control-label">desc</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="desc" [(ngModel)]="task.desc" placeholder="desc"/>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">props</label>
            <div class="col-sm-9">
                <props-config [inputProps]="tmpProps" (outputProps)="propsChanged($event)"></props-config>
            </div>
        </div>
     </div>
    <hr/>
    <br/>
    <div class="col-sm-offset-3 col-sm-9">
        <div class="form-group">
            <button (click)="save()" class="btn btn-primary">save</button>
        </div>
    </div>
     <pre>{{propsPre}}</pre>
    `,
    directives: [Props]
})
export class TaskDetail {
    task:Task = {
        name: "",
        props: {
            streamScript: ""
        },
        desc: ""
    };
    tmpProps:any = {};
    edit:boolean = false;

    constructor(public http:Http, params:RouteParams) {
        var id = params.get("id");
        if (id) {
            this.edit = true;
            this.http.get(`${Global.TASK_PATH()}/${id}`)
                .subscribe(res => {
                    this.task = res.json();
                    this.tmpProps = this.task.props;
                });
        }
    }

    save() {
        if (this.edit) {
            this.http.post(`${Global.TASK_PATH()}/${this.task.id}`, JSON.stringify(this.task))
                .subscribe(res => console.log("update", res));
        } else {
            this.http.post(Global.TASK_PATH(), JSON.stringify(this.task))
                .subscribe(res => {
                    this.task = res.json();
                    this.tmpProps = this.task.props;
                    console.log("save", res);
                });
        }
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