import {Component} from 'angular2/core';
import {Http,Headers,Request,RequestOptions,RequestMethod} from 'angular2/http';
import {RouteParams,Location,ROUTER_DIRECTIVES} from 'angular2/router';
import * as Global from "../Global";
import {Stream} from "../Types";
import {Props} from "./Props";
import {Autocomplete} from "../third/Autocomplete"
import {Search} from "./Search"

@Component({
    selector: 'stream-list',
    template: `
    <form class="form-inline">
        <div class="form-group">
            <label class="sr-only" for="search">search</label>
            <input type="search" class="form-control" id="search" placeholder="like {id:3434}" [(ngModel)]="searchText">
        </div>
        <button class="btn btn-default" [routerLink]="['/StreamList',{'page':page, 'search':searchText}]">search</button>
    </form>
    <table class="table table-striped table-hover">
        <tbody>
            <tr>
                <th>id</th>
                <th>taskId</th>
                <th>name</th>
                <th>props</th>
                <th><a class="btn btn-primary" [routerLink]="['/StreamDetailCreate']">add</a></th>
            </tr>
            <tr *ngFor="#stream of streams">
                <td><a [routerLink]="['/StreamDetailEdit', {'id':stream.id}]">{{stream.id}}</a></td>
                <td><a [routerLink]="['/TaskDetailEdit', {'id':stream.taskId}]">{{stream.taskId}}</a></td>
                <td>{{stream.name}}</td>
                <td title="{{stream.props}}">{{stream.props|json|slice:0:50}}...</td>
                <td><button class="btn btn-danger" (click)="delete(stream.id)">del</button></td>
            </tr>
        </tbody>
    </table>
    <hr/>
    <div class="row">
        <div class="col-sm-1">
            <h4 class="pull-right"><a *ngIf="page>1" [routerLink]="['/StreamList',{'page':page-1}]">pre</a></h4>
        </div>
        <div class="col-sm-10"></div>
        <div class="col-sm-1">
            <h4 class="pull-left"><a *ngIf="streams.length==pageSize" [routerLink]="['/StreamList',{'page':page+1}]">next</a></h4>
        </div>
    </div>
    `,
    directives: [ROUTER_DIRECTIVES]
})
export class StreamList {
    page:number = 1;
    pageSize:number = Global.PAGE_SIZE;
    streams:Stream[] = [];
    searchText:string;

    constructor(public http:Http, params:RouteParams) {
        let currPage = parseInt(params.get("page")) || 1;
        this.searchText = decodeURI(params.get("search") || "{}");
        this.page = currPage;
        this.query(currPage, this.searchText);
    }

    query(page, search = "{}") {
        this.http.get(`${Global.STREAM_PATH()}?page=${page}&pageSize=${this.pageSize}&search=${search}`)
            .subscribe(res => {
                this.streams = res.json();
            }
        );
    }

    delete(id) {
        if (confirm('Are you sure you want to delete?')) {
            this.http.get(`${Global.STREAM_PATH()}/${id}?action=delete`)
                .subscribe(res => {
                    console.log("delete success", res);
                    this.query(this.page);
                });
        }
    }
}

@Component({
    selector: 'stream-detail',
    template: `
     <div class="form-horizontal">
        <div class="form-group">
            <label for="id" class="col-sm-3 control-label">id</label>
            <div class="col-sm-9">
                <input type="number" class="form-control" id="id" [(ngModel)]="stream.id" placeholder="id" readonly/>
            </div>
        </div>
        <div class="form-group">
            <label for="id" class="col-sm-3 control-label">taskId</label>
            <div class="col-sm-9">
                <autocomplete [initialValue]="stream.taskId"
                [searchUrl]="taskSearch"
                (result)="stream.taskId=$event"></autocomplete>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">name</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="stream.name" placeholder="name" *ngIf="edit" readonly/>
                <input type="text" class="form-control" id="name" [(ngModel)]="stream.name" placeholder="name" *ngIf="!edit"/>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">status</label>
            <div class="col-sm-9">
               <select id="status" [(ngModel)]="stream.status" class="form-control">
                    <option value="VALID">VALID</option>
                    <option value="INVALID">INVALID</option>
                    <option value="RUNNING">RUNNING</option>
                    <option value="ERROR">ERROR</option>
                    <option value="COMPLETE">COMPLETE</option>
                </select>
            </div>
        </div>
        <div class="form-group">
            <label for="desc" class="col-sm-3 control-label">desc</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="desc" [(ngModel)]="stream.desc" placeholder="desc"/>
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
    directives: [Props, Autocomplete]
})
export class StreamDetail extends Search {
    stream:Stream = {
        name: "",
        props: {
            streamDef: "",
            maxParallelismSource: "",
        },
        desc: ""
    };
    tmpProps:any;
    edit:boolean = false;

    constructor(public http:Http, params:RouteParams) {
        super();
        var id = params.get("id");
        if (id) {
            this.edit = true;
            this.http.get(`${Global.STREAM_PATH()}/${id}`)
                .subscribe(res => {
                    this.stream = res.json();
                    this.tmpProps = this.stream.props;
                });
        } else {
            this.tmpProps = this.stream.props;
        }
    }

    save() {
        if (this.edit) {
            this.http.post(`${Global.STREAM_PATH()}/${this.stream.id}`, JSON.stringify(this.stream))
                .subscribe(res => console.log("update", res));
        } else {
            this.http.post(Global.STREAM_PATH(), JSON.stringify(this.stream))
                .subscribe(res => {
                    this.stream = res.json();
                    this.tmpProps = this.stream.props;
                    console.log("save", res);
                });
        }
    }

    get propsPre() {
        if (this.stream.props) {
            return JSON.stringify(this.stream.props, null, '\t');
        }
    }

    private propsChanged(event:any):void {
        this.stream.props = event;
    }
}