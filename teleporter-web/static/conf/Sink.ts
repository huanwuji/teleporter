/// <reference path="../../typings/tsd.d.ts" />
import {Component} from 'angular2/core';
import {Http,Headers,Request,RequestOptions} from 'angular2/http';
import {RouteParams,Location,ROUTER_DIRECTIVES} from 'angular2/router';
import * as Global from "../Global";
import {Sink} from "../Types";
import * as _ from "underscore";
import {MetricsChart} from "../MetricsChart";
import {Autocomplete} from "../third/Autocomplete"
import {Search} from "./Search"
import {DataSourceSinkForm} from "./DataSource"

@Component({
    selector: 'sink-list',
    template: `
    <form class="form-inline">
        <div class="form-group">
            <label class="sr-only" for="search">search</label>
            <input type="search" class="form-control" id="search" placeholder="like {id:3434}" [(ngModel)]="searchText">
        </div>
        <button class="btn btn-default" [routerLink]="['/SinkList',{'page':page, 'search':searchText}]">search</button>
    </form>
    <table class="table table-striped table-hover">
        <tbody>
            <tr>
                <th>id</th>
                <th>name</th>
                <th>category</th>
                <th>taskId</th>
                <th>addressId</th>
                <th>props</th>
                <th><a class="btn btn-primary" [routerLink]="['/SinkDetailCreate']">add</a></th>
            </tr>
            <tr *ngFor="#sink of sinks">
                <td>
                    <a [routerLink]="['/SinkDetailEdit', {'id':sink.id}]">{{sink.id}}</a>
                </td>
                <td>{{sink.name}}&nbsp;<span class="glyphicon glyphicon-stats" aria-hidden="true" (click)="metricsName=sink.name"></span></td>
                <td>{{sink.category}}</td>
                <td><a [routerLink]="['/TaskDetailEdit', {'id':sink.taskId}]">{{sink.taskId}}</a></td>
                <td><a [routerLink]="['/AddressDetailEdit', {'id':sink.addressId}]">{{sink.addressId}}</a></td>
                <td title="{{sink.props}}">{{sink.props|json|slice:0:50}}...</td>
                <td><button class="btn btn-danger" (click)="delete(sink.id)">del</button></td>
            </tr>
        </tbody>
    </table>
    <hr/>
    <div class="row">
        <div class="col-sm-1">
            <h4 class="pull-right"><a *ngIf="page>1" [routerLink]="['/SinkList',{'page':page-1}]">pre</a></h4>
        </div>
        <div class="col-sm-10"></div>
        <div class="col-sm-1">
            <h4 class="pull-left"><a *ngIf="sinks.length==pageSize" [routerLink]="['/SinkList',{'page':page+1}]">next</a></h4>
        </div>
    </div>
   <div class="row" *ngIf="metricsName">
        <metrics-chart [name]="metricsName"></metrics-chart>
    </div>
    `,
    directives: [ROUTER_DIRECTIVES, MetricsChart]
})
export class SinkList {
    page:number = 1;
    pageSize:number = Global.PAGE_SIZE;
    sinks:Sink[] = [];
    searchText:string;

    constructor(public http:Http, params:RouteParams) {
        let currPage = parseInt(params.get("page")) || 1;
        this.searchText = decodeURI(params.get("search") || "{}");
        this.page = currPage;
        this.query(currPage, this.searchText);
    }

    query(page, search = "{}") {
        this.http.get(`${Global.SINK_PATH()}?page=${page}&pageSize=${this.pageSize}&search=${search}`)
            .subscribe(res => {
                this.sinks = res.json();
            }
        );
    }

    delete(id) {
        if (confirm('Are you sure you want to delete?')) {
            this.http.get(`${Global.SINK_PATH()}/${id}?action=delete`)
                .subscribe(res => {
                    console.log("delete success", res);
                    this.query(this.page);
                });
        }
    }
}

@Component({
    selector: 'sink-detail',
    template: `
    <div class="form-horizontal">
        <div class="form-group">
            <label for="id" class="col-sm-3 control-label">id</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="id" [(ngModel)]="sink.id" placeholder="id" readonly/>
            </div>
        </div>
        <div class="form-group">
            <label for="taskId" class="col-sm-3 control-label">taskId</label>
            <div class="col-sm-9">
                <autocomplete [initialValue]="sink.taskId"
                [searchUrl]="taskSearch"
                (result)="sink.taskId=$event"></autocomplete>
            </div>
        </div>
        <div class="form-group">
            <label for="streamId" class="col-sm-3 control-label">streamId</label>
            <div class="col-sm-9">
                <autocomplete [initialValue]="sink.streamId"
                [searchUrl]="streamSearch"
                (result)="sink.streamId=$event"></autocomplete>
            </div>
        </div>
        <div class="form-group">
            <label for="addressId" class="col-sm-3 control-label">addressId</label>
            <div class="col-sm-9">
                <autocomplete [initialValue]="sink.addressId"
                [searchUrl]="addressSearch"
                (result)="sink.addressId=$event"></autocomplete>
            </div>
        </div>
        <div class="form-group">
            <label for="category" class="col-sm-3 control-label">category</label>
            <div class="col-sm-9">
                <select id="category" [(ngModel)]="sink.category" (change)="categoryChange()" class="form-control">
                    <option value="kafka">kafka</option>
                    <option value="dataSource">dataSource</option>
                </select>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">name</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="sink.name" placeholder="name" *ngIf="edit" readonly/>
                <input type="text" class="form-control" id="name" [(ngModel)]="sink.name" placeholder="name" *ngIf="!edit"/>
            </div>
        </div>
    </div>
     <hr/>
     <br/>
     <div class="form-horizontal">
     <!--
        <div class="form-group">
           <label for="requestStrategy" class="col-sm-3 control-label">requestStrategy</label>
           <div class="col-sm-9">
                <input type="text" class="form-control" id="requestStrategy" [(ngModel)]="sink.props.requestStrategy" placeholder="eg, like oneByOne,zero,watermark(10,5),default:watermark(100,50)"/>
           </div>
        </div>
        -->
        <div class="form-group">
           <label for="parallelism" class="col-sm-3 control-label">parallelism</label>
           <div class="col-sm-9">
                <input type="number" class="form-control" id="parallelism" [(ngModel)]="sink.props.subscriber.parallelism" placeholder="1"/>
           </div>
        </div>
        <div class="form-group">
           <label for="prototype" class="col-sm-3 control-label">prototype</label>
           <div class="col-sm-9">
                <input type="text" class="form-control" id="prototype" [(ngModel)]="sink.props.prototype" placeholder="true | false"/>
           </div>
        </div>
     </div>
     <h4>cmp</h4>
     <data-source-sink *ngIf="sink.category=='dataSource'" [inputProps]="tmpCmpProps" (outputProps)="propsChanged($event)"></data-source-sink>
     <div class="col-sm-offset-3 col-sm-9">
        <div class="form-group">
            <button (click)="save()" class="btn btn-primary">save</button>
        </div>
     </div>
     <pre>{{propsPre}}</pre>
    `,
    directives: [Autocomplete, DataSourceSinkForm]
})
export class SinkDetail extends Search {
    sink:Sink = {
        category: 'kafka', name: '', props: {
            subscriber: {
                parallelism: 1
            },
            cmp: {}
        }
    };
    tmpCmpProps:any;
    edit:boolean = false;

    constructor(public http:Http, public location:Location, params:RouteParams) {
        super();
        let id = params.get("id");
        if (id) {
            this.edit = true;
            this.http.get(`${Global.SINK_PATH()}/${id}`)
                .subscribe(res => {
                    this.sink = res.json();
                    let props = this.sink.props;
                    if (!this.sink.props.subscriber) {
                        this.sink.props.subscriber = {};
                    }
                    this.tmpCmpProps = props.cmp
                });
        }
    }

    save() {
        if (this.edit) {
            this.http.post(`${Global.SINK_PATH()}/${this.sink.id}`, JSON.stringify(this.sink))
                .subscribe(res => {
                    console.log("update", res);
                    this.location.back();
                });
        } else {
            this.http.post(Global.SINK_PATH(), JSON.stringify(this.sink))
                .subscribe(res => {
                    this.sink = res.json();
                    let props = this.sink.props;
                    this.tmpCmpProps = props.cmp
                });
        }
    }

    categoryChange() {
        this.sink.props = {};
    }

    get propsPre() {
        if (this.sink.props) {
            return JSON.stringify(this.sink.props, null, '\t');
        }
    }

    private propsChanged(event:any):void {
        this.sink.props.cmp = event;
    }
}