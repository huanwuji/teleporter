/// <reference path="../../typings/tsd.d.ts" />
import {Component} from 'angular2/core';
import {Http,Headers,Request,RequestOptions} from 'angular2/http';
import {RouteParams,Location,ROUTER_DIRECTIVES} from 'angular2/router';
import * as Global from "../Global";
import {Source} from "../Types";
import {KafkaSourceForm} from "./Kafka";
import {DataSourceSourceForm} from "./DataSource";
import {MongoSourceForm} from "./Mongo";
import * as _ from "underscore";
import {MetricsChart}  from "../MetricsChart";
import {Autocomplete} from "../third/Autocomplete"
import {Search} from "./Search"

@Component({
    selector: 'source-list',
    template: `
    <form class="form-inline">
        <div class="form-group">
            <label class="sr-only" for="search">search</label>
            <input type="search" class="form-control" id="search" placeholder="like {id:3434}" [(ngModel)]="searchText">
        </div>
        <button class="btn btn-default" [routerLink]="['/SourceList',{'page':page, 'search':searchText}]">search</button>
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
                <th><a class="btn btn-primary" [routerLink]="['/SourceDetailCreate']">add</a></th>
            </tr>
            <tr *ngFor="#source of sources">
                <td><a [routerLink]="['/SourceDetailEdit', {'id':source.id}]">{{source.id}}</a></td>
                <td>{{source.name}}&nbsp;<span class="glyphicon glyphicon-stats" aria-hidden="true" (click)="metricsName=source.name"></span></td>
                <td>{{source.category}}</td>
                <td><a [routerLink]="['/TaskDetailEdit', {'id':source.taskId}]">{{source.taskId}}</a></td>
                <td><a [routerLink]="['/AddressDetailEdit', {'id':source.addressId}]">{{source.addressId}}</a></td>
                <td title="{{source.props|json}}">{{source.props|json|slice:0:50}}...</td>
                <td><button class="btn btn-danger" (click)="delete(source.id)">del</button></td>
            </tr>
        </tbody>
    </table>
    <hr/>
    <div class="row">
        <div class="col-sm-1">
            <h4 class="pull-right"><a *ngIf="page>1" [routerLink]="['/SourceList',{'page':page-1}]">pre</a></h4>
        </div>
        <div class="col-sm-10"></div>
        <div class="col-sm-1">
            <h4 class="pull-left"><a *ngIf="sources.length==pageSize" [routerLink]="['/SourceList',{'page':page+1}]">next</a></h4>
        </div>
    </div>
    <div class="row" *ngIf="metricsName">
        <metrics-chart [name]="metricsName"></metrics-chart>
    </div>
    `,
    directives: [ROUTER_DIRECTIVES, MetricsChart]
})
export class SourceList {
    page:number = 1;
    pageSize:number = Global.PAGE_SIZE;
    sources:Source[] = [];
    searchText:string;

    constructor(public http:Http, params:RouteParams) {
        let currPage = parseInt(params.get("page")) || 1;
        this.searchText = decodeURI(params.get("search") || "{}");
        this.page = currPage;
        this.query(currPage, this.searchText);
    }

    query(page, search = "{}") {
        this.http.get(`${Global.SOURCE_PATH()}?page=${page}&pageSize=${this.pageSize}&search=${search}`)
            .subscribe(res => {
                this.sources = res.json();
            }
        );
    }

    delete(id) {
        if (confirm('Are you sure you want to delete?')) {
            this.http.get(`${Global.SOURCE_PATH()}/${id}?action=delete`)
                .subscribe(res => {
                    console.log("delete success", res);
                    this.query(this.page);
                });
        }
    }
}

@Component({
    selector: 'source-detail',
    template: `
     <div class="form-horizontal">
        <div class="form-group">
            <label for="id" class="col-sm-3 control-label">id</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="id" [(ngModel)]="source.id" placeholder="id" readonly/>
            </div>
        </div>
        <div class="form-group">
            <label for="taskId" class="col-sm-3 control-label">taskId</label>
            <div class="col-sm-9">
                <autocomplete [initialValue]="source.taskId"
                [searchUrl]="taskSearch"
                (result)="source.taskId=$event"></autocomplete>
            </div>
        </div>
        <div class="form-group">
            <label for="streamId" class="col-sm-3 control-label">streamId</label>
            <div class="col-sm-9">
                <autocomplete [initialValue]="source.streamId"
                [searchUrl]="streamSearch"
                (result)="source.streamId=$event"></autocomplete>
            </div>
        </div>
        <div class="form-group">
            <label for="addressId" class="col-sm-3 control-label">addressId</label>
            <div class="col-sm-9">
                <autocomplete [initialValue]="source.addressId"
                [searchUrl]="addressSearch"
                (result)="source.addressId=$event"></autocomplete>
            </div>
        </div>
        <div class="form-group">
            <label for="category" class="col-sm-3 control-label">category</label>
            <div class="col-sm-9">
                <select id="category" [(ngModel)]="source.category" (change)="categoryChange()" class="form-control">
                    <option value="{{source.category}}">{{source.category}}</option>
                    <option value="kafka">kafka</option>
                    <option value="shadow:kafka">shadow:kafka</option>
                    <option value="dataSource">dataSource</option>
                    <option value="mongo">mongo</option>
                    <option value="forward">forward</option>
                </select>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">name</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="source.name" placeholder="name" *ngIf="edit" readonly/>
                <input type="text" class="form-control" id="name" [(ngModel)]="source.name" placeholder="name" *ngIf="!edit"/>
            </div>
        </div>
        <div class="form-group">
            <label for="status" class="col-sm-3 control-label">status</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="status" [(ngModel)]="source.status" placeholder="status"/>
            </div>
        </div>
     </div>
     <hr/>
     <br/>
     <div class="form-horizontal">
        <h4>transaction</h4>
        <div class="form-group">
           <label for="transactionChannelSize" class="col-sm-3 control-label">channelSize</label>
           <div class="col-sm-9">
                <input type="number" class="form-control" id="name" [(ngModel)]="source.props.transaction.channelSize" placeholder="transactionChannelSize"/>
           </div>
        </div>
        <div class="form-group">
           <label for="transactionBatchSize" class="col-sm-3 control-label">batchSize</label>
           <div class="col-sm-9">
                <input type="number" class="form-control" id="name" [(ngModel)]="source.props.transaction.batchSize" placeholder="transactionBatchSize"/>
           </div>
        </div>
        <div class="form-group">
           <label for="transactionMaxAge" class="col-sm-3 control-label">maxAge</label>
           <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="source.props.transaction.maxAge" placeholder="transactionMaxAge"/>
            </div>
        </div>
        <div class="form-group">
           <label for="transactionMaxCacheSize" class="col-sm-3 control-label">maxCacheSize</label>
           <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="source.props.transaction.maxCacheSize" placeholder="transactionMaxCacheSize"/>
           </div>
        </div>
        <div class="form-group">
           <label for="commitDelay" class="col-sm-3 control-label">commitDelay</label>
           <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="source.props.transaction.commitDelay" placeholder="1.minutes"/>
           </div>
        </div>
        <div class="form-group">
           <label for="recoveryPointEnabled" class="col-sm-3 control-label">recoveryPointEnabled</label>
           <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="source.props.transaction.recoveryPointEnabled" placeholder="true"/>
           </div>
        </div>
        <div class="form-group">
           <label for="timeoutRetry" class="col-sm-3 control-label">timeoutRetry</label>
           <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="source.props.transaction.timeoutRetry" placeholder="true"/>
           </div>
        </div>
     </div>
     <h4>cmp</h4>
     <kafka-source *ngIf="source.category=='kafka'" [inputProps]="tmpCmpProps" (outputProps)="propsChanged($event)"></kafka-source>
     <kafka-source *ngIf="source.category=='shadow:kafka'" [inputProps]="tmpCmpProps" (outputProps)="propsChanged($event)"></kafka-source>
     <data-source-source *ngIf="source.category=='dataSource'" [inputProps]="tmpCmpProps" (outputProps)="propsChanged($event)"></data-source-source>
     <mongo-source *ngIf="source.category=='mongo'" [inputProps]="tmpCmpProps" (outputProps)="propsChanged($event)"></mongo-source>
     <hr/>
     <div class="col-sm-offset-3 col-sm-9">
        <div class="form-group">
            <button (click)="save()" class="btn btn-primary">save</button>
        </div>
     </div>
     <pre>{{propsPre}}</pre>
    `,
    directives: [KafkaSourceForm, DataSourceSourceForm, MongoSourceForm, Autocomplete]
})
export class SourceDetail extends Search {
    source:Source = {
        category: 'kafka',
        name: '',
        props: {
            transaction: {
                channelSize: 1,
                batchSize: 10000,
                maxAge: '1.minutes',
                maxCacheSize: 100000,
                recoveryPointEnabled: true,
                timeoutRetry: true
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
            this.http.get(`${Global.SOURCE_PATH()}/${id}`)
                .subscribe(res => {
                    this.source = res.json();
                    this.source.props.transaction = this.source.props.transaction || {};
                    this.tmpCmpProps = this.source.props.cmp;
                });
        }
    }

    save() {
        if (this.edit) {
            this.http.post(`${Global.SOURCE_PATH()}/${this.source.id}`, JSON.stringify(this.source))
                .subscribe(res => {
                    console.log("update", res);
                    this.location.back();
                });
        } else {
            this.http.post(Global.SOURCE_PATH(), JSON.stringify(this.source))
                .subscribe(res => {
                    console.log("save", res);
                    this.source = res.json();
                    let props = this.source.props;
                    this.tmpCmpProps = props.cmp;
                });
        }
    }

    categoryChange() {
        this.source.props.cmp = {};
    }

    get propsPre() {
        if (this.source.props) {
            return JSON.stringify(this.source.props, null, '\t');
        }
    }

    private propsChanged(event:any):void {
        this.source.props.cmp = event;
    }
}