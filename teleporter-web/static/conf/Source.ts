/// <reference path="../../typings/tsd.d.ts" />
import {
    Component, Injectable, View,
    NgSwitch,NgSwitchWhen,NgFor,NgIf,
    Control, ControlGroup, ControlArray,
    CORE_DIRECTIVES, FORM_DIRECTIVES
} from 'angular2/angular2';
import {Http,Headers,Request,RequestOptions,RequestMethods} from 'angular2/http';
import {RouteParams,Location,ROUTER_DIRECTIVES} from 'angular2/router';
import * as Global from "../Global";
import {Source} from "../Types";
import {KafkaSourceForm} from "./KafkaCmp";
import {DataSourceSourceForm} from "./DataSourceCmp";
import * as _ from "underscore";

@Component({
    selector: 'source-list'
})
@View ({
    template: `
    <table class="table table-striped table-hover">
        <thread>
            <tr>
                <th>id</th>
                <th>name</th>
                <th>category</th>
                <th>taskId</th>
                <th>addressId</th>
                <th>props</th>
                <th><a class="btn btn-primary" [router-link]="['/SourceDetailCreate']">add</a></th>
            </tr>
        </thread>
        <tbody>
            <tr *ng-for="#source of sources">
                <td>
                    <a [router-link]="['/SourceDetailEdit', {'id':source.id}]">{{source.id}}</a>
                </td>
                <td>{{source.name}}</td>
                <td>{{source.category}}</td>
                <td>{{source.taskId}}</td>
                <td>{{source.addressId}}</td>
                <td title="{{source.props}}">{{source.props|json|slice:0:50}}...</td>
                <td><button class="btn btn-danger" (click)="delete(source.id)">del</button></td>
            </tr>
        </tbody>
    </table>
    <hr/>
    <div class="row">
        <div class="col-sm-1">
            <h4 class="pull-right"><a *ng-if="page>1" [router-link]="['/SourceList',{'page':page-1}]">pre</a></h4>
        </div>
        <div class="col-sm-10"></div>
        <div class="col-sm-1">
            <h4 class="pull-left"><a *ng-if="sources.length==pageSize" [router-link]="['/sourceList',{'page':page+1}]">next</a></h4>
        </div>
    </div>
    `,
    directives: [
        NgSwitch, NgSwitchWhen, NgFor, NgIf,
        CORE_DIRECTIVES, FORM_DIRECTIVES, ROUTER_DIRECTIVES
    ]
})
export class SourceList {
    page:number = 1;
    pageSize:number = Global.PAGE_SIZE;
    sources:Source[] = [];

    constructor(public http:Http, params:RouteParams) {
        let currPage = parseInt(params.get("page")) || 1;
        this.page = currPage;
        this.query(currPage);
    }

    query(page) {
        this.http.get(`${Global.SOURCE_PATH}?page=${page}&pageSize=${this.pageSize}`)
            .subscribe(res => {
                this.sources = JSON.parse(res.text());
            }
        );
    }

    delete(id) {
        if (confirm('Are you sure you want to delete?')) {
            this.http.get(`${Global.SOURCE_PATH}/${id}?action=delete`)
                .subscribe(res => {
                    console.log("delete success", res);
                    this.query(this.page);
                });
        }
    }
}

@Component({
    selector: 'source-detail'
})
@View ({
    template: `
     <div class="form-inline">
        <div class="form-group">
            <label for="id">id</label>
            <input type="text" class="form-control" id="id" [(ng-model)]="source.id" placeholder="id" readonly/>
        </div>
        <div class="form-group">
            <label for="taskId">taskId</label>
            <input type="number" class="form-control" id="taskId" [(ng-model)]="source.taskId" placeholder="taskId"/>
        </div>
        <div class="form-group">
            <label for="addressId">addressId</label>
            <input type="number" class="form-control" id="addressId" [(ng-model)]="source.addressId" placeholder="addressId"/>
        </div>
        <div class="form-group">
            <label for="category">category</label>
            <select id="category" [(ng-model)]="source.category" (change)="categoryChange()">
                <option value="kafka">kafka</option>
                <option value="dataSource">dataSource</option>
                <option value="forward">forward</option>
            </select>
        </div>
        <div class="form-group">
            <label for="name">name</label>
            <input type="text" class="form-control" id="name" [(ng-model)]="source.name" placeholder="name" (change)="nameChange()" *ng-if="edit" readonly/>
            <input type="text" class="form-control" id="name" [(ng-model)]="source.name" placeholder="name" (change)="nameChange()" *ng-if="!edit"/>
        </div>
     </div>
     </br>
     <div class="form-horizontal">
        <h4>transaction</h4>
        <div class="form-group">
           <label for="transactionChannelSize" class="col-sm-3 control-label">channelSize</label>
           <div class="col-sm-9">
                <input type="number" class="form-control" id="name" [(ng-model)]="source.props.transaction.channelSize" placeholder="transactionChannelSize"/>
           </div>
        </div>
        <div class="form-group">
           <label for="transactionBatchSize" class="col-sm-3 control-label">batchSize</label>
           <div class="col-sm-9">
                <input type="number" class="form-control" id="name" [(ng-model)]="source.props.transaction.batchSize" placeholder="transactionBatchSize"/>
           </div>
        </div>
        <div class="form-group">
           <label for="transactionMaxAge" class="col-sm-3 control-label">maxAge</label>
           <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ng-model)]="source.props.transaction.maxAge" placeholder="transactionMaxAge"/>
            </div>
        </div>
        <div class="form-group">
           <label for="transactionMaxCacheSize" class="col-sm-3 control-label">maxCacheSize</label>
           <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ng-model)]="source.props.transaction.maxCacheSize" placeholder="transactionMaxCacheSize"/>
           </div>
        </div>
     </div>
     <h4>cmp</h4>
     <kafka-source *ng-if="source.category=='kafka'" [input-props]="tmpCmpProps" (output-props)="propsChanged($event)"></kafka-source>
     <data-source-source *ng-if="source.category=='dataSource'" [input-props]="tmpCmpProps" (output-props)="propsChanged($event)"></data-source-source>
     </hr>
     <div class="col-sm-offset-3 col-sm-9">
        <div class="form-group">
            <button (click)="save()" class="btn btn-primary">save</button>
        </div>
     </div>
     <pre>{{propsPre}}</pre>
    `,
    directives: [
        NgFor, NgIf,
        CORE_DIRECTIVES, FORM_DIRECTIVES, ROUTER_DIRECTIVES,
        KafkaSourceForm, DataSourceSourceForm
    ]
})
export class SourceDetail {
    source:Source = {
        category: 'kafka',
        name: '',
        props: {
            transaction: {
                channelSize: 1,
                batchSize: 10000,
                maxAge: '1.minutes',
                maxCacheSize: 100000
            },
            cmp: {}
        }
    };
    tmpCmpProps:any;
    edit:boolean = false;

    constructor(public http:Http, public location:Location, params:RouteParams) {
        let id = params.get("id");
        if (id) {
            this.edit = true;
            this.http.get(`${Global.SOURCE_PATH}/${id}`)
                .subscribe(res => {
                    this.source = JSON.parse(res.text());
                    let props = this.source.props;
                    this.tmpCmpProps = props.cmp;
                });
        }
    }

    save() {
        if (this.edit) {
            this.http.post(`${Global.SOURCE_PATH}/${this.source.id}`, JSON.stringify(this.source))
                .subscribe(res => {
                    console.log("update", res);
                    this.location.back();
                });
        } else {
            this.http.post(Global.SOURCE_PATH, JSON.stringify(this.source))
                .subscribe(res => {
                    console.log("save", res);
                    this.location.back();
                });
        }
    }

    nameChange():void {
        this.source.id = Global.hashCode(this.source.name);
    }

    categoryChange() {
        this.source.props = {};
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