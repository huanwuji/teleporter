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
import {Sink} from "../Types";
import * as _ from "underscore";

@Component({
    selector: 'sink-list'
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
                <th><a class="btn btn-primary" [router-link]="['/SinkDetailCreate']">add</a></th>
            </tr>
        </thread>
        <tbody>
            <tr *ng-for="#sink of sinks">
                <td>
                    <a [router-link]="['/SinkDetailEdit', {'id':sink.id}]">{{sink.id}}</a>
                </td>
                <td>{{sink.name}}</td>
                <td>{{sink.category}}</td>
                <td>{{sink.taskId}}</td>
                <td>{{sink.addressId}}</td>
                <td title="{{sink.props}}">{{sink.props|json|slice:0:50}}...</td>
                <td><button class="btn btn-danger" (click)="delete(sink.id)">del</button></td>
            </tr>
        </tbody>
    </table>
    <hr/>
    <div class="row">
        <div class="col-sm-1">
            <h4 class="pull-right"><a *ng-if="page>1" [router-link]="['/SinkList',{'page':page-1}]">pre</a></h4>
        </div>
        <div class="col-sm-10"></div>
        <div class="col-sm-1">
            <h4 class="pull-left"><a *ng-if="sinks.length==pageSize" [router-link]="['/sinkList',{'page':page+1}]">next</a></h4>
        </div>
    </div>
    `,
    directives: [
        NgSwitch, NgSwitchWhen, NgFor, NgIf,
        CORE_DIRECTIVES, FORM_DIRECTIVES, ROUTER_DIRECTIVES
    ]
})
export class SinkList {
    page:number = 1;
    pageSize:number = Global.PAGE_SIZE;
    sinks:Sink[] = [];

    constructor(public http:Http, params:RouteParams) {
        let currPage = parseInt(params.get("page")) || 1;
        this.page = currPage;
        this.query(currPage);
    }

    query(page) {
        this.http.get(`${Global.SINK_PATH}?page=${page}&pageSize=${this.pageSize}`)
            .subscribe(res => {
                this.sinks = JSON.parse(res.text());
            }
        );
    }

    delete(id) {
        if (confirm('Are you sure you want to delete?')) {
            this.http.get(`${Global.SINK_PATH}/${id}?action=delete`)
                .subscribe(res => {
                    console.log("delete success", res);
                    this.query(this.page);
                });
        }
    }
}

@Component({
    selector: 'sink-detail'
})
@View ({
    template: `
     <div class="form-inline">
        <div class="form-group">
            <label for="id">id</label>
            <input type="text" class="form-control" id="id" [(ng-model)]="sink.id" placeholder="id" readonly/>
        </div>
        <div class="form-group">
            <label for="taskId">taskId</label>
            <input type="number" class="form-control" id="task" [(ng-model)]="sink.taskId" placeholder="taskId"/>
        </div>
        <div class="form-group">
            <label for="addressId">addressId</label>
            <input type="number" class="form-control" id="task" [(ng-model)]="sink.addressId" placeholder="addressId"/>
        </div>
        <div class="form-group">
            <label for="category">category</label>
            <select id="category" [(ng-model)]="sink.category" (change)="categoryChange()">
                <option value="kafka">kafka</option>
                <option value="dataSource">dataSource</option>
            </select>
        </div>
        <div class="form-group">
            <label for="name">name</label>
            <input type="text" class="form-control" id="name" [(ng-model)]="sink.name" placeholder="name" (change)="nameChange()" *ng-if="edit" readonly/>
            <input type="text" class="form-control" id="name" [(ng-model)]="sink.name" placeholder="name" (change)="nameChange()" *ng-if="!edit"/>
        </div>
     </div>
     </br>
     </hr>
     <div class="form-horizontal">
        <div class="form-group">
           <label for="requestStrategy" class="col-sm-3 control-label">requestStrategy</label>
           <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ng-model)]="sink.props.requestStrategy" placeholder="eg, like oneByOne,zero,watermark(10,5),default:watermark(100,50)"/>
           </div>
        </div>
     </div>
     <div class="col-sm-offset-3 col-sm-9">
        <div class="form-group">
            <button (click)="save()" class="btn btn-primary">save</button>
        </div>
     </div>
     <pre>{{propsPre}}</pre>
    `,
    directives: [
        NgFor, NgIf,
        CORE_DIRECTIVES, FORM_DIRECTIVES, ROUTER_DIRECTIVES
    ]
})
export class SinkDetail {
    sink:Sink = {
        category: 'kafka', name: '', props: {
            cmp: {}
        }
    };
    tmpCmpProps:any;
    edit:boolean = false;

    constructor(public http:Http, public location:Location, params:RouteParams) {
        let id = params.get("id");
        if (id) {
            this.edit = true;
            this.http.get(`${Global.SINK_PATH}/${id}`)
                .subscribe(res => {
                    this.sink = JSON.parse(res.text());
                    let props = this.sink.props;
                    this.tmpCmpProps = props.cmp
                });
        }
    }

    save() {
        if (this.edit) {
            this.http.post(`${Global.SINK_PATH}/${this.sink.id}`, JSON.stringify(this.sink))
                .subscribe(res => {
                    console.log("update", res);
                    this.location.back();
                });
        } else {
            this.http.post(Global.SINK_PATH, JSON.stringify(this.sink))
                .subscribe(res => {
                    console.log("save", res);
                    this.location.back();
                });
        }
    }

    nameChange():void {
        this.sink.id = Global.hashCode(this.sink.name);
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