import {Component} from 'angular2/core';
import {Http,Headers,Request,Response,RequestOptions,RequestMethod} from 'angular2/http';
import {RouteParams,Location,ROUTER_DIRECTIVES} from 'angular2/router';
import * as Global from "../Global";
import {GlobalProps} from "../Types";
import {Props} from "./Props";

@Component({
    selector: 'global-props-list',
    template: `
    <form class="form-inline">
        <div class="form-group">
            <label class="sr-only" for="search">search</label>
            <input type="search" class="form-control" id="search" placeholder="like {id:3434}" [(ngModel)]="searchText">
        </div>
        <button class="btn btn-default" [routerLink]="['/GlobalPropsList',{'page':page, 'search':searchText}]">search</button>
    </form>
    <table class="table table-striped table-hover">
        <tbody>
            <tr>
                <th>id</th>
                <th>name</th>
                <th>props</th>
                <th><a class="btn btn-primary" [routerLink]="['/GlobalPropsDetailCreate']">add</a></th>
            </tr>
            <tr *ngFor="#prop of globalProps">
                <td>
                    <a [routerLink]="['/GlobalPropsDetailEdit', {'id':prop.id}]">{{prop.id}}</a>
                </td>
                <td>{{prop.name}}</td>
                <td title="{{prop.props|json}}">{{prop.props|json|slice:0:50}}...</td>
                <td><button class="btn btn-danger" (click)="delete(prop.id)">del</button></td>
            </tr>
        </tbody>
    </table>
    <hr/>
    <div class="row">
        <div class="col-sm-1">
            <h4 class="pull-right"><a *ngIf="page>1" [routerLink]="['/GlobalPropsList',{'page':page-1}]">pre</a></h4>
        </div>
        <div class="col-sm-10"></div>
        <div class="col-sm-1">
            <h4 class="pull-left"><a *ngIf="globalProps.length==pageSize" [routerLink]="['/GlobalPropsList',{'page':page+1}]">next</a></h4>
        </div>
    </div>
    `,
    directives: [ROUTER_DIRECTIVES]
})
export class GlobalPropsList {
    page:number = 1;
    pageSize:number = Global.PAGE_SIZE;
    globalProps:GlobalProps[] = [];
    searchText:string;

    constructor(public http:Http, params:RouteParams) {
        let currPage = parseInt(params.get("page")) || 1;
        this.searchText = decodeURI(params.get("search") || "{}");
        this.page = currPage;
        this.query(currPage, this.searchText);
    }

    query(page, search = "{}") {
        this.http.get(`${Global.GLOBAL_PROPS_PATH()}?page=${page}&pageSize=${this.pageSize}&search=${search}`)
            .subscribe(res => {
                this.globalProps = res.json();
            }
        );
    }

    delete(id) {
        if (confirm('Are you sure you want to delete?')) {
            this.http.get(`${Global.GLOBAL_PROPS_PATH()}/${id}?action=delete`)
                .subscribe(res => {
                    console.log("delete success", res);
                    this.query(this.page);
                });
        }
    }
}

@Component({
    selector: 'global-props-detail',
    template: `
     <div class="form-horizontal">
        <div class="form-group">
            <label for="id" class="col-sm-3 control-label">id</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="id" [(ngModel)]="globalProps.id" placeholder="id" readonly/>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">name</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="globalProps.name" placeholder="name" *ngIf="edit" readonly/>
                <input type="text" class="form-control" id="name" [(ngModel)]="globalProps.name" placeholder="name" *ngIf="!edit"/>
            </div>
        </div>
        <div class="form-group">
            <label for="desc" class="col-sm-3 control-label">desc</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="desc" [(ngModel)]="globalProps.desc" placeholder="desc"/>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">props</label>
            <div class="col-sm-9">
                <props-config [inputProps]="tmpProps" (outputProps)="propsChanged($event)"></props-config>
            </div>
        </div>
        <hr/>
        <br/>
        <div class="col-sm-offset-3 col-sm-9">
            <div class="form-group">
                <button (click)="save()" class="btn btn-primary">save</button>
            </div>
        </div>
     </div>
     <pre>{{propsPre}}</pre>
    `,
    directives: [Props]
})
export class GlobalPropsDetail {
    globalProps:GlobalProps = {name: '', props: {}, desc: ''};
    tmpProps:any = {};
    edit:boolean = false;

    constructor(public http:Http, params:RouteParams) {
        var id = params.get("id");
        if (id) {
            this.edit = true;
            this.http.get(`${Global.GLOBAL_PROPS_PATH()}/${id}`)
                .subscribe(res => {
                    this.globalProps = res.json();
                    this.tmpProps = this.globalProps.props;
                });
        }
    }

    save() {
        if (this.edit) {
            this.http.post(`${Global.GLOBAL_PROPS_PATH()}/${this.globalProps.id}`, JSON.stringify(this.globalProps))
                .subscribe(res => console.log("update", res));
        } else {
            this.http.post(Global.GLOBAL_PROPS_PATH(), JSON.stringify(this.globalProps))
                .subscribe(res => {
                    this.globalProps = res.json();
                    this.tmpProps = this.globalProps.props;
                    this.edit = true;
                    console.log("save", res);
                });
        }
    }

    get propsPre() {
        if (this.globalProps.props) {
            return JSON.stringify(this.globalProps.props, null, '\t');
        }
    }

    private propsChanged(event:any):void {
        this.globalProps.props = event;
    }
}