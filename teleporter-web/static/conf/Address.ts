import {Component} from 'angular2/core';
import {Http,Headers,Request,RequestOptions} from 'angular2/http';
import {RouteParams,Location,ROUTER_DIRECTIVES} from 'angular2/router';
import * as Global from "../Global";
import {Address} from "../Types";
import {KafkaConsumerForm,KafkaProducerForm} from "./Kafka";
import {DataSourceAddressForm} from "./DataSource";
import {MongoAddressForm} from "./Mongo";
import {InfluxdbAddressForm} from "./Influxdb";

@Component({
    selector: 'address-list',
    template: `
    <form class="form-inline">
        <div class="form-group">
            <label class="sr-only" for="search">search</label>
            <input type="search" class="form-control" id="search" placeholder="like {id:3434}" [(ngModel)]="searchText">
        </div>
        <button class="btn btn-default" [routerLink]="['/AddressList',{'page':page, 'search':searchText}]">search</button>
    </form>
    <table class="table table-striped table-hover">
        <tbody>
             <tr>
                <th>id</th>
                <th>name</th>
                <th>category</th>
                <th>props</th>
                <th><a class="btn btn-primary" [routerLink]="['/AddressDetailCreate']">add</a></th>
            </tr>
            <tr *ngFor="#address of addresses">
                <td>
                    <a [routerLink]="['/AddressDetailEdit', {'id':address.id}]">{{address.id}}</a>
                </td>
                <td>{{address.name}}</td>
                <td>{{address.category}}</td>
                <td title="{{address.props}}">{{address.props|json|slice:0:50}}...</td>
                <td><button class="btn btn-danger" (click)="delete(address.id)">del</button></td>
            </tr>
        </tbody>
    </table>
    <hr/>
    <div class="row">
        <div class="col-sm-1">
            <h4 class="pull-right"><a *ngIf="page>1" [routerLink]="['/AddressList',{'page':page-1}]">pre</a></h4>
        </div>
        <div class="col-sm-10"></div>
        <div class="col-sm-1">
            <h4 class="pull-left"><a *ngIf="addresses.length==pageSize" [routerLink]="['/AddressList',{'page':page+1}]">next</a></h4>
        </div>
    </div>
    `,
    directives: [ROUTER_DIRECTIVES]
})
export class AddressList {
    page:number = 1;
    pageSize:number = Global.PAGE_SIZE;
    addresses:Address[] = [];
    searchText:string;

    constructor(public http:Http, params:RouteParams) {
        let currPage = parseInt(params.get("page")) || 1;
        this.searchText = decodeURI(params.get("search") || "{}");
        this.page = currPage;
        this.query(currPage, this.searchText);
    }

    query(page, search = "{}") {
        this.http.get(`${Global.ADDRESS_PATH()}?page=${page}&pageSize=${this.pageSize}&search=${search}`)
            .subscribe(res => {
                this.addresses = res.json();
            }
        );
    }

    delete(id) {
        if (confirm('Are you sure you want to delete?')) {
            this.http.get(`${Global.ADDRESS_PATH()}/${id}?action=delete`)
                .subscribe(res => {
                    console.log("delete success", res);
                    this.query(this.page);
                });
        }
    }
}

@Component({
    selector: 'address-detail',
    template: `
     <div class="form-horizontal">
        <div class="form-group">
            <label for="id" class="col-sm-3 control-label">id</label>
            <div class="col-sm-9">
                <input type="number" class="form-control" id="id" [(ngModel)]="address.id" placeholder="id" readonly/>
            </div>
        </div>
        <div class="form-group">
            <label for="category" class="col-sm-3 control-label">category</label>
            <div class="col-sm-9">
                <select id="category" [(ngModel)]="address.category" (change)="categoryChange()" class="form-control">
                    <optgroup label="kafka">
                        <option value="kafka_consumer">kafka_consumer</option>
                        <option value="kafka_producer">kafka_producer</option>
                    </optgroup>
                    <option value="dataSource">dataSource</option>
                    <option value="mongo">mongo</option>
                    <option value="influxdb">influxdb</option>
                </select>
            </div>
        </div>
        <div class="form-group">
            <label for="name" class="col-sm-3 control-label">name</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" id="name" [(ngModel)]="address.name" placeholder="name" *ngIf="edit" readonly/>
                <input type="text" class="form-control" id="name" [(ngModel)]="address.name" placeholder="name" *ngIf="!edit"/>
            </div>
        </div>
     </div>
     <hr/>
     <br/>
     <kafka-consumer-address *ngIf="address.category=='kafka_consumer'" [inputProps]="tmpCmpProps" (outputProps)="propsChanged($event)"></kafka-consumer-address>
     <kafka-producer-address *ngIf="address.category=='kafka_producer'" [inputProps]="tmpCmpProps" (outputProps)="propsChanged($event)"></kafka-producer-address>
     <data-source-address  *ngIf="address.category=='dataSource'"  [inputProps]="tmpCmpProps" (outputProps)="propsChanged($event)"></data-source-address>
     <mongo-address  *ngIf="address.category=='mongo'"  [inputProps]="tmpCmpProps" (outputProps)="propsChanged($event)"></mongo-address>
     <influxdb-address  *ngIf="address.category=='influxdb'"  [inputProps]="tmpCmpProps" (outputProps)="propsChanged($event)"></influxdb-address>
     <hr/>
     <div class="col-sm-offset-3 col-sm-9">
        <div class="form-group">
            <button (click)="save()" class="btn btn-primary">save</button>
        </div>
     </div>
     <pre>{{propsPre}}</pre>
    `,
    directives: [
        KafkaConsumerForm, KafkaProducerForm, DataSourceAddressForm, MongoAddressForm, InfluxdbAddressForm
    ]
})
export class AddressDetail {
    address:Address = {
        category: 'kafka_consumer', name: '', props: {
            cmp: {}
        }
    };
    tmpCmpProps:any;
    edit:boolean = false;
    id:number;

    constructor(public http:Http, public location:Location, params:RouteParams) {
        this.id = parseInt(params.get("id"));
        if (this.id) {
            this.edit = true;
            this.http.get(`${Global.ADDRESS_PATH()}/${this.id}`)
                .subscribe(res => {
                    this.address = res.json();
                    this.tmpCmpProps = this.address.props.cmp;
                });
        }
    }

    save() {
        if (this.id) {
            this.http.post(`${Global.ADDRESS_PATH()}/${this.id}`, JSON.stringify(this.address))
                .subscribe(res => {
                    console.log("update", res);
                    this.location.back();
                });
        } else {
            this.http.post(Global.ADDRESS_PATH(), JSON.stringify(this.address))
                .subscribe(res => {
                    console.log("save", res);
                    this.address = res.json();
                    this.tmpCmpProps = this.address.props.cmp;
                });
        }
    }

    categoryChange() {
        this.address.props = {};
    }

    get propsPre() {
        if (this.address.props) {
            return JSON.stringify(this.address.props, null, '\t');
        }
    }

    private propsChanged(event:any):void {
        this.address.props.cmp = event;
    }
}