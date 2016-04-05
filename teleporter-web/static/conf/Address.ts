import {
    Component, Injectable, View,
    NgSwitch,NgSwitchWhen,NgFor,NgIf,
    Control, ControlGroup, ControlArray,
    CORE_DIRECTIVES, FORM_DIRECTIVES
} from 'angular2/angular2';
import {Http,Headers,Request,RequestOptions,RequestMethods} from 'angular2/http';
import {RouteParams,Location,ROUTER_DIRECTIVES} from 'angular2/router';
import * as Global from "../Global";
import {Address} from "../Types";
import {KafkaConsumerForm,KafkaProducerForm} from "./KafkaCmp";
import {DataSourceAddressForm} from "./DataSourceCmp";

@Component({
    selector: 'address-list'
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
                <th>props</th>
                <th><a class="btn btn-primary" [router-link]="['/AddressDetailCreate']">add</a></th>
            </tr>
        </thread>
        <tbody>
            <tr *ng-for="#address of addresss">
                <td>
                    <a [router-link]="['/AddressDetailEdit', {'id':address.id}]">{{address.id}}</a>
                </td>
                <td>{{address.name}}</td>
                <td>{{address.category}}</td>
                <td>{{address.taskId}}</td>
                <td title="{{address.props}}">{{address.props|json|slice:0:50}}...</td>
                <td><button class="btn btn-danger" (click)="delete(address.id)">del</button></td>
            </tr>
        </tbody>
    </table>
    <hr/>
    <div class="row">
        <div class="col-sm-1">
            <h4 class="pull-right"><a *ng-if="page>1" [router-link]="['/AddressList',{'page':page-1}]">pre</a></h4>
        </div>
        <div class="col-sm-10"></div>
        <div class="col-sm-1">
            <h4 class="pull-left"><a *ng-if="addresss.length==pageSize" [router-link]="['/AddressList',{'page':page+1}]">next</a></h4>
        </div>
    </div>
    `,
    directives: [
        NgSwitch, NgSwitchWhen, NgFor, NgIf,
        CORE_DIRECTIVES, FORM_DIRECTIVES, ROUTER_DIRECTIVES
    ]
})
export class AddressList {
    page:number = 1;
    pageSize:number = Global.PAGE_SIZE;
    addresss:Address[] = [];

    constructor(public http:Http, params:RouteParams) {
        let currPage = parseInt(params.get("page")) || 1;
        this.page = currPage;
        this.query(currPage);
    }

    query(page) {
        this.http.get(`${Global.ADDRESS_PATH}?page=${page}&pageSize=${this.pageSize}`)
            .subscribe(res => {
                this.addresss = JSON.parse(res.text());
            }
        );
    }

    delete(id) {
        if (confirm('Are you sure you want to delete?')) {
            this.http.get(`${Global.ADDRESS_PATH}/${id}?action=delete`)
                .subscribe(res => {
                    console.log("delete success", res);
                    this.query(this.page);
                });
        }
    }
}

@Component({
    selector: 'address-detail'
})
@View ({
    template: `
     <div class="form-inline">
        <div class="form-group">
            <label for="id">id</label>
            <input type="number" class="form-control" id="id" [(ng-model)]="address.id" placeholder="id" readonly/>
        </div>
        <div class="form-group">
            <label for="taskId">taskId</label>
            <input type="number" class="form-control" id="task" [(ng-model)]="address.taskId" placeholder="taskId"/>
        </div>
        <div class="form-group">
            <label for="category">category</label>
            <select id="category" [(ng-model)]="address.category" (change)="categoryChange()">
                <optgroup label="kafka">
                    <option value="kafka_consumer">kafka_consumer</option>
                    <option value="kafka_producer">kafka_producer</option>
                </optgroup>
                <option value="dataSource">dataSource</option>
            </select>
        </div>
        <div class="form-group">
            <label for="name">name</label>
            <input type="text" class="form-control" id="name" [(ng-model)]="address.name" placeholder="name" (change)="nameChange()" *ng-if="edit" readonly/>
            <input type="text" class="form-control" id="name" [(ng-model)]="address.name" placeholder="name" (change)="nameChange()" *ng-if="!edit"/>
        </div>
     </div>
     </br>
     <kafka-consumer-address *ng-if="address.category=='kafka_consumer'" [input-props]="tmpProps" (output-props)="propsChanged($event)"></kafka-consumer-address>
     <kafka-producer-address *ng-if="address.category=='kafka_producer'" [input-props]="tmpProps" (output-props)="propsChanged($event)"></kafka-producer-address>
     <data-source-address  *ng-if="address.category=='dataSource'"  [input-props]="tmpProps" (output-props)="propsChanged($event)"></data-source-address>
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
        KafkaConsumerForm, KafkaProducerForm, DataSourceAddressForm
    ]
})
export class AddressDetail {
    address:Address = {
        category: 'kafka_consumer', name: '', props: {
            cmp: {}
        }
    };
    tmpProps:any;
    edit:boolean = false;
    id:number;

    constructor(public http:Http, public location:Location, params:RouteParams) {
        this.id = parseInt(params.get("id"));
        if (this.id) {
            this.edit = true;
            this.http.get(`${Global.ADDRESS_PATH}/${this.id}`)
                .subscribe(res => {
                    this.address = JSON.parse(res.text());
                    this.tmpProps = this.address.props.cmp;
                });
        }
    }

    nameChange():void {
        this.address.id = Global.hashCode(this.address.name);
    }

    save() {
        if (this.id) {
            this.http.post(`${Global.ADDRESS_PATH}/${this.id}`, JSON.stringify(this.address))
                .subscribe(res => {
                    console.log("update", res);
                    this.location.back();
                });
        } else {
            this.http.post(Global.ADDRESS_PATH, JSON.stringify(this.address))
                .subscribe(res => {
                    console.log("save", res);
                    //this.location.go("/conf/address");
                    this.location.back();
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