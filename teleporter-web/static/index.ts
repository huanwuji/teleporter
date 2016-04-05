/// <reference path="../typings/tsd.d.ts" />
import {bootstrap} from 'angular2/bootstrap';
import {Component} from 'angular2/core';
import {HTTP_PROVIDERS} from 'angular2/http';
import {RouteConfig,RouterLink,ROUTER_PROVIDERS,ROUTER_DIRECTIVES} from 'angular2/router';
import {AddressDetail, AddressList} from './conf/Address'
import {SourceList,SourceDetail} from './conf/Source'
import {SinkList,SinkDetail} from './conf/Sink'
import {TaskList,TaskDetail} from './conf/Task'
import {StreamList,StreamDetail} from './conf/Stream';
import {GlobalPropsList,GlobalPropsDetail} from './conf/GlobalProps'
import {MetricsChart} from './MetricsChart'
import * as Global from "./Global";

@Component({
    selector: 'app',
    styles: [
        `
.routerLink-active {
    color: #fff;
    background-color: #337ab7;
}
        `
    ],
    template: `
<nav class="navbar navbar-inverse navbar-fixed-top">
    <div class="container">
        <a class="navbar-brand" href="#">Teleporter</a>
        <ul class="nav navbar-nav">
            <li class="nav-item">
                <a class="nav-link" href="#">Home <span class="sr-only">current</span></a>
            </li>
            <li class="nav-item">
                <a class="nav-link" [routerLink]="['/TaskList']">Task</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" [routerLink]="['/StreamList']">stream</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" [routerLink]="['/AddressList']">Address</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" [routerLink]="['/SourceList']">Source</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" [routerLink]="['/SinkList']">Sink</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" [routerLink]="['/GlobalPropsList']">GlobalProps</a>
            </li>
        </ul>
        <ul class="nav navbar-nav navbar-right">
            <li class="nav-item">
                <select class="form-control" [(ngModel)]="idc" (change)="changeBackendUrl($event.target.value)">
                    <option value="local" selected>本地</option>
                    <option value="sh" selected>上海</option>
                    <option value="ali">阿里</option>
                    <option value="ucloud">ucloud</option>
                </select>
            </li>
        </ul>
    </div>
</nav>
<div class="container">
    <router-outlet></router-outlet>
</div>
    `,
    directives: [
        AddressList, AddressDetail,
        TaskList, TaskDetail,
        StreamList, StreamDetail,
        SourceList, SourceDetail,
        GlobalPropsList, GlobalPropsDetail,
        MetricsChart,
        ROUTER_DIRECTIVES
    ]
})
@RouteConfig([
    {path: '/conf/task', as: 'TaskList', component: TaskList},
    {path: '/conf/task/create', as: 'TaskDetailCreate', component: TaskDetail},
    {path: '/conf/task/:id', as: 'TaskDetailEdit', component: TaskDetail},
    {path: '/conf/stream', as: 'StreamList', component: StreamList},
    {path: '/conf/stream/create', as: 'StreamDetailCreate', component: StreamDetail},
    {path: '/conf/stream/:id', as: 'StreamDetailEdit', component: StreamDetail},
    {path: '/conf/address', as: 'AddressList', component: AddressList},
    {path: '/conf/address/create', as: 'AddressDetailCreate', component: AddressDetail},
    {path: '/conf/address/:id', as: 'AddressDetailEdit', component: AddressDetail},
    {path: '/conf/source', as: 'SourceList', component: SourceList},
    {path: '/conf/source/create', as: 'SourceDetailCreate', component: SourceDetail},
    {path: '/conf/source/:id', as: 'SourceDetailEdit', component: SourceDetail},
    {path: '/conf/sink', as: 'SinkList', component: SinkList},
    {path: '/conf/sink/create', as: 'SinkDetailCreate', component: SinkDetail},
    {path: '/conf/sink/:id', as: 'SinkDetailEdit', component: SinkDetail},
    {path: '/conf/global_props', as: 'GlobalPropsList', component: GlobalPropsList},
    {path: '/conf/global_props/create', as: 'GlobalPropsDetailCreate', component: GlobalPropsDetail},
    {path: '/conf/global_props/:id', as: 'GlobalPropsDetailEdit', component: GlobalPropsDetail}
])
export class Home {
    name:string;
    idc:string;

    constructor() {
        this.name = 'teleporter'; // used in logo
        let idc = window.localStorage.getItem("idc");
        this.idc = idc;
        if (idc) {
            this.changeBackendUrl(idc);
        }
    }

    changeBackendUrl(idc) {
        window.localStorage.setItem("idc", idc);
        Global.changeUrl(idc);
    }
}
bootstrap(Home, [HTTP_PROVIDERS, ROUTER_PROVIDERS]);