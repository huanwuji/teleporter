import {Component, View, bootstrap, CORE_DIRECTIVES, FORM_DIRECTIVES} from 'angular2/angular2';
import {HTTP_PROVIDERS} from 'angular2/http';
import {AddressDetail, AddressList} from './conf/Address'
import {SourceList,SourceDetail} from './conf/Source'
import {SinkList,SinkDetail} from './conf/Sink'
import {TaskList,TaskDetail} from './conf/Task'
import {
    RouteConfig,
    ROUTER_DIRECTIVES, ROUTER_PROVIDERS
} from 'angular2/router';

@Component({
    selector: 'app'
})
@View({
    styles: [
        `
.router-link-active {
    color: #fff;
    background-color: #337ab7;
}
        `
    ],
    template: `
<nav class="navbar navbar-inverse navbar-fixed-top">
    <a class="navbar-brand" href="#">Project name</a>
    <ul class="nav nav-pills">
        <li class="nav-item">
            <a class="nav-link" href="#">Home <span class="sr-only">(current)</span></a>
        </li>
        <li class="nav-item">
            <a class="nav-link" [router-link]="['/TaskList']">Task</a>
        </li>
        <li class="nav-item">
            <a class="nav-link" [router-link]="['/AddressList']">Address</a>
        </li>
        <li class="nav-item">
            <a class="nav-link" [router-link]="['/SourceList']">Source</a>
        </li>
        <li class="nav-item">
            <a class="nav-link" [router-link]="['/SinkList']">Sink</a>
        </li>
    </ul>
</nav>
<div class="container">
    <router-outlet/>
</div>
    `,
    directives: [
        CORE_DIRECTIVES, FORM_DIRECTIVES, ROUTER_DIRECTIVES,
        AddressList, AddressDetail,
        TaskList, TaskDetail,
        SourceList, SourceDetail
    ]
})
@RouteConfig([
    {path: '/conf/task', as: 'TaskList', component: TaskList},
    {path: '/conf/task/create', as: 'TaskDetailCreate', component: TaskDetail},
    {path: '/conf/task/:id', as: 'TaskDetailEdit', component: TaskDetail},
    {path: '/conf/address', as: 'AddressList', component: AddressList},
    {path: '/conf/address/create', as: 'AddressDetailCreate', component: AddressDetail},
    {path: '/conf/address/:id', as: 'AddressDetailEdit', component: AddressDetail},
    {path: '/conf/source', as: 'SourceList', component: SourceList},
    {path: '/conf/source/create', as: 'SourceDetailCreate', component: SourceDetail},
    {path: '/conf/source/:id', as: 'SourceDetailEdit', component: SourceDetail},
    {path: '/conf/sink', as: 'SinkList', component: SinkList},
    {path: '/conf/sink/create', as: 'SinkDetailCreate', component: SinkDetail},
    {path: '/conf/sink/:id', as: 'SinkDetailEdit', component: SinkDetail},
])
export class Home {
    name:string;

    constructor() {
        this.name = 'teleporter'; // used in logo
    }
}
bootstrap(Home, [HTTP_PROVIDERS, ROUTER_PROVIDERS]);