import {NgModule} from "@angular/core";
import {BrowserModule} from "@angular/platform-browser";
import {AppComponent} from "./app.component";
import {FormsModule, ReactiveFormsModule} from "@angular/forms";
import {RouterModule, Routes} from "@angular/router";
import {NamespaceListComponent, NamespaceDetailComponent} from "./namespace/namespace.component";
import {AddressListComponent, AddressDetailComponent} from "./namespace/address/address.component";
import {VariableListComponent, VariableDetailComponent} from "./namespace/variable/variable.component";
import {TaskListComponent, TaskDetailComponent} from "./namespace/task/task.component";
import {PartitionListComponent, PartitionDetailComponent} from "./namespace/task/partition/partition.component";
import {StreamListComponent, StreamDetailComponent} from "./namespace/task/stream/stream.component";
import {SourceListComponent, SourceDetailComponent} from "./namespace/task/stream/source/source.component";
import {SinkListComponent, SinkDetailComponent} from "./namespace/task/stream/sink/sink.component";
import {GroupListComponent, GroupDetailComponent} from "./namespace/group/group.component";
import {InstanceListComponent, InstanceDetailComponent} from "./namespace/instance/instance.component";
import {BrokerDetailComponent, BrokerListComponent} from "./namespace/broker/broker.component";
import {HttpModule} from "@angular/http";
import {AddressService, RuntimeAddressService} from "./namespace/address/address.service";
import {NamespaceService} from "./namespace/namespace.service";
import {BrokerService} from "./namespace/broker/broker.service";
import {GroupService} from "./namespace/group/group.service";
import {InstanceService, RuntimeInstanceService} from "./namespace/instance/instance.service";
import {TaskService} from "./namespace/task/task.service";
import {PartitionService, RuntimePartitionService} from "./namespace/task/partition/partition.service";
import {StreamService} from "./namespace/task/stream/stream.service";
import {SourceService} from "./namespace/task/stream/source/source.service";
import {SinkService} from "./namespace/task/stream/sink/sink.service";
import {VariableService, VariableRuntimeService} from "./namespace/variable/variable.service";
import {BaseChartComponent} from "./plugins/chart/charts";
import {DynamicFormItemComponent} from "./dynamic/form/dynamic-form-item.component";
import {FormItemService} from "./dynamic/form/form-item.service";
import {MetricsChart} from "./plugins/chart/metrics.chart";
import {MetricsService} from "./plugins/chart/metrics.service";

export const routes: Routes = [
  {path: '', redirectTo: '/ns', pathMatch: 'full'},
  {path: 'ns', component: NamespaceListComponent, pathMatch: 'full'},
  {path: 'ns/add', component: NamespaceDetailComponent, pathMatch: 'full'},
  {path: 'ns/:ns', component: NamespaceDetailComponent},
  {path: 'address/:ns', component: AddressListComponent, pathMatch: 'full'},
  {path: 'address/:ns/add', component: AddressDetailComponent, pathMatch: 'full'},
  {path: 'address/:ns/:address', component: AddressDetailComponent},
  {path: 'broker/:ns', component: BrokerListComponent, pathMatch: 'full'},
  {path: 'broker/:ns/add', component: BrokerDetailComponent, pathMatch: 'full'},
  {path: 'broker/:ns/:broker', component: BrokerDetailComponent},
  {path: 'group/:ns', component: GroupListComponent, pathMatch: 'full'},
  {path: 'group/:ns/add', component: GroupDetailComponent, pathMatch: 'full'},
  {path: 'group/:ns/:group', component: GroupDetailComponent},
  {path: 'instance/:ns', component: InstanceListComponent, pathMatch: 'full'},
  {path: 'instance/:ns/add', component: InstanceDetailComponent, pathMatch: 'full'},
  {path: 'instance/:ns/:instance', component: InstanceDetailComponent},
  {path: 'variable/:ns', component: VariableListComponent, pathMatch: 'full'},
  {path: 'variable/:ns/add', component: VariableDetailComponent, pathMatch: 'full'},
  {path: 'variable/:ns/:variable', component: VariableDetailComponent},
  {path: 'task/:ns', component: TaskListComponent, pathMatch: 'full'},
  {path: 'task/:ns/add', component: TaskDetailComponent, pathMatch: 'full'},
  {path: 'task/:ns/:task', component: TaskDetailComponent},
  {path: 'partition/:ns/:task', component: PartitionListComponent, pathMatch: 'full'},
  {path: 'partition/:ns/:task/add', component: PartitionDetailComponent, pathMatch: 'full'},
  {path: 'partition/:ns/:task/:partition', component: PartitionDetailComponent},
  {path: 'stream/:ns/:task', component: StreamListComponent, pathMatch: 'full'},
  {path: 'stream/:ns/:task/add', component: StreamDetailComponent, pathMatch: 'full'},
  {path: 'stream/:ns/:task/:stream', component: StreamDetailComponent},
  {path: 'source/:ns/:task/:stream', component: SourceListComponent, pathMatch: 'full'},
  {path: 'source/:ns/:task/:stream/add', component: SourceDetailComponent, pathMatch: 'full'},
  {path: 'source/:ns/:task/:stream/:source', component: SourceDetailComponent},
  {path: 'sink/:ns/:task/:stream', component: SinkListComponent, pathMatch: 'full'},
  {path: 'sink/:ns/:task/:stream/add', component: SinkDetailComponent, pathMatch: 'full'},
  {path: 'sink/:ns/:task/:stream/:sink', component: SinkDetailComponent}
];

@NgModule({
  imports: [
    BrowserModule,
    RouterModule.forRoot(routes),
    HttpModule,
    ReactiveFormsModule,
    FormsModule
  ],
  providers: [
    FormItemService,
    MetricsService,
    NamespaceService,
    AddressService,
    RuntimeAddressService,
    BrokerService,
    GroupService,
    InstanceService,
    RuntimeInstanceService,
    TaskService,
    PartitionService,
    RuntimePartitionService,
    StreamService,
    SourceService,
    SinkService,
    VariableService,
    VariableRuntimeService
  ],
  declarations: [
    BaseChartComponent,
    MetricsChart,
    DynamicFormItemComponent,
    AddressListComponent,
    AddressDetailComponent,
    NamespaceListComponent,
    NamespaceDetailComponent,
    BrokerListComponent,
    BrokerDetailComponent,
    GroupListComponent,
    GroupDetailComponent,
    InstanceListComponent,
    InstanceDetailComponent,
    TaskListComponent,
    TaskDetailComponent,
    PartitionListComponent,
    PartitionDetailComponent,
    StreamListComponent,
    StreamDetailComponent,
    SourceListComponent,
    SourceDetailComponent,
    SinkListComponent,
    SinkDetailComponent,
    VariableListComponent,
    VariableDetailComponent,
    AppComponent
  ],
  bootstrap: [AppComponent]
})
export class AppModule {
}
