import {DataSourceProps,DataSourceSourceProps,DataSourceSinkProps} from '../Types'
import {Component,Input,Output,EventEmitter} from 'angular2/core';
import {Config} from "./Config";
import * as Global from "../Global";

@Component({
    selector: 'data-source-address',
    template: `
        <div class="form-horizontal">
           <div class="form-group">
               <label for="jdbcUrl" class="col-sm-3 control-label">jdbcUrl</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps.jdbcUrl" placeholder="jdbcUrl"/>
               </div>
           </div>
           <div class="form-group">
               <label for="username" class="col-sm-3 control-label">username</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps.username" placeholder="username"/>
               </div>
           </div>
           <div class="form-group">
               <label for="password" class="col-sm-3 control-label">password</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps.password" placeholder="password"/>
               </div>
           </div>
           <div class="form-group">
               <label for="maximumPoolSize" class="col-sm-3 control-label">maximumPoolSize</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps.maximumPoolSize" placeholder="maximumPoolSize"/>
               </div>
           </div>
        </div>
    `
})
export class DataSourceAddressForm extends Config<DataSourceProps> {
    _inputProps:DataSourceProps = {};

    constructor() {
        super();
    }
}

@Component({
    selector: 'data-source-source',
    template: `
    <div class="form-horizontal">
       <div class="form-group">
           <label for="sql" class="col-sm-3 control-label">sql</label>
           <div class="col-sm-9">
               <input type="text" class="form-control" [(ngModel)]="_inputProps['sql']" placeholder="select * from table where start>#{start} and end<#{end} limit {offset},{pageSize}"/>
           </div>
       </div>
       ${Global.sourceErrorRulesTemplate}
       ${Global.cronTemplate}
       ${Global.pageRollerTemplate}
       ${Global.timeRollerTemplate}
    </div>
    `
})
export class DataSourceSourceForm extends Config<DataSourceSourceProps> {
    _inputProps:DataSourceSourceProps = {
        sql: ''
    };

    constructor() {
        super();
    }
}

@Component({
    selector: 'data-source-sink',
    template: `
    <div class="form-horizontal">
       ${Global.sinkErrorRulesTemplate}
    </div>
    `
})
export class DataSourceSinkForm extends Config<DataSourceSinkProps> {
    _inputProps:DataSourceSinkProps = {
        errorRules: ''
    };

    constructor() {
        super();
    }
}