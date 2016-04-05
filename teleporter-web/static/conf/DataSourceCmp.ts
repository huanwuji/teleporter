import {DataSourceProps,DataSourceSourceProps} from '../Types'
import {
    Component, Injectable, View, NgFor, bootstrap,
    Control, ControlGroup, ControlArray,
    EventEmitter,
    CORE_DIRECTIVES, FORM_DIRECTIVES
} from 'angular2/angular2';
import {CmpConfig} from "./Cmp";
import * as Global from "../Global";

@Component({
    selector: 'data-source-address',
    inputs: ['inputProps'],
    outputs: ['outputProps']
})
@View ({
    directives: [CORE_DIRECTIVES, FORM_DIRECTIVES],
    template: `
        <div class="form-horizontal">
           <div class="form-group">
               <label for="jdbcUrl" class="col-sm-3 control-label">jdbcUrl</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ng-model)]="_inputProps['jdbcUrl']" placeholder="jdbcUrl"/>
               </div>
           </div>
           <div class="form-group">
               <label for="username" class="col-sm-3 control-label">username</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ng-model)]="_inputProps['username']" placeholder="username"/>
               </div>
           </div>
           <div class="form-group">
               <label for="password" class="col-sm-3 control-label">password</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ng-model)]="_inputProps['password']" placeholder="password"/>
               </div>
           </div>
           <div class="form-group">
               <label for="maximumPoolSize" class="col-sm-3 control-label">maximumPoolSize</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ng-model)]="_inputProps['maximumPoolSize']" placeholder="maximumPoolSize"/>
               </div>
           </div>
        </div>
    `
})
export class DataSourceAddressForm extends CmpConfig<DataSourceProps> {
    _inputProps:DataSourceProps = {};

    constructor() {
        super();
    }
}

@Component({
    selector: 'data-source-source',
    inputs: ['inputProps'],
    outputs: ['outputProps']
})
@View ({
    directives: [CORE_DIRECTIVES, FORM_DIRECTIVES],
    template: `
    <div class="form-horizontal">
       <div class="form-group">
           <label for="sql" class="col-sm-3 control-label">sql</label>
           <div class="col-sm-9">
               <input type="text" class="form-control" [(ng-model)]="_inputProps['sql']" placeholder="sql"/>
           </div>
       </div>
       ${Global.pageRollerTemplate}
       ${Global.timeRollerTemplate}
    </div>
    `
})
export class DataSourceSourceForm extends CmpConfig<DataSourceSourceProps> {
    _inputProps:DataSourceSourceProps = {
        sql: ''
    };

    constructor() {
        super();
    }
}