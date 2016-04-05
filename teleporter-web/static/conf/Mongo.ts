import {MongoProps,MongoSourceProps} from '../Types'
import {Component,Input,Output,EventEmitter} from 'angular2/core';
import {Config} from "./Config";
import * as Global from "../Global";

@Component({
    selector: 'mongo-address',
    template: `
        <div class="form-horizontal">
           <div class="form-group">
               <label for="url" class="col-sm-3 control-label">url</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps.url" placeholder="url"/>
               </div>
           </div>
        </div>
    `
})
export class MongoAddressForm extends Config<MongoProps> {
    _inputProps:MongoProps = {
        url: ''
    };

    constructor() {
        super();
    }
}

@Component({
    selector: 'mongo-source',
    template: `
    <div class="form-horizontal">
       <div class="form-group">
           <label for="database" class="col-sm-3 control-label">database</label>
           <div class="col-sm-9">
               <input type="text" class="form-control" [(ngModel)]="_inputProps.database" placeholder="database"/>
           </div>
       </div>
       <div class="form-group">
           <label for="collection" class="col-sm-3 control-label">collection</label>
           <div class="col-sm-9">
               <input type="text" class="form-control" [(ngModel)]="_inputProps.collection" placeholder="collection"/>
           </div>
       </div>
       <div class="form-group">
           <label for="query" class="col-sm-3 control-label">query</label>
           <div class="col-sm-9">
               <input type="text" class="form-control" [(ngModel)]="_inputProps.query" placeholder="query"/>
           </div>
       </div>
       ${Global.cronTemplate}
       ${Global.pageRollerTemplate}
       ${Global.timeRollerTemplate}
    </div>
    `
})
export class MongoSourceForm extends Config<MongoSourceProps> {
    _inputProps:MongoSourceProps = {
        collection: '',
        query: ''
    };

    constructor() {
        super();
    }
}