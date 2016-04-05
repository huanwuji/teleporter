import {InfluxdbProps} from '../Types'
import {Component,Input,Output,EventEmitter} from 'angular2/core';
import {Config} from "./Config";
import * as Global from "../Global";

@Component({
    selector: 'influxdb-address',
    template: `
        <div class="form-horizontal">
           <div class="form-group">
               <label for="host" class="col-sm-3 control-label">host</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps.host" placeholder="host"/>
               </div>
           </div>
           <div class="form-group">
               <label for="port" class="col-sm-3 control-label">port</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps.port" placeholder="port"/>
               </div>
           </div>
           <div class="form-group">
               <label for="db" class="col-sm-3 control-label">db</label>
               <div class="col-sm-9">
                   <input type="text" class="form-control" [(ngModel)]="_inputProps.db" placeholder="db"/>
               </div>
           </div>
        </div>
    `
})
export class InfluxdbAddressForm extends Config<InfluxdbProps> {
    _inputProps:InfluxdbProps = {};

    constructor() {
        super();
    }
}