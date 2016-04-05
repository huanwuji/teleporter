import {
    Component, Injectable, View, NgFor, bootstrap,
    Control, ControlGroup, ControlArray,
    EventEmitter,
    CORE_DIRECTIVES, FORM_DIRECTIVES
} from 'angular2/angular2';
import {CmpConfig} from "./Cmp";

@Component({
    selector: 'props-config',
    inputs: ['inputProps'],
    outputs: ['outputProps']
})
@View ({
    template: `
        <div class="form-horizontal">
           <div class="form-group" *ng-for="#prop of _inputProps;#i=index">
               <label for="prop" class="col-sm-3 control-label">
                  <input type="text" class="form-control" [(ng-model)]="prop.name" placeholder="name"/>
               </label>
               <div class="col-sm-4">
                  <input type="text" class="form-control" [(ng-model)]="prop.value" placeholder="value"/>
               </div>
               <div class="col-sm-1">
                  <button class="btn btn-danger" (click)="delProps(i)">del</button>
               </div>
           </div>
           <div class="col-sm-offset-7 col-sm-1"><button class="btn btn-primary" (click)="_inputProps.push({'':''})">add</button></div>
        </div>
    `,
    directives: [
        CORE_DIRECTIVES, FORM_DIRECTIVES,
        PropsCmp,
        NgFor
    ],
})
export class PropsCmp {
    outputProps = new EventEmitter();
    _inputProps:Array<any> = [];
    intervalId:number;

    constructor() {
        this.intervalId = setInterval(() => {
            let obj = {};
            this._inputProps.forEach((ele, index) => obj[ele.name] = ele.value);
            this.outputProps.next(obj)
        }, 100);
    }

    set inputProps(inputProps:any) {
        if (inputProps && !Object.keys(this._inputProps).length) {
            for (let name in inputProps) {
                this._inputProps.push({name: name, value: inputProps[name]});
            }
        }
    }

    delProps(i) {
        console.log(i);
        this._inputProps = this._inputProps.filter((v, ai, a)=>ai != i)
    }

    get inputProps() {
        return JSON.stringify(this._inputProps);
    }

    onDestroy() {
        clearInterval(this.intervalId);
        console.log("dataSource will destory")
    }
}