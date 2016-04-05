import {Component,Input,Output,EventEmitter,OnDestroy} from 'angular2/core';
import {Config} from "./Config";

@Component({
    selector: 'props-config',
    template: `
        <div class="form-horizontal">
           <div class="form-group" *ngFor="#prop of _inputProps;#i=index">
               <label for="prop" class="col-sm-3 control-label">
                  <input type="text" class="form-control" [(ngModel)]="prop.name" placeholder="name"/>
               </label>
               <div class="col-sm-8">
                   <textarea *ngIf="prop.value.length>100" rows="{{prop.value.length/60+1}}" class="form-control" [(ngModel)]="prop.value" placeholder="value"></textarea>
                   <input *ngIf="prop.value.length<=100" type="text" class="form-control" [(ngModel)]="prop.value" placeholder="value"/>
               </div>
               <div class="col-sm-1">
                  <button class="btn btn-danger" (click)="delProps(i)">del</button>
               </div>
           </div>
           <div class="col-sm-offset-11 col-sm-1"><button class="btn btn-primary" (click)="_inputProps.push({name:'',value:''})">add</button></div>
        </div>
    `
})
export class Props implements OnDestroy {
    _inputProps:Array<any> = [];
    @Output() outputProps = new EventEmitter();
    intervalId:number;

    constructor() {
        this.intervalId = setInterval(() => {
            let obj = {};
            this._inputProps.forEach((ele, index) => {
                if (ele.name) obj[ele.name] = ele.value
            });
            this.outputProps.next(obj)
        }, 100);
    }

    @Input() set inputProps(inputProps:any) {
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

    ngOnDestroy() {
        clearInterval(this.intervalId);
        console.log("dataSource will destory")
    }
}