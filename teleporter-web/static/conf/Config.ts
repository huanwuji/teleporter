import {
    Component,Input,Output,EventEmitter,OnDestroy
} from 'angular2/core';

export class Config<T> implements OnDestroy {
    @Input() set inputProps(inputProps:any) {
        if (inputProps) {
            this._inputProps = inputProps;
        }
    }

    _inputProps:T;
    @Output() outputProps = new EventEmitter();
    intervalId:number;

    constructor() {
        this.intervalId = setInterval(() => this.outputProps.next(this._inputProps), 100);
    }

    ngOnDestroy() {
        clearInterval(this.intervalId);
        console.log("dataSource will destory")
    }
}