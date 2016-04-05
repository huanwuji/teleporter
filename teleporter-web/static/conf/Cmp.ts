import {
    Component, Injectable, View, NgFor, bootstrap,
    Control, ControlGroup, ControlArray,
    EventEmitter,
    CORE_DIRECTIVES, FORM_DIRECTIVES
} from 'angular2/angular2';

export class CmpConfig<T> {
    outputProps = new EventEmitter();
    _inputProps:T;
    intervalId:number;

    constructor() {
        this.intervalId = setInterval(() => this.outputProps.next(this._inputProps), 100);
    }

    set inputProps(inputProps:any) {
        if (inputProps) {
            this._inputProps = inputProps;
        }
    }

    onDestroy() {
        clearInterval(this.intervalId);
        console.log("dataSource will destory")
    }
}