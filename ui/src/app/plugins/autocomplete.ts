import {Component, Input, Output, EventEmitter} from "@angular/core";
import {Http} from "@angular/http";

@Component({
  selector: 'autocomplete',
  template: `
        <div class="autoComp" (keydown)="keyEvent($event)">
            <input type="text" class="form-control" [(ngModel)]="theValue" (ngModelChange)="onChange($event)">
            <div class="autoComp-data-wrapper">
                <div class="autoComp-item" [ngClass]="{'autoComp-selected': d.selected}" *ngFor="let d of foundData" (click)="onSelect(d)">
                    {{d.name}}
                </div>
            </div>
        </div>
    `,
  styles: [`
        .autoComp {
            position: relative;
            float: left;
            width: 100%;
        }
        .autoComp input {
            width: 100%;
            float: left;
        }
        .autoComp-data-wrapper {
            position: absolute;
            top: 100%;
            width: 100%;
            left: 0;
            opacity: 0.9;
            background: #f5f5f5;
            z-index:10;
        }
        .autoComp-item {
            width: 100%;
            float: left;
            height: 34px;
        }
        .autoComp-selected {
            background-color:#d9edf7
        }
    `]
})

export class Autocomplete {
  private KEYCODES = {UP: 38, DOWN: 40, ENTER: 13};
  private theValue: any;
  private foundData: any = [];
  @Input() minLength: number = 1;
  @Input() searchUrl: any;
  private _initialValue: any;
  private selectIndex: number = 0;

  @Input() set initialValue(value: any) {
    if (!this.theValue) {
      this.theValue = value;
      this._initialValue = value;
    }
  }

  @Output() result = new EventEmitter();

  constructor(private http: Http) {
  }

  onChange(event: string) {
    this.foundData = [];
    this.searchText(event);
  }

  onSelect(item: any) {
    this.foundData = [];
    this.theValue = item.id;
    this.result.emit(item.id);
  }

  searchText(searchText: string) {
    if (searchText.length >= this.minLength) {
      let params = {searchText: searchText};
      let url = this.searchUrl.replace(/#{(.*?)}/g, function (all: string, letter: string) {
        return params[letter];
      });
      this.http.get(url)
        .subscribe(res => {
          this.foundData = res.json();
          this.selectIndex = -1;
        });
    } else {
      this.theValue = this._initialValue;
    }
  }

  keyEvent(event: any) {
    switch (event.keyCode) {
      case this.KEYCODES.UP:
        if (this.selectIndex > 0) {
          this.foundData[this.selectIndex].selected = false;
          this.selectIndex--;
          this.foundData[this.selectIndex].selected = true;
        }
        break;
      case this.KEYCODES.DOWN:
        if (this.selectIndex < this.foundData.length - 1) {
          if (this.selectIndex != -1) {
            this.foundData[this.selectIndex].selected = false;
          }
          this.selectIndex++;
          this.foundData[this.selectIndex].selected = true;
        }
        break;
      case this.KEYCODES.ENTER:
        this.onSelect(this.foundData[this.selectIndex]);
        break;
    }
  }
}
