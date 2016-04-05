/// <reference path="../typings/tsd.d.ts" />
import {Component, EventEmitter,Input} from 'angular2/core';
import {CORE_DIRECTIVES, FORM_DIRECTIVES, NgClass} from 'angular2/common';
import {Http,Headers,Request,RequestOptions} from 'angular2/http';
import {CHART_DIRECTIVES} from './third/charts';
import * as Global from "./Global";

export interface Serie {
    name:string;
    columns:string;
    values:Array<Array<any>>;
}
export interface Result {
    series:Array<Serie>
}
export interface Results {
    results:Array<Result>;
}

export interface Params {
    start?:string;
    end?:string;
    duration?:string;
    period?:string;
    sqlTemplate?:string;
}

@Component({
    selector: 'metrics-chart',
    template: `
<div class="row">
    <div class="col-md-10">
        <div class="form-group-sm form-inline">
            <p>{{params.name}}</p>
            <span class="glyphicon glyphicon-cog" aria-hidden="true" (click)="isHide=!isHide"></span>
            <input type="number" class="form-control input-sm" [(ngModel)]="period" placeholder="period"/>
            <input type="checkbox"  placeholder="autoRefresh" (click)="autoRefresh($event)"/>autoRefresh
        </div>
       <base-chart class="chart"
                   [data]="lineChartData"
                   [options]="lineChartOptions"
                   [legend]="lineChartLegend"
                   [chartType]="lineChartType"></base-chart>
    </div>
    <div class="col-md-2">
        <div [ngClass]="{hide: isHide}">
            <div class="form-group">
                <label for="start" class="col-sm-3 control-label">start</label>
                <div>
                    <input type="datetime-local" class="form-control" [(ngModel)]="params.start" placeholder="start"/>
                </div>
            </div>
            <div class="form-group">
                <label for="end" class="col-sm-3 control-label">end</label>
                <div>
                    <input type="datetime-local" class="form-control" [(ngModel)]="params.end" placeholder="end"/>
                </div>
            </div>
            <div class="form-group">
                <label for="duration" class="col-sm-3 control-label">duration</label>
                <div>
                    <input type="text" class="form-control" [(ngModel)]="params.duration" placeholder="duration"/>
                </div>
            </div>
            <div class="form-group">
                <label for="period" class="col-sm-3 control-label">period</label>
                <div>
                    <input type="text" class="form-control" [(ngModel)]="params.period" placeholder="period"/>
                </div>
            </div>
        </div>
    </div>
</div>
<div class="row">
    <div class="form-group">
        <label for="sqlTemplate" class="col-sm-3 control-label">sqlTemplate</label>
        <div>
            <input type="text" class="form-control" [(ngModel)]="sqlTemplate" placeholder="sqlTemplate" (click)="refresh()"/>
        </div>
    </div>
    <div class="form-group">
       <select id="category" (change)="sqlTemplate=$event.target.value" class="form-control">
            <option value='select sum(count) from "{name}" where time > now() - {duration} group by time({period})'>count-最近</option>
            <option value="select sum(count) from &quot;{name}&quot; where time > '{start}' and time < '{end}' group by time({period})">count-时间段</option>
       </select>
    </div>
    <pre>{{searchSql}}</pre>
</div>
    `,
    directives: [NgClass, CHART_DIRECTIVES, CORE_DIRECTIVES, FORM_DIRECTIVES]
})
export class MetricsChart {
    private isHide:boolean = true;
    private params:Params = {period: "1m", duration: "10m"};
    private sqlTemplate:string = 'select sum(count) from "{name}" where time > now() - {duration} group by time({period})';
    private period:number = 10000;
    private intervalId:number = -1;

    @Input() private set name(metricsName:string) {
        this.params['name'] = metricsName;
        this.refresh();
    }

    constructor(public http:Http) {
    }

    private refresh() {
        let metricsConfig = Global.ServerConfig.metrics;
        this.http.get(`${metricsConfig.url}/query?q=${this.searchSql}&db=${metricsConfig.db}&epoch=ms`)
            .subscribe(res => {
                this.influxToChart(res.json());
            })
    }

    private DEFAULT_DATA = {
        series: ['metrics'],
        labels: ['0', '1', '2', '3', '4', '5', '6'],
        data: [[0, 0, 0, 0, 0, 0, 0]]
    };
    // lineChart
    private lineChartData:{series:Array<string>,labels:Array<string>, data:Array<Array<number>> } = this.DEFAULT_DATA;
    private lineChartOptions:any = {
        datasetFill: false,
        animation: false,
        multiTooltipTemplate: '<%if (datasetLabel){%><%=datasetLabel %>: <%}%><%= value %>'
    };
    private lineChartLegend:boolean = true;
    private lineChartType:string = 'Line';

    influxToChart(res:Results) {
        if (!res.results[0].series) {
            this.lineChartData = this.DEFAULT_DATA;
            return;
        }
        let labels:any[] = [];
        let rows = [];
        let seriesName = [];

        res.results.forEach(result => {
            let series = result.series;
            series.forEach(serie => {
                seriesName.push(serie.name);
                let row = [];
                labels = [];
                let values = serie.values;
                if (values.length > 2) {
                    if (!values[0][1]) values.shift();
                    if (!values[values.length - 1][1]) values.pop();
                }
                values.forEach(value => {
                    let d = new Date(value[0]);
                    labels.push(Global.defaultDateFormat(d));
                    row.push(value[1]);
                });
                rows.push(row);
            })
        });
        this.lineChartData = {
            series: seriesName,
            labels: labels,
            data: rows
        };
    }

    get searchSql():string {
        let _params = (<any>Object).assign({}, this.params);
        if (this.params.start) {
            _params.start = Global.dateTimeLocalAjust2Str(this.params.start);
        }
        if (this.params.end) {
            _params.end = Global.dateTimeLocalAjust2Str(this.params.end);
        }
        return Global.template(this.sqlTemplate, _params);
    }

    autoRefresh(event) {
        let isAutoRefresh = event.target.value;
        if (isAutoRefresh && this.intervalId == -1 && this.period) {
            this.intervalId = setInterval(() => {
                this.refresh();
            }, this.period);
        } else {
            if (this.intervalId != -1) {
                clearInterval(this.intervalId);
                this.intervalId = -1;
            }
        }
    }
}