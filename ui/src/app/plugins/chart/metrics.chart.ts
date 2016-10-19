import {Component, Input, Output, EventEmitter} from "@angular/core";
import {ChartConfig} from "./charts";
import {MetricsService} from "./metrics.service";

export interface Serie {
  name: string;
  columns: string;
  values: any[][];
}
export interface Result {
  series: Serie[]
}
export interface Results {
  results: Result[];
}

export interface Params {
  start?: string;
  end?: string;
  duration?: string;
  period?: string;
  sqlTemplate?: string;
}

@Component({
  selector: 'metrics-chart',
  templateUrl: './metrics.chart.html'
})
export class MetricsChart {
  private isHide: boolean = true;
  private params: Params = {period: "1m", duration: "10m"};
  private sqlTemplate: string = `select sum(count) from "{name}" where time > now() - {duration} group by time({period})`;
  private period: number = 10000;
  private intervalId: number = -1;
  private config: ChartConfig;

  @Input()
  private set name(metricsName: string) {
    this.params['name'] = metricsName;
    this.refresh();
  }

  @Output() private closer: EventEmitter<boolean> = new EventEmitter<boolean>();

  constructor(private metricsService: MetricsService) {
  }

  triggerClose() {
    this.closer.emit(true);
  }

  private refresh() {
    this.metricsService.getMetrics(this.searchSql)
      .then((results: Results) => {
        this.influxToChart(results);
      });
  }

  influxToChart(res: Results) {
    let series = res.results[0].series;
    if (!series) {
      return;
    }
    this.config = {
      type: 'line',
      options: {
        tooltips: {
          mode: 'x-axis'
        }
      },
      data: {
        labels: series[0].values.map(([time, v]) => this.defaultDateFormat(new Date(time))),
        datasets: series.map((serie: Serie) => {
          return {
            fill: false,
            label: serie.name,
            data: serie.values.map(([time, v]) => v)
          }
        })
      }
    };
  }

  get searchSql(): string {
    let _params = (<any>Object).assign({}, this.params);
    if (this.params.start) {
      _params.start = this.dateTimeLocalAjust2Str(this.params.start);
    }
    if (this.params.end) {
      _params.end = this.dateTimeLocalAjust2Str(this.params.end);
    }
    return this.template(this.sqlTemplate, _params);
  }

  autoRefresh(event: any) {
    let isAutoRefresh = event.target.value;
    if (isAutoRefresh && this.intervalId == -1 && this.period) {
      this.intervalId = window.setInterval(() => {
        this.refresh();
      }, this.period);
    } else {
      if (this.intervalId != -1) {
        clearInterval(this.intervalId);
        this.intervalId = -1;
      }
    }
  }

  defaultDateFormat(d: Date): string {
    return `${d.getFullYear()}-${this.padLeft(d.getMonth() + 1)}-${this.padLeft(d.getDate())} ${this.padLeft(d.getHours())}:${this.padLeft(d.getMinutes())}:${this.padLeft(d.getSeconds())}`;
  }

  dateTimeLocalAjust2Str(d: string): string {
    return this.defaultDateFormat(this.dateTimeLocalAjust(d));
  }

  private OFFSET_TIME = 2 * 8 * 3600 * 1000;

  dateTimeLocalAjust(d: string): Date {
    let date = new Date(d);
    return new Date(date.getTime() - this.OFFSET_TIME);
  }

  padLeft(num: number, len: number = 2): string {
    let str = '' + num;
    let strLen = str.length;
    for (let i = 0; i < len - strLen; i++) {
      str = '0' + str;
    }
    return str;
  }

  template(template: string, params: {}): string {
    return template.replace(/{(.*?)}/g, function (all, letter) {
      return params[letter];
    });
  }
}
