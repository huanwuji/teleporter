import {Component, OnDestroy, OnInit, ElementRef, Input} from "@angular/core";

export class Rgba {

  constructor(public r: number, public g: number, public b: number, public a: number) {
  }

  toStr(): string {
    return `rgba(${this.r},${this.g},${this.b},${this.a})`;
  }

  clone(): Rgba {
    return new Rgba(this.r, this.g, this.b, this.a);
  }
}

export interface Dataset {
  label?: string;
  data: number[];
  backgroundColor?: string;
  borderColor?: string;
  strokeColor?: string;
  borderWidth?: number;
}

export interface ChartConfig {
  type?: string;
  options?: {
    tooltips?: {
      mode?: string;
    }
  };
  data?: {
    labels?: string[];
    datasets?: Dataset[];
  }
}

declare var Chart: any;

@Component({
  selector: 'base-chart',
  template: `<canvas></canvas>`,
  styles: [`:host { display: block;}`]
})
export class BaseChartComponent implements OnDestroy, OnInit {
  private ctx: any;
  private chart: any;
  private element: ElementRef;

  @Input() set config(config: ChartConfig) {
    if (config) {
      if (!this.chart) {
        this.ctx = this.element.nativeElement.children[0].getContext('2d');
        this.chart = this.chartInit(this.ctx, config);
      } else {
        this.refresh(config);
      }
    }
  }

  constructor(element: ElementRef) {
    this.element = element;
  }

  ngOnInit(): any {
  }

  ngOnDestroy(): any {
    if (this.chart) {
      this.chart.destroy();
      this.chart = void 0;
    }
  }

  private chartInit(ctx: any, config: ChartConfig): any {
    if (typeof Chart === 'undefined') {
      throw new Error('charts configuration issue: Embedding Chart.js lib is mandatory');
    }
    this.fillColor(config);
    return new Chart(ctx, config);
  }

  private fillColor(config: ChartConfig) {
    if (config.data.datasets) {
      let baseColor = new Rgba(253, 180, 92, 0.4);
      config.data.datasets.forEach((dataset: Dataset, i: number) => {
        let color = baseColor.clone();
        let step = 20;
        color.r = (color.r + step * 0.3 * i) % 255;
        color.g = (color.g + step * 0.6 * i) % 255;
        color.b = (color.b + step * i) % 255;
        if (!dataset.backgroundColor) {
          let backgroundColor = color.clone();
          dataset.backgroundColor = backgroundColor.toStr();
        }
        if (!dataset.borderColor) {
          let borderColor = color.clone();
          borderColor.a = 1;
          dataset.borderColor = borderColor.toStr();
        }
      });
    }
  }

  private refresh(config: ChartConfig): any {
    if (config.data.datasets) {
      this.fillColor(config);
      this.chart.data.datasets = config.data.datasets;
    }
    if (config.data.labels) {
      this.chart.data.labels = config.data.labels;
    }
    this.chart.update();
  }
}
