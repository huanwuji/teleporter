import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import {Global} from "../../global";
import {Results} from "./metrics.chart";
import {ConfigService, KeyBean} from "../../rest.servcie";
import {Address} from "../../namespace/address/address.service";

@Injectable()
export class MetricsService extends ConfigService<Address> {
  private metricsConfig: {url: string,db: string};

  constructor(public http: Http) {
    super(http);
  }

  getMetrics(sql: string): Promise<Results> {
    if (!this.metricsConfig) {
      return this.findOne('/address/teleporter/influxdb')
        .then((kb: KeyBean<Address>) => {
          let client = kb.value.client;
          this.metricsConfig = {
            url: `http://${client.host}:${client.port}`,
            db: client.db
          };
        }).then(() => this._getMetrics(sql));
    } else {
      return this._getMetrics(sql);
    }
  }

  private _getMetrics(sql: string): Promise<Results> {
    return this.http.get(`${this.metricsConfig.url}/query?q=${sql}&db=${this.metricsConfig.db}&epoch=ms`)
      .toPromise()
      .then(response => <Results>response.json())
      .catch(Global.handleError);
  }
}
