import {Injectable} from "@angular/core";
import {Http} from "@angular/http";
import {Global} from "../../global";
import {Results} from "./metrics.chart";

@Injectable()
export class MetricsService {
  constructor(public http: Http) {
  }

  getMetrics(sql: string): Promise<Results> {
    let metricsConfig = {url: "http://10.132.178.140:8081", db: "teleporter"};
    return this.http.get(`${metricsConfig.url}/query?q=${sql}&db=${metricsConfig.db}&epoch=ms`)
      .toPromise()
      .then(response => <Results>response.json())
      .catch(Global.handleError);
  }
}
