import {Http, Response} from "@angular/http";
import {Global} from "./global";

export interface Identity {
  id?: number;
}

export interface KeyBean<T extends Identity> {
  key: string;
  value: T;
}

export interface KeyValue {
  key: string;
  value: string;
}

export abstract class RestService<T extends Identity> {
  protected serviceName: string;

  constructor(public http: Http) {
  }

  findOne(key: string): Promise<KeyBean<T>> {
    return this.http.get(`${Global.server}/${this.serviceName}?key=${key}`)
      .toPromise()
      .then(response => this.toBean(response.json()))
      .catch(Global.handleError);
  }

  range(key: string, start: number = 0, limit: number = 200): Promise<KeyBean<T>[]> {
    return <Promise<KeyBean<T>[]>>this.http.get(`${Global.server}/${this.serviceName}/range?key=${key}&start=${start}&limit=${limit}`)
      .toPromise()
      .then(response => (<KeyValue[]> response.json()).map((kv: KeyValue) => this.toBean(kv)))
      .catch(Global.handleError);
  }

  save(key: string, value: T): Promise<KeyBean<T>> {
    return this._save(key, value);
  }

  private _save(key: string, value: T): Promise<KeyBean<T>> {
    return this.http.post(`${Global.server}/${this.serviceName}`, JSON.stringify({
      key: key,
      value: JSON.stringify(value)
    }), Global.jsonHeaders)
      .toPromise()
      .then(response => this.toBean(response.json()))
      .catch(Global.handleError);
  }

  remove(key: string): Promise<Response> {
    return this.http.delete(`${Global.server}/${this.serviceName}?key=${key}`)
      .toPromise()
      .catch(Global.handleError);
  }

  refresh(key: string): Promise<Response> {
    return this.http.get(`${Global.server}/${this.serviceName}/notify?key=${key}`)
      .toPromise()
      .catch(Global.handleError);
  }

  atomicSave(key: string, expect: T, update: T): Promise<Response> {
    return this._atomicSave(key, expect, update);
  }

  private _atomicSave(key: string, expect: T, update: T): Promise<Response> {
    return this.http.post(`${Global.server}/${this.serviceName}/atomic`, JSON.stringify({
      key: key,
      expect: JSON.stringify(expect),
      update: JSON.stringify(update)
    }), Global.jsonHeaders)
      .toPromise()
      .catch(Global.handleError);
  }

  toBean(kv: KeyValue): KeyBean<T> {
    return {key: kv.key, value: <T>JSON.parse(kv.value)}
  }
}

export class ConfigService<T> extends RestService<T> {
  protected serviceName: string = "config";

  findOne(key: string): Promise<KeyBean<T>> {
    return super.findOne(key);
  }
}

export class RuntimeService<T> extends RestService<T> {
  protected serviceName: string = "runtime";

  findOne(key: string): Promise<KeyBean<T>> {
    return this.http.get(`${Global.server}/${this.serviceName}?key=${key}`)
      .toPromise()
      .then(response => this.toBean(response.json()))
      .catch((error: any) => {
        console.log(`${error}, May be the runtime ${key} not found`);
        return {};
      });
  }
}
