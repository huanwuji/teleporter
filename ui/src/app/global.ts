import {Headers, RequestOptions} from "@angular/http";
export class Global {
  static server: String = "http://localhost:9021";
  // static server: String = "";

  static handleError(error: any) {
    console.error('An error occurred', error);
    return Promise.reject(error.message || error);
  }

  static jsonHeaders = (function () {
    let headers = new Headers({'Content-Type': 'application/json'});
    return new RequestOptions({headers: headers});
  })();

  static formHeaders = (function () {
    var headers = new Headers();
    headers.append('Content-Type', 'application/x-www-form-urlencoded');
    return {
      headers: headers
    };
  })();
}
interface NsKey {
  ns: string;
}
export class KeyTemplates {
  //{tasks:[], instances:[]}
  static GROUP = "/config/group/:ns/:group";
  //{ip:"",port:""}
  static BROKER = "/config/broker/:ns/:broker";
  //{tasks:[:task], group:""}
  static INSTANCE = "/config/instance/:ns/:instance";
  static NAMESPACE = "/config/namespace/:namespace";
  //{instances:[:instance], group: ""}
  static TASK = "/config/task/:ns/:task";
  //{keys:"/stream/:ns/:task/:streamReg", resource:{cpu:"",memory:"",disk:"",network:""}}
  static PARTITION = "/config/partition/:ns/:task/:partition";
  static PARTITIONS = "/config/partition/:ns/:task/";
  static STREAM = "/config/stream/:ns/:task/:stream";
  static STREAMS = "/config/stream/:ns/:task/";
  static SOURCE = "/config/source/:ns/:task/:stream/:source";
  static TASK_SOURCES = "/config/source/:ns/:task/";
  static STREAM_SOURCES = "/config/source/:ns/:task/:stream/";
  static SINK = "/config/sink/:ns/:task/:stream/:sink";
  static TASK_SINKS = "/config/sink/:ns/:task/";
  static STREAM_SINKS = "/config/sink/:ns/:task/:stream/";
  static ADDRESS = "/config/address/:ns/:address";
  static VARIABLE = "/config/variable/:ns/:variable";
  //{keys:[], timestamp:194893439434}
  static RUNTIME_ADDRESS = "/runtime/address/:ns/:address/links/:instance";
  //{keys:[], timestamp:13489348934}
  static RUNTIME_VARIABLE = "/runtime/variable/:ns/:variable/links/:instance";
  // value: {instance:"/",timestamp:139489343948}
  static RUNTIME_PARTITION = "/runtime/partition/:ns/:task/:partition";
  static RUNTIME_PARTITIONS = "/runtime/partition/:ns/:task/";
  //{timestamp:13439483434}
  static RUNTIME_TASK_BROKER = "/runtime/task/:ns/:task/broker/:broker";
  //{timestamp:13439483434}
  static RUNTIME_BROKER_TASK = "/runtime/broker/:broker/task/:ns/:task";
  //{timestamp:13439483434}
  static RUNTIME_BROKER = "/runtime/broker/:broker";
  //{ip:"", port:1133}
  static RUNTIME_INSTANCE = "/runtime/instance/:ns/:instance";
}

export class Keys {
  static delimiter = /:(\w+)/;

  static applyArgs(template: string, ...args: string[]): string {
    return template.replace(this.delimiter, () => args.shift());
  }

  static applyObject(template: string, params: Object): string {
    return template.replace(this.delimiter, (group: string, group1: string) => params[group1]);
  }

  static mapping(key: string, templateSource: string, templateTarget: string): string {
    return Keys.apply(templateTarget, Keys.unapply(key, templateSource));
  }

  static unapply(key: string, template: string): Object {
    let sections = key.split("/");
    let tempSections = template.split(",");
    let result = {};
    for (let i = 0; i < tempSections.length; i++) {
      let tempTerm = tempSections[i];
      if (tempTerm.startsWith(":")) {
        result[tempTerm.substring(1)] = sections[i];
      }
    }
    return result;
  }
}
