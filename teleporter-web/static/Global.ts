export const ServerConfigs = {
    local: {
        serverUrl: "http://localhost:8080",
        metrics: {
            url: "http://172.18.2.154:8086",
            db: "teleporter"
        }
    }
};
export var ServerConfig = ServerConfigs.sh;
export const ADDRESS_PATH = function () {
    return `${ServerConfig.serverUrl}/conf/address`;
};
export const SOURCE_PATH = function () {
    return `${ServerConfig.serverUrl}/conf/source`;
};
export const SINK_PATH = function () {
    return `${ServerConfig.serverUrl}/conf/sink`;
};
export const TASK_PATH = function () {
    return `${ServerConfig.serverUrl}/conf/task`;
};
export const STREAM_PATH = function () {
    return `${ServerConfig.serverUrl}/conf/stream`;
};
export const GLOBAL_PROPS_PATH = function () {
    return `${ServerConfig.serverUrl}/conf/global_props`;
};

export function changeUrl(location) {
    ServerConfig = ServerConfigs[location];
}
export const PAGE_SIZE = 10;
export function dot2Camel(name:string):string {
    return name.replace(/\.(\w)/g, function (all, letter) {
        return letter.toUpperCase();
    })
}
export function camel2Dot(name:string):string {
    return name.replace(/([A-Z])/g, function (all, letter) {
        return '.' + letter.toLowerCase();
    })
}

export function template(template:string, params:{}):string {
    return template.replace(/{(.*?)}/g, function (all, letter) {
        return params[letter];
    });
}

export function defaultDateFormat(d:Date):string {
    return `${d.getFullYear()}-${padLeft(d.getMonth() + 1)}-${padLeft(d.getDate())} ${padLeft(d.getHours())}:${padLeft(d.getMinutes())}:${padLeft(d.getSeconds())}`;
}
const OFFSET_TIME = 2 * 8 * 3600 * 1000;
export function dateTimeLocalAjust(d:string):Date {
    let date = new Date(d);
    return new Date(date.getTime() - OFFSET_TIME);
}

export function dateTimeLocalAjust2Str(d:string):string {
    return defaultDateFormat(dateTimeLocalAjust(d));
}

function padLeft(num:number, len:number = 2):string {
    let str = '' + num;
    let strLen = str.length;
    for (let i = 0; i < len - strLen; i++) {
        str = '0' + str;
    }
    return str;
}

export const KEYCODES = {UP: 38, DOWN: 40, ENTER: 13};

export const pageRollerTemplate:string = `
        <div>pageRoller</div>
        <div class="form-group">
            <label for="page" class="col-sm-3 control-label">page</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ngModel)]="_inputProps.page" placeholder="page"/>
            </div>
        </div>
        <div class="form-group">
            <label for="pageSize" class="col-sm-3 control-label">pageSize</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ngModel)]="_inputProps.pageSize" placeholder="pageSize"/>
            </div>
        </div>
        <div class="form-group">
            <label for="maxPage" class="col-sm-3 control-label">maxPage</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ngModel)]="_inputProps.maxPage" placeholder="maxPage"/>
            </div>
        </div>
        <div class="form-group">
            <label for="offset" class="col-sm-3 control-label">offset</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ngModel)]="_inputProps.offset" placeholder="offset"/>
            </div>
        </div>
    `;
export const timeRollerTemplate:string = `
        <div>timeRoller</div>
        <div class="form-group">
            <label for="deadline" class="col-sm-3 control-label">deadline</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ngModel)]="_inputProps.deadline" placeholder="1.minutes"/>
            </div>
        </div>
        <div class="form-group">
            <label for="start" class="col-sm-3 control-label">start</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ngModel)]="_inputProps.start" placeholder="start"/>
            </div>
        </div>
        <div class="form-group">
            <label for="period" class="col-sm-3 control-label">period</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ngModel)]="_inputProps.period" placeholder="period"/>
            </div>
        </div>
        <div class="form-group">
            <label for="maxPeriod" class="col-sm-3 control-label">maxPeriod</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ngModel)]="_inputProps.maxPeriod" placeholder="maxPeriod"/>
            </div>
        </div>
    `;
export const cronTemplate:string = `
        <div>cron</div>
        <div class="form-group">
            <label for="cron" class="col-sm-3 control-label">cron</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ngModel)]="_inputProps.cron" placeholder="* * * * *"/>
            </div>
        </div>
    `;
export const sourceErrorRulesTemplate:string = `
       <div class="form-group">
          <label for="errorRules" class="col-sm-3 control-label">errorRules</label>
          <div class="col-sm-9">
               <textarea class="form-control" id="errorRules" rows="3" [(ngModel)]="_inputProps.errorRules"
               placeholder="errorRules:error class regex:error message regex::restart|restart(1.minutes)|stop|retry:DEATH|DANGER|WARNING|INFO
multiple rules use newline, lonly match the first one,
eg: RuntimeException:SocketTime:retry:WARNING"></textarea>
          </div>
       </div>
    `;
export const sinkErrorRulesTemplate:string = `
       <div class="form-group">
          <label for="errorRules" class="col-sm-3 control-label">errorRules</label>
          <div class="col-sm-9">
               <textarea class="form-control" id="errorRules" rows="3" [(ngModel)]="_inputProps.errorRules"
               placeholder="errorRules:error class regex:error message regex::restart|restart(1.minutes)|stop:DEATH|DANGER|WARNING|INFO
multiple rules use newline, lonly match the first one,
eg: RuntimeException:SocketTime:retry:WARNING"></textarea>
          </div>
       </div>
    `;
