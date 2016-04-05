export const serverUrl = "http://172.18.2.156:8080"; //local

export const ADDRESS_PATH = `${serverUrl}/conf/address`;
export const SOURCE_PATH = `${serverUrl}/conf/source`;
export const SINK_PATH = `${serverUrl}/conf/sink`;
export const TASK_PATH = `${serverUrl}/conf/task`;
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

export function hashCode(str:string):number {
    var hash = 0;
    if (str.length == 0) return hash;
    for (var i = 0; i < str.length; i++) {
        hash = ((hash << 5) - hash) + str.charCodeAt(i);
        hash = hash & hash;
    }
    return Math.abs(hash);
}

export const pageRollerTemplate:string = `
        <div>pageRoller</div>
        <div class="form-group">
            <label for="page" class="col-sm-3 control-label">page</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ng-model)]="_inputProps.pageRoller.page" placeholder="page"/>
            </div>
        </div>
        <div class="form-group">
            <label for="pageSize" class="col-sm-3 control-label">pageSize</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ng-model)]="_inputProps.pageRoller.pageSize" placeholder="pageSize"/>
            </div>
        </div>
        <div class="form-group">
            <label for="maxPage" class="col-sm-3 control-label">maxPage</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ng-model)]="_inputProps.pageRoller.maxPage" placeholder="maxPage"/>
            </div>
        </div>
    `;
export const timeRollerTemplate:string = `
        <div>timeRoller</div>
        <div class="form-group">
            <label for="deadline" class="col-sm-3 control-label">deadline</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ng-model)]="_inputProps.timeRoller.deadline" placeholder="1.minutes"/>
            </div>
        </div>
        <div class="form-group">
            <label for="start" class="col-sm-3 control-label">start</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ng-model)]="_inputProps.timeRoller.start" placeholder="start"/>
            </div>
        </div>
        <div class="form-group">
            <label for="period" class="col-sm-3 control-label">period</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ng-model)]="_inputProps.timeRoller.period" placeholder="period"/>
            </div>
        </div>
        <div class="form-group">
            <label for="maxPeriod" class="col-sm-3 control-label">maxPeriod</label>
            <div class="col-sm-9">
                <input type="text" class="form-control" [(ng-model)]="_inputProps.timeRoller.maxPeriod" placeholder="maxPeriod"/>
            </div>
        </div>
    `;