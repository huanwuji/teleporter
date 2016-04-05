import * as Global from "../Global";

export class Search {
    taskSearch():string {
        return `${Global.TASK_PATH()}?page=1&pageSize=10&search={name:/#{searchText}/}`;
    }

    streamSearch():string {
        return `${Global.STREAM_PATH()}?page=1&pageSize=10&search={name:/#{searchText}/}`;
    }

    addressSearch():string {
        return `${Global.ADDRESS_PATH()}?page=1&pageSize=10&search={name:/#{searchText}/}`;
    }
}