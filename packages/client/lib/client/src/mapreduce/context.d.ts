import { MasterServiceClient } from '@dcfjs/proto/dcf/MasterService';
import { MasterExecFunction } from '@dcfjs/common';
export interface DCFMapReduceOptions {
    client?: MasterServiceClient;
    masterEndpoint?: string;
}
export declare class DCFMapReduceContext {
    readonly client: MasterServiceClient;
    constructor(options: DCFMapReduceOptions);
    exec<T>(f: MasterExecFunction<T>): Promise<T>;
}
