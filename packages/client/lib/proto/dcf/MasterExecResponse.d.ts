/// <reference types="node" />
export interface MasterExecResponse {
    /**
     * 使用 msgpack 包装返回值
     */
    'result'?: (Buffer | Uint8Array | string);
}
export interface MasterExecResponse__Output {
    /**
     * 使用 msgpack 包装返回值
     */
    'result'?: (Buffer);
}
