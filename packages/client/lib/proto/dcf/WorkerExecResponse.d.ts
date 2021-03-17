/// <reference types="node" />
export interface WorkerExecResponse {
    /**
     * 使用 msgpack 包装返回值
     */
    'result'?: (Buffer | Uint8Array | string);
}
export interface WorkerExecResponse__Output {
    /**
     * 使用 msgpack 包装返回值
     */
    'result'?: (Buffer);
}
