/// <reference types="node" />
export interface WorkerExecRequest {
    /**
     * 使用 objpack 包装 SerializedFunction，无参数，返回任意可序列化结果
     */
    'func'?: (Buffer | Uint8Array | string);
}
export interface WorkerExecRequest__Output {
    /**
     * 使用 objpack 包装 SerializedFunction，无参数，返回任意可序列化结果
     */
    'func'?: (Buffer);
}
