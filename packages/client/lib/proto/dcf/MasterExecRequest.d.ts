/// <reference types="node" />
export interface MasterExecRequest {
    /**
     * 使用 objpack 包装 SerializedFunction，参数为({dispatchWork})，返回任意可序列化结果
     */
    'func'?: (Buffer | Uint8Array | string);
}
export interface MasterExecRequest__Output {
    /**
     * 使用 objpack 包装 SerializedFunction，参数为({dispatchWork})，返回任意可序列化结果
     */
    'func'?: (Buffer);
}
