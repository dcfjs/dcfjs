import type * as grpc from '@grpc/grpc-js';
import type { WorkerExecRequest as _dcf_WorkerExecRequest, WorkerExecRequest__Output as _dcf_WorkerExecRequest__Output } from '../dcf/WorkerExecRequest';
import type { WorkerExecResponse as _dcf_WorkerExecResponse, WorkerExecResponse__Output as _dcf_WorkerExecResponse__Output } from '../dcf/WorkerExecResponse';
export interface WorkerServiceClient extends grpc.Client {
    exec(argument: _dcf_WorkerExecRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_WorkerExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_WorkerExecRequest, metadata: grpc.Metadata, callback: (error?: grpc.ServiceError, result?: _dcf_WorkerExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_WorkerExecRequest, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_WorkerExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_WorkerExecRequest, callback: (error?: grpc.ServiceError, result?: _dcf_WorkerExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_WorkerExecRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_WorkerExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_WorkerExecRequest, metadata: grpc.Metadata, callback: (error?: grpc.ServiceError, result?: _dcf_WorkerExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_WorkerExecRequest, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_WorkerExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_WorkerExecRequest, callback: (error?: grpc.ServiceError, result?: _dcf_WorkerExecResponse__Output) => void): grpc.ClientUnaryCall;
}
export interface WorkerServiceHandlers extends grpc.UntypedServiceImplementation {
    exec: grpc.handleUnaryCall<_dcf_WorkerExecRequest__Output, _dcf_WorkerExecResponse>;
}
