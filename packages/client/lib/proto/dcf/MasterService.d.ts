import type * as grpc from '@grpc/grpc-js';
import type { MasterExecRequest as _dcf_MasterExecRequest, MasterExecRequest__Output as _dcf_MasterExecRequest__Output } from '../dcf/MasterExecRequest';
import type { MasterExecResponse as _dcf_MasterExecResponse, MasterExecResponse__Output as _dcf_MasterExecResponse__Output } from '../dcf/MasterExecResponse';
import type { RegisterWorkerRequest as _dcf_RegisterWorkerRequest, RegisterWorkerRequest__Output as _dcf_RegisterWorkerRequest__Output } from '../dcf/RegisterWorkerRequest';
import type { RegisterWorkerResponse as _dcf_RegisterWorkerResponse, RegisterWorkerResponse__Output as _dcf_RegisterWorkerResponse__Output } from '../dcf/RegisterWorkerResponse';
import type { UnregisterWorkerRequest as _dcf_UnregisterWorkerRequest, UnregisterWorkerRequest__Output as _dcf_UnregisterWorkerRequest__Output } from '../dcf/UnregisterWorkerRequest';
import type { UnregisterWorkerResponse as _dcf_UnregisterWorkerResponse, UnregisterWorkerResponse__Output as _dcf_UnregisterWorkerResponse__Output } from '../dcf/UnregisterWorkerResponse';
export interface MasterServiceClient extends grpc.Client {
    exec(argument: _dcf_MasterExecRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_MasterExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_MasterExecRequest, metadata: grpc.Metadata, callback: (error?: grpc.ServiceError, result?: _dcf_MasterExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_MasterExecRequest, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_MasterExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_MasterExecRequest, callback: (error?: grpc.ServiceError, result?: _dcf_MasterExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_MasterExecRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_MasterExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_MasterExecRequest, metadata: grpc.Metadata, callback: (error?: grpc.ServiceError, result?: _dcf_MasterExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_MasterExecRequest, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_MasterExecResponse__Output) => void): grpc.ClientUnaryCall;
    exec(argument: _dcf_MasterExecRequest, callback: (error?: grpc.ServiceError, result?: _dcf_MasterExecResponse__Output) => void): grpc.ClientUnaryCall;
    registerWorker(argument: _dcf_RegisterWorkerRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_RegisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    registerWorker(argument: _dcf_RegisterWorkerRequest, metadata: grpc.Metadata, callback: (error?: grpc.ServiceError, result?: _dcf_RegisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    registerWorker(argument: _dcf_RegisterWorkerRequest, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_RegisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    registerWorker(argument: _dcf_RegisterWorkerRequest, callback: (error?: grpc.ServiceError, result?: _dcf_RegisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    registerWorker(argument: _dcf_RegisterWorkerRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_RegisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    registerWorker(argument: _dcf_RegisterWorkerRequest, metadata: grpc.Metadata, callback: (error?: grpc.ServiceError, result?: _dcf_RegisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    registerWorker(argument: _dcf_RegisterWorkerRequest, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_RegisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    registerWorker(argument: _dcf_RegisterWorkerRequest, callback: (error?: grpc.ServiceError, result?: _dcf_RegisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    unregisterWorker(argument: _dcf_UnregisterWorkerRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_UnregisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    unregisterWorker(argument: _dcf_UnregisterWorkerRequest, metadata: grpc.Metadata, callback: (error?: grpc.ServiceError, result?: _dcf_UnregisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    unregisterWorker(argument: _dcf_UnregisterWorkerRequest, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_UnregisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    unregisterWorker(argument: _dcf_UnregisterWorkerRequest, callback: (error?: grpc.ServiceError, result?: _dcf_UnregisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    unregisterWorker(argument: _dcf_UnregisterWorkerRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_UnregisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    unregisterWorker(argument: _dcf_UnregisterWorkerRequest, metadata: grpc.Metadata, callback: (error?: grpc.ServiceError, result?: _dcf_UnregisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    unregisterWorker(argument: _dcf_UnregisterWorkerRequest, options: grpc.CallOptions, callback: (error?: grpc.ServiceError, result?: _dcf_UnregisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
    unregisterWorker(argument: _dcf_UnregisterWorkerRequest, callback: (error?: grpc.ServiceError, result?: _dcf_UnregisterWorkerResponse__Output) => void): grpc.ClientUnaryCall;
}
export interface MasterServiceHandlers extends grpc.UntypedServiceImplementation {
    exec: grpc.handleUnaryCall<_dcf_MasterExecRequest__Output, _dcf_MasterExecResponse>;
    registerWorker: grpc.handleUnaryCall<_dcf_RegisterWorkerRequest__Output, _dcf_RegisterWorkerResponse>;
    unregisterWorker: grpc.handleUnaryCall<_dcf_UnregisterWorkerRequest__Output, _dcf_UnregisterWorkerResponse>;
}
