import type * as grpc from '@grpc/grpc-js';
import type { ServiceDefinition, EnumTypeDefinition, MessageTypeDefinition } from '@grpc/proto-loader';
import type { MasterServiceClient as _dcf_MasterServiceClient } from './dcf/MasterService';
import type { WorkerServiceClient as _dcf_WorkerServiceClient } from './dcf/WorkerService';
declare type SubtypeConstructor<Constructor extends new (...args: any) => any, Subtype> = {
    new (...args: ConstructorParameters<Constructor>): Subtype;
};
export interface ProtoGrpcType {
    dcf: {
        MasterExecRequest: MessageTypeDefinition;
        MasterExecResponse: MessageTypeDefinition;
        MasterService: SubtypeConstructor<typeof grpc.Client, _dcf_MasterServiceClient> & {
            service: ServiceDefinition;
        };
        RegisterWorkerRequest: MessageTypeDefinition;
        RegisterWorkerResponse: MessageTypeDefinition;
        UnregisterWorkerRequest: MessageTypeDefinition;
        UnregisterWorkerResponse: MessageTypeDefinition;
        WorkerExecRequest: MessageTypeDefinition;
        WorkerExecResponse: MessageTypeDefinition;
        WorkerService: SubtypeConstructor<typeof grpc.Client, _dcf_WorkerServiceClient> & {
            service: ServiceDefinition;
        };
        WorkerStatus: EnumTypeDefinition;
    };
}
export {};
