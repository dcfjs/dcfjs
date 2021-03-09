import * as protoLoader from '@grpc/proto-loader';
import * as grpc from '@grpc/grpc-js';
import { ProtoGrpcType } from '@dcfjs/proto';

export const PROTO_ROOT_FN = require.resolve('@dcfjs/proto/proto/index.proto');

export const packageDefinition = protoLoader.loadSync(PROTO_ROOT_FN);
export const protoDescriptor = (grpc.loadPackageDefinition(
  packageDefinition
) as unknown) as ProtoGrpcType;
