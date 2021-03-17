// @noCaptureEnv
'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var grpc = require('@grpc/grpc-js');
var common = require('@dcfjs/common');

class DCFMapReduceContext {
    constructor(options) {
        if (options.client) {
            this.client = options.client;
        }
        else {
            this.client = new common.protoDescriptor.dcf.MasterService(options.masterEndpoint || 'localhost:17731', grpc.credentials.createInsecure());
        }
    }
    exec(f) {
        return new Promise((resolve, reject) => {
            this.client.exec({
                func: common.encode(common.serializeFunction(f)),
            }, (err, result) => {
                if (err) {
                    reject(err);
                }
                else {
                    let ret = result?.result;
                    resolve(ret ? common.decode(ret) : ret);
                }
            });
        });
    }
}

exports.DCFMapReduceContext = DCFMapReduceContext;
