import yargs from 'yargs';
import { protoDescriptor } from '@dcfjs/common';
import * as grpc from '@grpc/grpc-js';
import { StorageServicer } from './index';
import Graceful from 'node-graceful';

const options = yargs(process.argv.slice(2))
  .option('port', {
    alias: 'p',
    type: 'string',
    description: 'Bind server port',
    default: '17741',
  })
  .option('host', {
    alias: 'h',
    type: 'string',
    description: 'Bind server host',
    default: '127.0.0.1',
  })
  .option('baseDir', {
    alias: 'd',
    type: 'string',
    description: 'Base directory to storage data',
    default: './storage',
  })
  .option('force', {
    type: 'boolean',
    description:
      'Start storage service even if storage directory is not empty.',
    default: false,
  })
  .option('cleanUp', {
    type: 'boolean',
    description:
      'Auto clean up directory content without any prompt when start storage server.',
    default: false,
  })
  .parse();

async function startServer() {
  const server = new grpc.Server();
  const servicer = new StorageServicer(options);
  console.log('Initializing...');
  await servicer.init();
  server.addService(
    protoDescriptor.dcf.StorageService.service,
    servicer.storageServiceHandlers
  );
  console.log('Ready.');

  const port = await new Promise((resolve, reject) =>
    server.bindAsync(
      `${options.host}:${options.port}`,
      grpc.ServerCredentials.createInsecure(),
      (err, port) => (err ? reject(err) : resolve(port))
    )
  );

  server.start();
  console.log(`Storage listening at ${options.host}:${port}`);
  Graceful.on('exit', async () => {
    console.log('Exiting server...');
    await new Promise((resolve, reject) => {
      server.tryShutdown((error) => {
        if (error) {
          reject(error);
        } else {
          resolve(null);
        }
      });
    });
    console.log('Cleaning up...');
    await servicer.cleanUp();
  });
  return server;
}

startServer().catch((e) => {
  setTimeout(() => {
    throw e;
  });
});
