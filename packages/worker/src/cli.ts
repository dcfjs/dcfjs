import yargs from 'yargs';
import * as grpc from '@grpc/grpc-js';
import Graceful from 'node-graceful';
import { createWorkerServer } from '.';
import { protoDescriptor } from '@dcfjs/common';

const options = yargs(process.argv.slice(2))
  .option('port', {
    alias: 'p',
    type: 'string',
    description: 'Bind server port',
    default: '0',
  })
  .option('host', {
    alias: 'h',
    type: 'string',
    description: 'Bind server host',
    default: '127.0.0.1',
  })
  .option('master', {
    alias: 'm',
    type: 'string',
    description: 'Master endpoint',
    default: '127.0.0.1:17731',
  })
  .parse();

function createMasterClient() {
  return new protoDescriptor.dcf.MasterService(
    options.master,
    grpc.credentials.createInsecure()
  );
}

async function register(endpoint: string) {
  const client = createMasterClient();
  await new Promise((resolve, reject) => {
    client.registerWorker(
      {
        endpoint,
      },
      (err) => (err ? reject(err) : resolve(undefined))
    );
  });
  const channel = client.getChannel();
  const state = channel.getConnectivityState(false);
  if (state !== grpc.connectivityState.READY) {
    throw new Error('Bad connection state');
  }
  channel.watchConnectivityState(state, Infinity, () => {
    Graceful.exit();
  });
}

async function unregister(endpoint: string) {
  const client = createMasterClient();
  await new Promise((resolve, reject) => {
    client.unregisterWorker(
      {
        endpoint,
      },
      (err) => (err ? reject(err) : resolve(undefined))
    );
  });
  client.close();
}

async function startServer() {
  const server = createWorkerServer();

  const port = await new Promise((resolve, reject) =>
    server.bindAsync(
      `${options.host}:${options.port}`,
      grpc.ServerCredentials.createInsecure(),
      (err, port) => (err ? reject(err) : resolve(port))
    )
  );
  server.start();

  const endpoint = `${options.host}:${port}`;
  console.log(`Worker listening at ${options.host}:${port}`);
  await register(endpoint);
  console.log('Registered.');

  Graceful.on('exit', async () => {
    console.log('Unregistering...');
    await unregister(endpoint);
    console.log('Exiting...');
    await new Promise((resolve, reject) => {
      server.tryShutdown((error) => {
        if (error) {
          reject(error);
        } else {
          resolve(null);
        }
      });
    });
  });
}

startServer();
