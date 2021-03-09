import yargs from 'yargs';
import * as grpc from '@grpc/grpc-js';
import Graceful from 'node-graceful';
import { createMasterServer } from '.';

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
  .parse();

async function startServer() {
  const server = createMasterServer();

  const port = await new Promise((resolve, reject) =>
    server.bindAsync(
      `${options.host}:${options.port}`,
      grpc.ServerCredentials.createInsecure(),
      (err, port) => (err ? reject(err) : resolve(port))
    )
  );

  console.log(`Listening at ${options.host}:${port}`);
  Graceful.on('exit', () => {
    console.log('Exiting...');
    return new Promise((resolve, reject) => {
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
