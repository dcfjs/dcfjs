import { StorageServiceClient } from '@dcfjs/proto/dcf/StorageService';
import { protoDescriptor } from './proto';
import * as grpc from '@grpc/grpc-js';
import {
  captureEnv,
  requireModule,
  symbolSerialize,
} from './serializeFunction';
import { Readable, Writable, Transform } from 'stream';

export interface StorageClientOptions {
  endpoint: string;
  timeout: number;
  renewInterval: number;
}

export class StorageClient {
  readonly client: StorageServiceClient;
  readonly options: StorageClientOptions;
  constructor(options: StorageClientOptions) {
    this.options = options;
    this.client = new protoDescriptor.dcf.StorageService(
      options.endpoint,
      grpc.credentials.createInsecure()
    );
  }

  [symbolSerialize]() {
    const { options } = this;
    const dcfc = require('@dcfjs/common');
    return captureEnv(
      () => {
        return new dcfc.StorageClient(options);
      },
      {
        options,
        dcfc: requireModule('@dcfjs/common'),
      }
    );
  }

  async startSession() {
    const sessionId = await new Promise<string>((resolve, reject) =>
      this.client.createSession({}, (err, result) => {
        if (err || !result?.sessionId) {
          reject(err);
          return;
        }
        resolve(result.sessionId);
      })
    );
    return new StorageSession(this, sessionId);
  }

  async withSession(f: (session: StorageSession) => void | Promise<void>) {
    const session = await this.startSession();
    try {
      await f(session);
    } finally {
      session.close();
    }
  }
}

export class StorageSession {
  client: StorageClient;
  sessionId: string;
  timer: NodeJS.Timer;
  currentRenew: Promise<void> | null = null;

  constructor(client: StorageClient, sessionId: string) {
    this.client = client;
    this.sessionId = sessionId;
    this.timer = setInterval(() => {
      this.renew();
    }, client.options.renewInterval);
  }

  [symbolSerialize]() {
    const { client, sessionId } = this;
    const dcfc = require('@dcfjs/common');
    return captureEnv(
      () => {
        return new dcfc.StorageSession(client, sessionId);
      },
      {
        client,
        sessionId,
        dcfc: requireModule('@dcfjs/common'),
      }
    );
  }

  async close() {
    clearInterval(this.timer);
    if (this.currentRenew) {
      await this.currentRenew;
    }
    return new Promise<void>((resolve, reject) => {
      this.client.client.closeSession(
        {
          sessionId: this.sessionId,
        },
        (err) => {
          err ? reject(err) : resolve();
        }
      );
    });
  }

  renew() {
    if (this.currentRenew) {
      console.warn('Last renew was not finished, something may goes wrong.');
      return;
    }
    this.currentRenew = new Promise<void>((resolve, reject) => {
      this.client.client.renewSession(
        {
          sessionId: this.sessionId,
        },
        (err) => {
          err ? reject(err) : resolve();
        }
      );
    });
    return this.currentRenew.then(() => {
      this.currentRenew = null;
      console.log('Here', this.currentRenew);
    });
  }

  createWriteStream(key: string) {
    let over = false;
    let finalCB: ((err?: Error | null) => void) | null = null;
    const transform = new Transform({
      readableObjectMode: true,
      transform(data, enc, cb) {
        cb(null, {
          data,
        });
      },
      final(cb) {
        if (over) {
          cb(new Error('Stream is already over.'));
          return;
        }
        finalCB = cb;
      },
    });
    const stream = this.client.client.putFile((err) => {
      over = true;
      if (finalCB) {
        finalCB(err);
      }
    });
    stream.write({
      sessionId: this.sessionId,
      key,
    });

    transform.pipe((stream as unknown) as Writable);
    return transform as Writable;
  }

  createReadableStream(key: string) {
    const stream = this.client.client.getFile({
      sessionId: this.sessionId,
      key,
    });
    const transform = new Transform({
      writableObjectMode: true,
      transform(data, enc, cb) {
        cb(null, data.data);
      },
    });
    return stream.pipe(transform);
  }
}
