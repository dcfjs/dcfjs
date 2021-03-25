import { StorageServiceClient } from '@dcfjs/proto/dcf/StorageService';
import { protoDescriptor } from './proto';
import * as grpc from '@grpc/grpc-js';
import {
  captureEnv,
  requireModule,
  symbolSerialize,
} from './serializeFunction';
import { Writable, Transform } from 'stream';

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
      this.client.createSession(
        {
          expireAt: Date.now() + this.options.timeout,
        },
        (err, result) => {
          if (err || !result?.sessionId) {
            reject(err);
            return;
          }
          resolve(result.sessionId);
        }
      )
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
  timer: NodeJS.Timer | null = null;
  currentRenew: Promise<void> | null = null;

  constructor(
    client: StorageClient,
    sessionId: string,
    autoRenew: boolean = true
  ) {
    this.client = client;
    this.sessionId = sessionId;
    if (autoRenew) {
      this.autoRenew();
    }
  }

  [symbolSerialize]() {
    const { client, sessionId } = this;
    const dcfc = require('@dcfjs/common');
    return captureEnv(
      () => {
        // Do not renew deserialized session because they may be not properly released.
        return new dcfc.StorageSession(client, sessionId, false);
      },
      {
        client,
        sessionId,
        dcfc: requireModule('@dcfjs/common'),
      }
    );
  }

  async close() {
    this.release();
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

  autoRenew() {
    if (!this.timer) {
      this.timer = setInterval(() => {
        this.renew();
      }, this.client.options.renewInterval);
    }
  }

  release() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
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
          expireAt: Date.now() + this.client.options.timeout,
        },
        (err) => {
          err ? reject(err) : resolve();
        }
      );
    });
    return this.currentRenew.then(() => {
      this.currentRenew = null;
    });
  }

  createWriteStream(key: string) {
    let over = false;
    let finalCB: ((err?: Error | null) => void) | null = null;
    const stream = this.client.client.putFile((err) => {
      over = true;
      if (finalCB) {
        finalCB(err);
      }
    });
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
        stream.end();
      },
    });
    stream.write({
      sessionId: this.sessionId,
      key,
    });
    stream.on('error', (err) => {
      transform.emit('error', err);
    });

    transform.pipe((stream as unknown) as Writable, { end: false });
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
    stream.on('error', () => {});
    return stream.pipe(transform);
  }

  readFile(key: string): Promise<Buffer>;
  readFile(key: string, encoding: BufferEncoding): Promise<string>;
  readFile(key: string, encoding?: BufferEncoding): Promise<Buffer | string> {
    return new Promise<Buffer | string>((resolve, reject) => {
      const stream = this.createReadableStream(key);
      const datas: Buffer[] = [];
      stream.on('error', reject);
      stream.on('readable', () => {
        let buf;
        while ((buf = stream.read())) {
          datas.push(buf);
        }
      });
      stream.on('end', () => {
        const ret = Buffer.concat(datas);
        if (encoding) {
          resolve(ret.toString(encoding));
          return;
        }
        resolve(ret);
      });
    });
  }

  writeFile(key: string, data: Buffer): Promise<void>;
  writeFile(
    key: string,
    data: string,
    encoding?: BufferEncoding
  ): Promise<void>;
  writeFile(
    key: string,
    data: string | Buffer,
    encoding?: BufferEncoding
  ): Promise<void> {
    if (typeof data === 'string') {
      data = Buffer.from(data, encoding);
    }
    return new Promise((resolve, reject) => {
      const stream = this.client.client.putFile((err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
      stream.write({
        sessionId: this.sessionId,
        key,
        data,
      });
      stream.end();
      stream.on('error', reject);
    });
  }
}
