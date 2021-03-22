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

  close() {
    this.client.close();
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
    return stream.pipe(transform);
  }

  readFile(key: string): Promise<Buffer>;
  readFile(key: string, encoding: BufferEncoding): Promise<string>;
  readFile(key: string, encoding?: BufferEncoding): Promise<Buffer | string> {
    const stream = this.createReadableStream(key);
    const datas: Buffer[] = [];
    stream.on('readable', () => {
      let buf;
      while ((buf = stream.read())) {
        datas.push(buf);
      }
    });
    return new Promise<Buffer | string>((resolve, reject) => {
      stream.on('end', () => {
        const ret = Buffer.concat(datas);
        if (encoding) {
          resolve(ret.toString(encoding));
          return;
        }
        resolve(ret);
      });
      stream.on('error', reject);
    });
  }

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
