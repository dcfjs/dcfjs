import { StorageServiceHandlers } from '@dcfjs/proto/dcf/StorageService';
import {
  promises as fs,
  existsSync,
  createWriteStream,
  createReadStream,
} from 'fs';
import * as path from 'path';
import { v4 as uuidv4 } from 'uuid';
import * as grpc from '@grpc/grpc-js';
import { Writable, Transform } from 'stream';

export interface StorageServerOptions {
  baseDir: string;
  force?: boolean;
  cleanUp?: boolean;
}

const VALID_KEY_REG = /^[A-Za-z0-9-_]+$/;

export async function emptyDirectory(dir: string) {
  const files = await fs.readdir(dir, { withFileTypes: true });
  for (const file of files) {
    const subpath = path.join(dir, file.name);
    if (file.isDirectory() && !file.isSymbolicLink()) {
      await emptyDirectory(subpath);
      await fs.rmdir(subpath);
    } else {
      await fs.unlink(subpath);
    }
  }
}

export class StorageSession {
  readonly baseDir: string;
  expireAt: number = Infinity;

  constructor(baseDir: string, expireAt: number) {
    this.baseDir = baseDir;
    this.expireAt = expireAt;
  }
  async init() {
    await fs.mkdir(this.baseDir);
  }
  async cleanUp() {
    await emptyDirectory(this.baseDir);
    await fs.rmdir(this.baseDir);
  }

  createWriteStream(key: string) {
    return createWriteStream(path.join(this.baseDir, key), {
      flags: 'w',
    });
  }

  createReadStream(key: string) {
    return createReadStream(path.join(this.baseDir, key), {
      flags: 'r',
    });
  }
}

export class StorageServicer {
  readonly baseDir: string;
  readonly options: StorageServerOptions;

  sessions: Map<string, StorageSession> = new Map();

  timer: NodeJS.Timer | null = null;

  constructor(options: StorageServerOptions) {
    this.options = options;
    this.baseDir = options.baseDir;
  }

  readonly storageServiceHandlers: StorageServiceHandlers = {
    createSession: async ({ request }, cb) => {
      try {
        const sessionId = await this.createSession(request.expireAt);
        cb(null, {
          sessionId,
        });
      } catch (e) {
        cb(e);
      }
    },
    renewSession: async ({ request }, cb) => {
      try {
        if (!request.sessionId) {
          throw {
            code: grpc.status.INVALID_ARGUMENT,
          };
        }
        this.renewSession(request.sessionId, request.expireAt);
        cb(null);
      } catch (e) {
        cb(e);
      }
    },
    closeSession: async ({ request }, cb) => {
      try {
        if (!request.sessionId) {
          throw {
            code: grpc.status.INVALID_ARGUMENT,
          };
        }
        this.closeSession(request.sessionId);
        cb(null);
      } catch (e) {
        cb(e);
      }
    },
    getFile: async (call) => {
      try {
        await this.getFile(call);
      } catch (e) {
        call.destroy(e);
      }
    },
    putFile: async (call, cb) => {
      try {
        await this.putFile(call);
        cb(null);
      } catch (e) {
        cb(e);
      }
    },
  };

  async init() {
    if (!existsSync(this.baseDir)) {
      // not exists
      await fs.mkdir(this.baseDir);
      return;
    }

    // check that baseDir IS a directory.
    const stat = await fs.stat(this.baseDir);
    if (!stat.isDirectory()) {
      throw new Error(`${this.baseDir} is not a directory`);
    }
    // make sure directory is empty.
    if (this.options.cleanUp) {
      await emptyDirectory(this.baseDir);
    } else if (this.options.force) {
      // skip check
    } else {
      if ((await fs.readdir(this.baseDir)).length > 0) {
        throw new Error(
          `${this.baseDir} is not empty. Try empty it manually or use --force options.`
        );
      }
    }

    this.timer = setInterval(() => {
      this.clearTimeoutSessions();
    }, 1000);
  }

  async cleanUp() {
    if (this.timer) {
      clearInterval(this.timer);
    }
    for (const value of Object.values(this.sessions)) {
      await value.cleanUp();
    }
  }

  async createSession(expireAt?: number) {
    for (let tryCount = 0; tryCount < 10; ++tryCount) {
      const sessionId = uuidv4();
      const dir = path.join(this.baseDir, sessionId);
      if (this.sessions.has(sessionId)) {
        continue;
      }

      const session = new StorageSession(dir, expireAt || Infinity);
      this.sessions.set(sessionId, session);

      await session.init();

      return sessionId;
    }
    // should not happen
    throw new Error('Internal Error.');
  }

  closeSession(sessionId: string) {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw {
        code: grpc.status.NOT_FOUND,
      };
    }
    this.sessions.delete(sessionId);
    session.cleanUp();
  }

  renewSession(sessionId: string, expireAt?: number) {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw {
        code: grpc.status.NOT_FOUND,
      };
    }
    session.expireAt = expireAt || Infinity;
  }

  async putFile(call: Parameters<StorageServiceHandlers['putFile']>[0]) {
    const writable: Writable = await new Promise<Writable>(
      (resolve, reject) => {
        call.once('readable', () => {
          const data = call.read();
          const { sessionId, key } = data;
          if (!sessionId || !key || !VALID_KEY_REG.test(key)) {
            call.destroy();
            return reject({
              code: grpc.status.INVALID_ARGUMENT,
            });
          }
          const session = this.sessions.get(sessionId);
          if (!session) {
            return reject({
              code: grpc.status.NOT_FOUND,
            });
          }
          const writable = session.createWriteStream(key);
          if (data.data) {
            writable.write(data.data);
          }
          resolve(writable);
        });
      }
    );
    return new Promise<void>((resolve, reject) => {
      const transformer = new Transform({
        writableObjectMode: true,
        transform(data, enc, cb) {
          cb(null, data.data);
        },
      });
      call.pipe(transformer).pipe(writable);
      call.on('end', () => {
        resolve();
      });
      call.on('error', reject);
    });
  }

  async getFile(call: Parameters<StorageServiceHandlers['getFile']>[0]) {
    const { request } = call;
    const { sessionId, key } = request;
    if (!sessionId || !key || !VALID_KEY_REG.test(key)) {
      throw {
        code: grpc.status.INVALID_ARGUMENT,
      };
    }
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw {
        code: grpc.status.NOT_FOUND,
      };
    }
    return new Promise<void>((resolve, reject) => {
      const readable = session.createReadStream(key);
      const transformer = new Transform({
        readableObjectMode: true,
        transform(data, enc, cb) {
          cb(null, {
            data,
          });
        },
      });
      readable.pipe(transformer).pipe((call as unknown) as Writable);
      readable.on('end', () => {
        resolve();
      });
      readable.on('error', reject);
    });
  }

  clearTimeoutSessions() {
    const now = Date.now();
    for (const [sessionId, session] of this.sessions.entries()) {
      if (session.expireAt < now) {
        this.sessions.delete(sessionId);
        session.cleanUp();
      }
    }
  }
}
