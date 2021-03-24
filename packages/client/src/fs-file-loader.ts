import { StorageClient } from '@dcfjs/common';
import { RDDWorkChain } from './chain';
import { FileLoader } from './file-loader';
import { listFiles } from './fs-helper';
import * as fs from 'fs';
import * as dcfc from '@dcfjs/common';
import { URL } from 'url';

export class FsFileLoader implements FileLoader {
  canHandleUrl(baseUri: URL): boolean {
    return baseUri.protocol === 'file:';
  }

  getChainFactory(
    baseUri: URL,
    {
      recursive = false,
      storage,
    }: {
      recursive?: boolean;
      storage?: StorageClient;
    }
  ): () => Promise<RDDWorkChain<[string, Buffer][], void>> {
    if (!storage) {
      throw new Error('No storage available.');
    }
    const finalStorage = storage;
    return async (metaonly: boolean = false) => {
      const session = await finalStorage.startSession();
      const files = listFiles(baseUri, recursive);

      if (metaonly) {
        return {
          n: files.length,
          p: () => () => [],
          t: 'binaryFiles()',
          d: [],
        };
      }

      const keys = await Promise.all(
        files.map(async (file, i) => {
          const key = `${i}`;
          const writable = session.createWriteStream(key);
          const readable = fs.createReadStream(new URL(file));
          readable.pipe(writable);
          await new Promise((resolve, reject) => {
            readable.on('error', reject);
            writable.on('error', reject);
            writable.on('finish', resolve);
          });
          return key;
        })
      );
      session.release();

      return {
        n: files.length,
        p: dcfc.captureEnv(
          (partitionId) => {
            const file = files[partitionId];
            const key = keys[partitionId];
            return dcfc.captureEnv(
              async () => {
                return [
                  [file, await session.readFile(key)] as [string, Buffer],
                ];
              },
              {
                file,
                key,
                session,
              }
            );
          },
          {
            files,
            keys,
            session,
            dcfc: dcfc.requireModule('@dcfjs/common'),
          }
        ),
        t: 'binaryFiles()',
        d: [
          {
            n: 0,
            p: () => () => [],
            t: 'binaryFiles()',
            d: [],
            i: dcfc.captureEnv(
              () => {
                session.autoRenew();
              },
              {
                session,
              }
            ),
            f: () => {},
            c: dcfc.captureEnv(
              () => {
                return session.close();
              },
              {
                session,
              }
            ),
          },
        ],
      };
    };
  }
}
