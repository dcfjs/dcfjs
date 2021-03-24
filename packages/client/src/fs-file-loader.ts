import { StorageClient } from '@dcfjs/common';
import { RDDFinalizedWorkChain, RDDWorkChain } from './chain';
import { FileLoader } from './file-loader';
import { listFiles } from './fs-helper';
import * as fs from 'fs';
import * as dcfc from '@dcfjs/common';
import { URL } from 'url';
import { finalizeChain, mapChain } from './chain';

async function emptyDirectory(dir: URL) {
  const files = await fs.promises.readdir(dir, { withFileTypes: true });
  for (const file of files) {
    const fileUrl = new URL(file.name, dir);
    if (file.isDirectory() && !file.isSymbolicLink()) {
      fileUrl.href += '/';
      await emptyDirectory(fileUrl);
      await fs.promises.rmdir(fileUrl);
    } else {
      await fs.promises.unlink(fileUrl);
    }
  }
}

export class FsFileLoader implements FileLoader {
  canHandleUrl(baseUri: URL): boolean {
    return baseUri.protocol === 'file:';
  }

  getReadFileChain(
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

  async getWriteFileChain(
    baseUrl: URL,
    baseChain: RDDWorkChain<[string, Buffer], void>,
    {
      overwrite,
      storage,
    }: {
      overwrite?: boolean;
      storage?: StorageClient;
    }
  ): Promise<[RDDFinalizedWorkChain, (arg: unknown) => Promise<void>]> {
    if (!storage) {
      throw new Error('No storage available.');
    }
    if (!baseUrl.href.endsWith('/')) {
      baseUrl.href += '/';
    }
    if (fs.existsSync(baseUrl)) {
      if (!overwrite) {
        throw new Error(
          `${baseUrl} already exists, consider use overwrite=true?`
        );
      }
      await emptyDirectory(baseUrl);
    } else {
      await fs.promises.mkdir(baseUrl);
    }

    const session = await storage.startSession();

    const finalChain = finalizeChain(
      mapChain(
        baseChain,
        dcfc.captureEnv(
          async (v, partitionId) => {
            const key = `${partitionId}`;
            await session.writeFile(key, v[1]);
            return [v[0], key] as [string, string];
          },
          {
            session,
          }
        )
      ),
      (v) => v,
      (t) => `${t}.save()`
    );

    const fetchFiles = async (v: [string, string][]): Promise<void> => {
      await Promise.all(
        v.map(([fn, key]) => {
          const writable = fs.createWriteStream(new URL(fn, baseUrl));
          const readable = session.createReadableStream(key);
          readable.pipe(writable);
          return new Promise((resolve, reject) => {
            readable.on('error', reject);
            writable.on('error', reject);
            writable.on('finish', resolve);
          });
        })
      );

      await session.close();
    };

    return [
      finalChain as RDDFinalizedWorkChain,
      fetchFiles as (arg: unknown) => Promise<void>,
    ];
  }
}
