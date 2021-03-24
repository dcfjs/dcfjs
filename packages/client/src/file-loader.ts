import { RDDWorkChain, RDDFinalizedWorkChain } from './chain';
import { StorageClient } from '@dcfjs/common';
import { URL } from 'url';

// Every method will run in client.
// only "chain" will be run in remote server.
export interface FileLoader {
  canHandleUrl(baseUri: URL): boolean;

  getReadFileChain(
    baseUrl: URL,
    options: {
      recursive?: boolean;
      storage?: StorageClient;
    }
  ): () => Promise<RDDWorkChain<[string, Buffer][]>>;

  getWriteFileChain(
    baseUrl: URL,
    baseChain: RDDWorkChain<[string, Buffer]>,
    options: {
      overwrite?: boolean;
      storage?: StorageClient;
    }
  ):
    | [RDDFinalizedWorkChain, (result: unknown) => void | Promise<void>]
    | Promise<
        [RDDFinalizedWorkChain, (result: unknown) => void | Promise<void>]
      >;
}
