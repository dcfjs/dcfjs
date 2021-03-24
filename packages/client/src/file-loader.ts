import { RDDWorkChain } from './chain';
import { StorageClient } from '@dcfjs/common';
import { URL } from 'url';

// Every method will run in client.
// only "chain" will be run in remote server.
export interface FileLoader {
  canHandleUrl(baseUri: URL): boolean;

  getChainFactory(
    baseUri: URL,
    options: {
      recursive?: boolean;
      storage?: StorageClient;
    }
  ): () => Promise<RDDWorkChain<[string, Buffer][]>>;
}
