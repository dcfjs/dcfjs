const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const { DCFContext } = require('@dcfjs/client');
const { StorageClient } = require('@dcfjs/common');
const path = require('path');

chai.use(chaiAsPromised);

describe('MapReduce With local worker', () => {
  const storage = new StorageClient({
    endpoint: 'localhost:17741',
    timeout: 600 * 1000,
    renewInterval: 60 * 1000,
  });
  const dcc = new DCFContext({
    defaultPartitions: 4,
    storage,
  });

  it('Test loaders', async () => {
    console.log(
      await dcc
        .binaryFiles(path.resolve(__dirname, '../testdata'))
        .map((v) => [v[0], v[1].length])
        .collect()
    );
  });
});
