const chai = require('chai');
const { expect } = chai;
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
    expect(
      (
        await dcc
          .binaryFiles(path.resolve(__dirname, '../testdata/'))
          .map((v) => [v[0], v[1].length])
          .collect()
      ).map((v) => v[1])
    ).deep.equals([378113, 422278]);

    expect(
      (
        await dcc
          .binaryFiles(path.resolve(__dirname, '../testdata/rfc2068.txt'))
          .map((v) => [v[0], v[1].length])
          .collect()
      ).map((v) => v[1])
    ).deep.equals([378113]);
  });
});
