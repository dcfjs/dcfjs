const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const { DCFContext } = require('@dcfjs/client');
const { StorageClient } = require('@dcfjs/common');

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
  it('Test sort', async () => {
    const tmp = [1, 5, 2, 4, 6, 9, 0, 8, 7, 3];
    expect(await dcc.parallelize(tmp).sort().collect()).deep.equals(
      [...tmp].sort()
    );
  });

  it('Test stable sort', async () => {
    const tmp = [
      [1, 1],
      [2, 1],
      [1, 2],
      [2, 2],
      [1, 3],
      [2, 3],
    ];
    expect(
      await dcc
        .parallelize(tmp)
        .sortBy((v) => v[0])
        .collect()
    ).deep.equals([...tmp].sort((a, b) => a[0] - b[0]));

    expect(
      await dcc
        .parallelize(tmp)
        .sortBy((v) => v[0], false)
        .collect()
    ).deep.equals([...tmp].sort((a, b) => b[0] - a[0]));
  });
});
