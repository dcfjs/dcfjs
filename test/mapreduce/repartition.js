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

  it('Test repartitionBy', async () => {
    const max = 100000;
    const tmp = dcc.range(0, max).partitionBy(3, (v) => v % 3);

    expect(await tmp.count()).equals(max);
    const compare = [];
    for (let i = 0; i < 3; i++) {
      compare[i] = [];
      for (let j = 0; j < max; j++) {
        if (j % 3 === i) {
          compare[i].push(j);
        }
      }
    }
    expect(await tmp.glom().collect()).deep.equals(compare);
  });

  it('Test coalesce', async () => {
    const max = 10000;
    const tmp = dcc.range(0, max, undefined, 4);
    const tmp1 = tmp.coalesce(5);

    expect(await tmp.collect()).deep.equals(await tmp1.collect());
    expect((await tmp1.getNumPartitions()) === 5);
    const sizes = await tmp1
      .glom()
      .map((v) => v.length)
      .collect();
    expect(sizes.every((v) => v >= 1990 && v <= 2010)).to.equals(true);
  });
});
