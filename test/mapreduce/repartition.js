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

  it('Test repartition', async () => {
    const max = 10000;
    const tmp = await dcc.range(0, max).repartition(16).cache();

    expect((await tmp.getNumPartitions()) === 16);
    expect(await tmp.count()).equals(max);
    expect(await tmp.max()).equals(max - 1);
    expect(await tmp.min()).equals(0);
    expect(await tmp.reduce((a, b) => a + b)).equals((max * (max - 1)) / 2);
    await tmp.unpersist();
  });

  it('Test distinct', async () => {
    const max = 100000;
    const tmp = dcc
      .range(0, max)
      .map((v) => 1)
      .distinct();
    expect(await tmp.collect()).deep.equals([1]);

    const tmp2 = dcc.range(0, max / 10).map((v) => 4);
    const tmp3 = dcc.range(0, max / 10).map((v) => 8);
    const tmp4 = dcc.range(0, max / 10).filter((v) => false);
    const tmp5 = tmp2.union(tmp3).union(tmp4).distinct();
    const res = await tmp5.collect();

    expect(res.sort()).deep.equals([4, 8]);
  });
});
