const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const { DCFContext } = require('@dcfjs/client');

chai.use(chaiAsPromised);

describe('MapReduce With local worker', () => {
  let dcc = new DCFContext({
    defaultPartitions: 4,
  });

  it('Test range count', async () => {
    expect(await dcc.range(100).count()).to.equals(100);
    expect(await dcc.range(0, 100).count()).to.equals(100);
    expect(await dcc.range(0, 100, 4).count()).to.equals(25);
  });

  it('Test range content', async () => {
    expect(await dcc.range(100).collect()).deep.equals(
      new Array(100).fill(0).map((v, i) => i)
    );
    expect(await dcc.range(0, 100).collect()).deep.equals(
      new Array(100).fill(0).map((v, i) => i)
    );
    expect(await dcc.range(0, 100, 4).collect()).deep.equals(
      new Array(25).fill(0).map((v, i) => i * 4)
    );
  });

  it('Test union', async () => {
    expect(
      await dcc.range(10).union(dcc.range(10, 20), dcc.range(20, 30)).collect()
    ).deep.equals(await dcc.range(30).collect());
  });

  it('Test take', async () => {
    const source = dcc.range(100);
    expect(await source.take(30)).deep.equals(await dcc.range(30).collect());

    const source2 = dcc.range(10);
    expect(await source2.take(30)).deep.equals([...Array(10).keys()]);

    const source3 = dcc.emptyRDD();
    expect(await source3.take(30)).deep.equals([]);
  });
});
