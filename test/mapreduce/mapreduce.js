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

  it('Test max', async () => {
    const source = dcc.range(1000);
    expect(await source.max()).deep.equals(999);

    const source2 = dcc.emptyRDD();
    expect(await source2.max()).deep.equals(undefined);

    const source3 = dcc.range(400).union(dcc.emptyRDD());
    expect(await source3.max()).deep.equals(399);
  });

  it('Test min', async () => {
    const source = dcc.range(120, 1000);
    expect(await source.min()).deep.equals(120);

    const source2 = dcc.emptyRDD();
    expect(await source2.min()).deep.equals(undefined);

    const source3 = dcc.range(120, 400).union(dcc.emptyRDD());
    expect(await source3.min()).deep.equals(120);
  });

  it('Test map', async function () {
    let target = 4;
    const source1 = dcc.range(10000);
    expect(await source1.map((v) => target).collect()).deep.equals(
      new Array(10000).fill(4)
    );

    const source2 = dcc.emptyRDD();
    expect(await source2.map((v) => 1).collect()).deep.equals([]);
  });

  it('Test flatMap', async function () {
    const mapFunc = () => new Array(10).fill(1);

    const source = dcc.range(100);
    expect(await source.flatMap(mapFunc).collect()).deep.equals(
      new Array(1000).fill(1)
    );

    const source2 = dcc.emptyRDD();
    expect(await source2.flatMap(mapFunc).collect()).deep.equals([]);

    const source3 = dcc.range(120).map((v) => (v < 30 ? 1 : -1));
    expect(
      await source3
        .flatMap((v) => (v === 1 ? new Array(10).fill(-1) : []))
        .collect()
    ).deep.equals(new Array(300).fill(-1));
  });

  it('Test filter', async function () {
    this.timeout(1000);

    const source1 = dcc.range(1000);
    expect(await source1.filter((v) => v < 100).collect()).deep.equals([
      ...Array(100).keys(),
    ]);

    const source2 = dcc.range(1000);
    expect(await source2.filter((v) => false).collect()).deep.equals([]);

    const source3 = dcc.emptyRDD();
    expect(await source3.filter((v) => true).collect()).deep.equals([]);
  });

  it('Test reduce', async function () {
    const source1 = dcc.range(10000).map((v) => 1);
    expect(await source1.reduce((a, b) => a + b)).deep.equals(10000);

    const source2 = dcc.range(1).map((v) => 1);
    expect(await source2.reduce((a, b) => a + b)).deep.equals(1);

    const source3 = dcc.emptyRDD();

    expect(await source3.reduce((a, b) => a + b)).deep.equals(undefined);
  });

  it('Test mapPartition', async function () {
    const source1 = dcc.range(10000);
    const res1 = await source1
      .mapPartitions((v) => new Array(v.length).fill(-1))
      .collect();
    expect(res1).deep.equals(new Array(10000).fill(-1));

    const source2 = dcc.emptyRDD();
    const res2 = await source2
      .mapPartitions((v) => new Array(v.length).fill(-1))
      .collect();
    expect(res2).deep.equals([]);

    const source3 = dcc.range(1);
    const res3 = await source3
      .mapPartitions((v) => new Array(v.length).fill(-1))
      .collect();
    expect(res3).deep.equals([-1]);
  });

  it('Test glom', async function () {
    const source = dcc.range(10000);
    const res = await source.mapPartitions((v) => [v]).collect();
    const tester = await source.glom().collect();

    expect(res).deep.equals(tester);
  });
});
