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
});
