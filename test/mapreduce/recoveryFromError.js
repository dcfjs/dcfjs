const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const { DCFContext } = require('@dcfjs/client');

chai.use(chaiAsPromised);
describe('Recovery from script error.', () => {
  let dcc = new DCFContext({
    defaultPartitions: 4,
  });

  it('Test dcf function error', async () => {
    expect(
      dcc
        .range(0, 100, 4)
        .map((a) => {
          throw new Error('some error');
        })
        .count()
    ).to.be.rejectedWith();
  });
});
