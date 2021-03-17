const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const { DCFMapReduceContext } = require('@dcfjs/client');

chai.use(chaiAsPromised);

function isPrime(v) {
  if (v < 2) {
    return false;
  }
  const max = Math.sqrt(v) + 0.5;
  for (let i = 2; i < max; i++) {
    if (v % i === 0) {
      return false;
    }
  }
  return true;
}

describe('Execute function With local worker', () => {
  it('Prime test', async () => {
    const client = await new DCFMapReduceContext();

    const primeCount = await client.executeByPartition(
      100,
      (i) => () => {
        const from = i * 10000;
        const to = (i + 1) * 10000;

        let ret = 0;

        for (let i = from; i < to; i++) {
          if (isPrime(i)) {
            ret++;
          }
        }
        return ret;
      },
      (counts) => {
        const ret = counts.reduce((a, b) => a + b);
        return ret;
      }
    );
    let ret = 0;
    for (let i = 0; i < 999999; i++) {
      if (isPrime(i)) {
        ret++;
      }
    }
    expect(primeCount).equals(ret);
  });
});
