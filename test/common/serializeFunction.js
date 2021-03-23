const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const { expect } = chai;
const { DCFContext } = require('@dcfjs/client');
const { StorageClient } = require('@dcfjs/common');

chai.use(chaiAsPromised);

describe('SerializeFunction', () => {
  const client = new DCFContext();

  it('SimpleFunction', async () => {
    const f = () => 1;
    expect(await client.execute(f)).to.equal(1);
  });

  it('AutoEnv', async () => {
    const a = 1;
    const f = () => a;
    expect(await client.execute(f)).to.equal(1);
  });

  it('AutoEnvFunction', async () => {
    const a = 1;
    function f() {
      return a;
    }
    expect(await client.execute(f)).to.equal(1);
  });

  it(`AutoEnvWithUpFunction`, async () => {
    const a = 1;
    function f() {
      return a;
    }
    function g() {
      return f();
    }
    expect(await client.execute(g)).to.equal(1);
  });

  it('MutateUpValue', async () => {
    let a = 0;
    const f = () => (a += 1);
    expect(client.execute(f)).to.be.rejectedWith();
  });

  it('localRequire', async () => {
    const f = () => require('os').cpus()[0].model;
    expect(await client.execute(f)).to.equal(f());
  });

  it('Override global name', async () => {
    const Buffer = 1;
    const f = () => Buffer;
    expect(await client.execute(f)).to.equal(1);
  });

  it('Instance cache', async () => {
    const a = {};
    const b = a;
    const c = {};
    expect(await client.execute(() => a === b)).to.equal(true);
    expect(await client.execute(() => a === c)).to.equal(false);
  });

  it('Serialize StorageClient', async () => {
    const a = new StorageClient({
      endpoint: 'localhost:17741',
      timeout: 600000,
      renewInterval: 60000,
    });
    expect(await client.execute(() => a.constructor.name)).to.equal(
      StorageClient.name
    );
    expect(await client.execute(() => a.endpoint)).to.equal(a.endpoint);
  });

  // This test case is not expected, because Buffer was changed(undefined -> 1) after function declared.
  // it('Override global name by var', () => {
  //   const f = () => Buffer;
  //   var Buffer = 1;
  //   const f1 = deserializeFunction(serializeFunction(f));
  //   expect(f1()).to.equal(f());
  // });
});
