const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const { expect } = chai;
const { StorageClient } = require('@dcfjs/common');

chai.use(chaiAsPromised);

describe('Storage Test', () => {
  const client = new StorageClient({
    endpoint: 'localhost:17741',
    timeout: 600 * 1000,
    renewInterval: 60 * 1000,
  });

  it('Write&Read Test', async () => {
    const data = 'Hello, world!';
    const session = await client.startSession();
    const writable = session.createWriteStream('test');
    writable.write(data);
    writable.end();

    const readable = session.createReadableStream('test');
    const buffers = [];
    readable.on('data', (buf) => {
      buffers.push(buf);
    });
    await new Promise((resolve, reject) => {
      readable.on('end', resolve);
      readable.on('error', reject);
    });
    const result = Buffer.concat(buffers).toString('utf-8');
    expect(result).to.equal(data);

    await session.close();
  });

  it('getFile&writeFile Test', async () => {
    const data = 'Hello, world!';
    const session = await client.startSession();
    await session.writeFile('test', data);
    const result = await session.readFile('test', 'utf-8');
    expect(result).to.equal(data);

    await session.close();
  });

  it('Not found test', async () => {
    const session = await client.startSession();

    expect(session.readFile('not_exists')).to.be.rejectedWith();

    await session.close();
  });
});
