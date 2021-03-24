const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const { DCFContext } = require('@dcfjs/client');
const { StorageClient } = require('@dcfjs/common');
const path = require('path');
const zlib = require('zlib');
const fs = require('fs');

chai.use(chaiAsPromised);

function recursiveRemoveSync(dirPath) {
  if (!dirPath.endsWith('/')) {
    dirPath = dirPath + '/';
  }

  const files = fs.readdirSync(dirPath);
  files.forEach(function (file) {
    if (fs.statSync(dirPath + file).isDirectory()) {
      recursiveRemoveSync(dirPath + file + '/');
      fs.rmdirSync(dirPath + file + '/');
    } else {
      fs.unlinkSync(dirPath + file);
    }
  });
}

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

  after(() => {
    const testFolderPath = path.resolve('./fsLoaderTest/');
    if (fs.existsSync(testFolderPath)) {
      recursiveRemoveSync(testFolderPath);
    }
    fs.rmdirSync(testFolderPath);
  });

  it('Test loaders', async () => {
    expect(
      await dcc.textFile(path.resolve(__dirname, '../testdata/')).count()
    ).to.be.equals(18934);
  });

  it('Test save&load with compress', async () => {
    const max = 10000;
    if (!fs.existsSync('./fsLoaderTest')) {
      fs.mkdirSync('./fsLoaderTest');
    }
    for (let i = 0; i < 3; i++) {
      const absPath = path.resolve('./fsLoaderTest/test' + i);

      await dcc
        .range(0, max)
        .repartition(10)
        .map((v) => `${v}`)
        .saveAsTextFile(absPath, {
          overwrite: true,
          extension: 'gz',
          compressor: (buf) => {
            return zlib.gzipSync(buf);
          },
        });
    }

    const absPath = path.resolve('./fsLoaderTest/');
    const tmp = dcc
      .textFile(absPath, {
        recursive: true,
        decompressor: (buf) => {
          // Skip empty file.
          if (buf.length === 0) {
            return buf;
          }
          return zlib.gunzipSync(buf);
        },
      })
      .map((v) => parseInt(v));

    expect(await tmp.count()).equals(max * 3);
    expect(await tmp.max()).equals(max - 1);
    expect(await tmp.min()).equals(0);
    expect(await tmp.reduce((a, b) => a + b)).equals(49995000 * 3);
  }).timeout(10000);
});
