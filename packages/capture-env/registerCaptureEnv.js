const { addHook } = require('pirates');
const { transformCode } = require('./transformCode.js');

const DEFAULT_EXTENSIONS = Object.freeze([
  '.js',
  '.jsx',
  '.es6',
  '.es',
  '.mjs',
]);

addHook((code) => transformCode(code), {
  exts: DEFAULT_EXTENSIONS,
});
