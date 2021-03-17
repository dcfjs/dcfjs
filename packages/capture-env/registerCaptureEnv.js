const { addHook } = require('pirates');
const { transformCode } = require('./transformCode.js');

const DEFAULT_EXTENSIONS = Object.freeze([
  '.js',
  '.jsx',
  '.es6',
  '.es',
  '.mjs',
]);

addHook(
  (code, filename) => {
    if (/[/\\]node_modules[/\\]/.test(filename)) {
      return code;
    }
    return transformCode(code);
  },
  {
    exts: DEFAULT_EXTENSIONS,
  }
);
