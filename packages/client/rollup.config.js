import typescript from 'rollup-plugin-typescript2';
import banner from 'rollup-plugin-banner';
import pkg from './package.json';

export default {
  input: './src/index.ts',
  output: [
    {
      file: pkg.main,
      format: 'cjs',
    },
    {
      file: pkg.module,
      format: 'es',
    },
  ],
  external: [
    ...Object.keys(pkg.dependencies || {}),
    ...Object.keys(pkg.peerDependencies || {}),
    'v8',
    'fs',
    'path',
  ],
  plugins: [banner('@noCaptureEnv'), typescript()],
};
