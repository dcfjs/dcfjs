import typescript from 'rollup-plugin-typescript2';
import banner from 'rollup-plugin-banner';
import bin from 'rollup-plugin-bin';
import pkg from './package.json';

export default [
  {
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
      'fs',
      'path',
      'stream',
      'v8',
    ],
    plugins: [banner('@noCaptureEnv'), typescript()],
  },
  {
    input: './src/cli.ts',
    output: {
      file: './dist/cli.js',
      format: 'cjs',
    },
    external: [
      ...Object.keys(pkg.dependencies || {}),
      ...Object.keys(pkg.peerDependencies || {}),
      'fs',
      'path',
      'stream',
      'v8',
    ],
    plugins: [banner('@noCaptureEnv'), typescript(), bin()],
  },
];
