import typescript from 'rollup-plugin-typescript2';
import clear from 'rollup-plugin-clear';
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
  ],
  plugins: [
    banner('@noCaptureEnv'),
    typescript(),
    clear({
      targets: ['dist'],
    }),
  ],
};
