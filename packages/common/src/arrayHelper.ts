export function concatArrays<T>(arr: T[][]): T[] {
  const ret = [];
  for (let subArr of arr) {
    for (let item of subArr) {
      ret.push(item);
    }
  }
  return ret;
}

export function takeArrays<T>(arr: T[][], limit: number): T[] {
  const ret = [];
  let count = 0;
  for (let subArr of arr) {
    for (let item of subArr) {
      ret.push(item);
      if (++count >= limit) {
        return ret;
      }
    }
  }
  return ret;
}
