import * as fs from 'fs';
import * as path from 'path';
import { URL } from 'url';

function walkSync(dirPath: URL, filelist: string[], recursive = false) {
  const files = fs.readdirSync(dirPath);
  for (const file of files) {
    const fileUrl = new URL(file, dirPath);

    if (recursive) {
      if (fs.statSync(fileUrl).isDirectory()) {
        fileUrl.href += '/';
        walkSync(fileUrl, filelist, recursive);
      } else {
        filelist.push(fileUrl.toString());
      }
    } else {
      if (!fs.statSync(fileUrl).isDirectory()) {
        filelist.push(fileUrl.toString());
      }
    }
  }
}

export function listFiles(dirOrFile: string | URL, recursive: boolean) {
  const fileList: string[] = [];
  dirOrFile = toUrl(dirOrFile);

  if (fs.statSync(dirOrFile).isDirectory()) {
    if (!dirOrFile.href.endsWith('/')) {
      dirOrFile.href += '/';
    }
    walkSync(toUrl(dirOrFile.href), fileList, recursive);
  } else {
    return [dirOrFile.toString()];
  }
  return fileList;
}

export function toUrl(pathOrUri: string | URL): URL {
  if (pathOrUri instanceof URL) {
    return pathOrUri;
  }
  const base = new URL(path.join(path.resolve(), './'), 'file:///');
  return new URL(pathOrUri, base);
}
