import * as fs from 'fs';

function walkSync(dirPath: string, filelist: string[], recursive = false) {
  if (!dirPath.endsWith('/')) {
    dirPath = dirPath + '/';
  }

  const files = fs.readdirSync(dirPath);
  for (const file of files) {
    if (recursive) {
      if (fs.statSync(dirPath + file).isDirectory()) {
        walkSync(dirPath + file + '/', filelist, recursive);
      } else {
        filelist.push(dirPath + file);
      }
    } else {
      if (!fs.statSync(dirPath + file).isDirectory()) {
        filelist.push(dirPath + file);
      }
    }
  }
}

export function listFiles(path: string, recursive: boolean) {
  const fileList: string[] = [];
  walkSync(path, fileList, recursive);
  return fileList;
}
