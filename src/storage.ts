import * as fs from "fs";
import * as path from "path";
import { storageDir } from "./config";

/**
 * "Uploads" a local file to our storage folder.
 * The s3Key is treated as the relative path inside the storageDir.
 */
export function uploadToS3(localFilePath: string, s3Key: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const destPath = path.join(storageDir, s3Key);
    const destDir = path.dirname(destPath);
    if (!fs.existsSync(destDir)) {
      fs.mkdirSync(destDir, { recursive: true });
    }
    fs.copyFile(localFilePath, destPath, (err) => {
      if (err) {
        reject(err);
      } else {
        console.log(`Copied ${localFilePath} to storage at ${destPath}`);
        resolve();
      }
    });
  });
}

/**
 * "Downloads" an object from our storage folder to a local path.
 */
export function downloadFromS3(s3Key: string, localPath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const srcPath = path.join(storageDir, s3Key);
    fs.copyFile(srcPath, localPath, (err) => {
      if (err) {
        reject(err);
      } else {
        console.log(`Copied ${srcPath} to ${localPath}`);
        resolve(localPath);
      }
    });
  });
}
