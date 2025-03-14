import * as path from "path";
import * as fs from "fs";

// This storage directory emulates S3. The interface remains the same.
export const storageDir: string = path.join(__dirname, "../storage");
// Ensure the storage directory exists.
if (!fs.existsSync(storageDir)) {
  fs.mkdirSync(storageDir, { recursive: true });
}

export const kafkaBrokers = ["localhost:9092"];
export const outputFormats: string[] = ["mp4", "avi", "webm", "mkv"];
