import * as fs from "fs";
import * as path from "path";
import { Kafka } from "kafkajs";
import ffmpeg from "fluent-ffmpeg";
import { downloadFromS3, uploadToS3 } from "./storage";
import { kafkaBrokers, outputFormats } from "./config";

/**
 * Encode into each format.
 */
function encodeVideo(inputPath: string, format: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const inputBasename = path.basename(inputPath, path.extname(inputPath));
    const outputDir = path.join(__dirname, "../temp_encoded");
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
    const outputPath = path.join(outputDir, `${inputBasename}_converted.${format}`); 
    ffmpeg(inputPath)
      .output(outputPath)
      .on("end", () => {
        console.log(`Finished encoding to ${format}: ${outputPath}`);
        resolve(outputPath);
      })
      .on("error", (err) => {
        console.error(`Error encoding to ${format}:`, err);
        reject(err);
      })
      .run();
  });
}


// ---------- Consumer Logic ----------

async function processVideo(s3Key: string): Promise<void> {
  const tempDir = path.join(__dirname, "../temp_encoded");
  if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });
  const localVideoPath = path.join(tempDir, path.basename(s3Key));
  await downloadFromS3(s3Key, localVideoPath);

  for (const format of outputFormats) {
    const encodedPath = await encodeVideo(localVideoPath, format);
    await uploadToS3(encodedPath, `encoded/${path.basename(encodedPath)}`);
  }

  fs.unlinkSync(localVideoPath);
}

/**
 * Implement the consumer logic
 */
async function runConsumer(): Promise<void> {
  const kafka = new Kafka({ brokers: kafkaBrokers });
  const consumer = kafka.consumer({ groupId: "video-processor" });

  await consumer.connect();
  await consumer.subscribe({ topic: "video-topic", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log(`Message received: ${message.value?.toString()}`);

      if (message.value) {
        const s3Key = message.value.toString();
        console.log(`Processing video: ${s3Key}`);
        await processVideo(s3Key);
      }
    },
  });
}

runConsumer().catch((err) => console.error("Fatal consumer error:", err));
