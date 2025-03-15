import { exec } from "child_process";
import * as fs from "fs";
import * as path from "path";
import { Kafka } from "kafkajs";
import { uploadToS3 } from "./storage";
import { kafkaBrokers } from "./config";

/**
 * Downloads a YouTube video using yt-dlp.
 */
function downloadYouTubeVideo(videoUrl: string, outputPath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const command = `yt-dlp -f 'bv*[height<=720][ext=mp4]+ba[ext=m4a]/b[height<=720]/b' -o "${outputPath}.mp4" ${videoUrl}`;
    console.log(`Executing: ${command}`);

    exec(command, (error, stdout) => {
      if (error) {
        console.error(`yt-dlp error: ${error.message}`);
        return reject(error);
      }
      console.log(`yt-dlp output: ${stdout}`);
      resolve(outputPath);
    });
  });
}

/**
 * Produces a Kafka event with the given S3 key.
 */
async function produceEvent(s3Key: string): Promise<void> {
  const kafka = new Kafka({ brokers: kafkaBrokers });
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: "video-topic",
    messages: [{ value: s3Key }],
  });
  await producer.disconnect();
  console.log(`Produced event for video: ${s3Key}`);
}

// ---------- Producer Logic ----------

async function runProducer(): Promise<void> {
  const tempDir = path.join(__dirname, "../temp");
  if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });

  const videoIds = ["-RXA143mC_0", "VIPH9lY7nUw", "fai_cwpo4dA"];
  for (let id of videoIds) {
    try {
      //const outputFilename = `downloaded_video_${id}.mp4`;
      const outputFilename = `downloaded_video_${id}`;
      let localOutputPath = path.join(tempDir, outputFilename);
      
      const videoUrl = `https://www.youtube.com/watch?v=${id}`;

      console.log(`Downloading video: ${videoUrl}`);
      localOutputPath = await downloadYouTubeVideo(videoUrl, localOutputPath);
      console.log(`Downloaded to: ${localOutputPath}`);

      const s3Key = `videos/${outputFilename}.mp4`;
      await uploadToS3(localOutputPath + ".mp4", s3Key);
      await produceEvent(s3Key);

      //fs.unlinkSync(localOutputPath);
      fs.unlinkSync(localOutputPath + ".mp4");
    } catch (err) {
      console.error("Producer error:", err);
    }
  }
}

runProducer().catch((err) => console.error("Fatal producer error:", err));
