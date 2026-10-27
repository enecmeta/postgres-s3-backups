import { spawn } from "child_process";
import { createGzip } from "zlib";
import { S3Client, S3ClientConfig, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { filesize } from "filesize";

import { env } from "./env.js";

const createClient = () => {
  const clientOptions: S3ClientConfig = {
    region: env.AWS_S3_REGION,
    forcePathStyle: env.AWS_S3_FORCE_PATH_STYLE
  }

  if (env.AWS_S3_ENDPOINT) {
    console.log(`Using custom endpoint: ${env.AWS_S3_ENDPOINT}`);
    clientOptions.endpoint = env.AWS_S3_ENDPOINT;
  }

  return new S3Client(clientOptions);
}

export const backup = async () => {
  console.log("Initiating DB backup...");

  const timestamp = new Date().toISOString().replace(/[:.]+/g, '-');
  let key = `${env.BACKUP_FILE_PREFIX}-${timestamp}.tar.gz`;
  if (env.BUCKET_SUBFOLDER) {
    key = env.BUCKET_SUBFOLDER + "/" + key;
  }

  const client = createClient();

  const dump = spawn("pg_dump", [
    `--dbname=${env.BACKUP_DATABASE_URL}`,
    "--format=tar",
    ...env.BACKUP_OPTIONS.split(" ").filter(Boolean),
  ]);

  let stderr = "";
  dump.stderr.on("data", (chunk) => { stderr += chunk; });
  const exited = new Promise<number | null>((resolve) => dump.on("close", resolve));

  const upload = new Upload({
    client,
    params: { Bucket: env.AWS_S3_BUCKET, Key: key, Body: dump.stdout.pipe(createGzip()) },
    partSize: 32 * 1024 * 1024,
    queueSize: 4,
  });

  let uploaded = 0;
  upload.on("httpUploadProgress", (p) => { uploaded = p.loaded ?? uploaded; });

  console.log("Streaming DB dump to S3...");

  let code: number | null;
  try {
    [code] = await Promise.all([exited, upload.done()]);
  } catch (error) {
    dump.kill();
    throw error;
  }

  if (stderr) {
    console.log({ stderr: stderr.trimEnd() });
  }

  if (code !== 0) {
    await client.send(new DeleteObjectCommand({ Bucket: env.AWS_S3_BUCKET, Key: key }));
    throw new Error(`pg_dump exited with code ${code}; partial upload deleted`);
  }

  console.log("Backup uploaded:", key, filesize(uploaded));
  console.log("DB backup complete...");
}
