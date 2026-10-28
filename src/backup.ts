import { spawn } from "child_process";
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

const dropPgCache = async () => {
  console.log("Dropping Postgres page cache...");

  const sql = `COPY (SELECT 1) TO PROGRAM 'find "$PGDATA" -type f -exec dd if={} iflag=nocache count=0 status=none \\;'`;
  const psql = spawn("psql", [env.BACKUP_DATABASE_URL, "-v", "ON_ERROR_STOP=1", "-c", sql]);

  let stderr = "";
  psql.stderr.on("data", (chunk) => { stderr += chunk; });
  const code = await new Promise<number | null>((resolve) => psql.on("close", resolve));

  if (code !== 0) {
    console.log("Could not drop page cache:", stderr.trimEnd());
    return;
  }

  console.log("Page cache dropped");
}

export const backup = async () => {
  console.log("Initiating DB backup...");

  const timestamp = new Date().toISOString().replace(/[:.]+/g, '-');
  let key = `${env.BACKUP_FILE_PREFIX}-${timestamp}.dump`;
  if (env.BUCKET_SUBFOLDER) {
    key = env.BUCKET_SUBFOLDER + "/" + key;
  }

  const client = createClient();

  const dump = spawn("pg_dump", [
    `--dbname=${env.BACKUP_DATABASE_URL}`,
    "--format=custom",
    ...env.BACKUP_OPTIONS.split(" ").filter(Boolean),
  ]);

  let stderr = "";
  dump.stderr.on("data", (chunk) => { stderr += chunk; });
  const exited = new Promise<number | null>((resolve) => dump.on("close", resolve));

  const upload = new Upload({
    client,
    params: { Bucket: env.AWS_S3_BUCKET, Key: key, Body: dump.stdout },
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

  await dropPgCache();

  console.log("DB backup complete...");
}
