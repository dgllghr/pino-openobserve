/*
 * Copyright 2024 Zinc Labs Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Writable } from "stream";
import * as url from "url";

interface AuthOptions {
  username: string;
  password: string;
}

interface TransportOptions {
  url: string;
  organization: string;
  streamName: string;
  auth: AuthOptions;
  batchSize?: number;
  timeThreshold?: number;
  silentSuccess?: boolean;
  silentError?: boolean;
}

function createApiUrl(
  baseUrl: string,
  organization: string,
  streamName: string,
): string {
  const parsedUrl = url.parse(baseUrl);
  const path = parsedUrl.pathname?.replace(/\/$/, "") ?? "";
  return `${parsedUrl.protocol}//${parsedUrl.host}${path}/api/${organization}/${streamName}/_multi`;
}

export default async function (opts: TransportOptions) {
  const {
    url: baseUrl,
    organization,
    streamName,
    auth,
    batchSize = 100,
    timeThreshold = 5 * 60 * 1000,
    silentSuccess = false,
    silentError = false,
  } = opts;

  const apiUrl = createApiUrl(baseUrl, organization, streamName);

  let logs: string[] = [];
  let timer: NodeJS.Timeout | null = null;
  let apiCallInProgress = false;

  async function sendLogs() {
    if (logs.length === 0 || apiCallInProgress) return;

    apiCallInProgress = true;
    const payload = logs.splice(0, batchSize).join("");

    try {
      const response = await fetch(apiUrl, {
        method: "POST",
        headers: {
          Authorization: `Basic ${Buffer.from(`${auth.username}:${auth.password}`).toString("base64")}`,
          "Content-Type": "application/json",
        },
        body: payload,
      });

      if (response.ok) {
        if (!silentSuccess) {
          console.log("[pino-openobserve] Logs sent successfully");
        }
      } else {
        if (!silentError)
          console.error(
            "[pino-openobserve] Failed:",
            response.status,
            response.statusText,
          );
      }
    } catch (err) {
      if (!silentError) console.error("[pino-openobserve] Error:", err);
    } finally {
      apiCallInProgress = false;
      schedule();
    }
  }

  function schedule() {
    if (timer) clearTimeout(timer);
    if (logs.length >= batchSize) {
      sendLogs();
    } else {
      timer = setTimeout(sendLogs, timeThreshold);
    }
  }

  process.on("beforeExit", () => {
    if (logs.length > 0 && !apiCallInProgress) sendLogs();
  });

  const writable = new Writable({
    objectMode: true,
    write(chunk, _, cb) {
      logs.push(chunk);
      schedule();
      cb();
    },
  });

  return writable;
}
