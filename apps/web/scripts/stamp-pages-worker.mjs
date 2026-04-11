import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const distDir = resolve(scriptDir, '..', 'dist');
const workerPath = resolve(distDir, '_worker.js');
const indexPath = resolve(distDir, 'index.html');

const indexHtml = await readFile(indexPath, 'utf8');
const workerSource = await readFile(workerPath, 'utf8');

const scriptMatch = indexHtml.match(/<script[^>]+type="module"[^>]+src="([^"]+)"/i);
if (!scriptMatch?.[1]) {
  throw new Error('Failed to locate the dist module script for Pages worker stamping');
}

const stylesheetMatch = indexHtml.match(/<link[^>]+rel="stylesheet"[^>]+href="([^"]+)"/i);
const deploySeed = [scriptMatch[1], stylesheetMatch?.[1] ?? ''].join('|');
const deployId = createHash('sha256').update(deploySeed).digest('hex').slice(0, 16);

const stampedSource = workerSource.replace(
  "const FALLBACK_DEPLOY_ID = '__UPTIMER_DEPLOY_ID__';",
  `const FALLBACK_DEPLOY_ID = '${deployId}';`,
);

if (stampedSource === workerSource) {
  throw new Error('Failed to stamp dist/_worker.js with the current deploy id');
}

await writeFile(workerPath, stampedSource, 'utf8');
console.log(`Stamped dist/_worker.js with deploy id ${deployId}`);
