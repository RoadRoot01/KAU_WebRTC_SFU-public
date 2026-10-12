import { createHash } from 'node:crypto';
import { readFile, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(scriptDirectory, '..');
const canonicalPath = path.join(
  projectRoot,
  'docs',
  'custom_code',
  'webCodecsPipeline.js'
);
const runtimePath = path.resolve(
  projectRoot,
  '..',
  'mediasoup',
  'node',
  'lib',
  'webCodecsPipeline.js'
);
const runtimeDirectory = path.dirname(runtimePath);

const workspaceHint = [
  'Expected workspace layout:',
  '  workspace/',
  '    mediasoup/',
  '    KAU_WebRTC_SFU-public/'
].join('\n');

function sha256(contents) {
  return createHash('sha256').update(contents).digest('hex');
}

async function readRequired(filePath, label) {
  try {
    return await readFile(filePath);
  } catch (error) {
    if (error?.code === 'ENOENT') {
      throw new Error(`${label} was not found:\n${filePath}\n\n${workspaceHint}`);
    }
    throw error;
  }
}

async function ensureRuntimeDirectory() {
  try {
    const directoryInfo = await stat(runtimeDirectory);
    if (!directoryInfo.isDirectory()) {
      throw new Error(`Runtime path is not a directory:\n${runtimeDirectory}`);
    }
  } catch (error) {
    if (error?.code === 'ENOENT') {
      throw new Error(
        `Built sibling mediasoup directory was not found:\n${runtimeDirectory}\n\n${workspaceHint}`
      );
    }
    throw error;
  }
}

async function verify() {
  const [canonical, runtime] = await Promise.all([
    readRequired(canonicalPath, 'Canonical pipeline'),
    readRequired(runtimePath, 'Runtime pipeline')
  ]);
  const canonicalHash = sha256(canonical);
  const runtimeHash = sha256(runtime);

  if (!canonical.equals(runtime)) {
    console.error([
      'FAIL: mediasoup pipeline files differ.',
      `canonical: ${canonicalPath}`,
      `canonical SHA-256: ${canonicalHash}`,
      `runtime: ${runtimePath}`,
      `runtime SHA-256: ${runtimeHash}`,
      '',
      'Run: npm run sync-mediasoup-pipeline'
    ].join('\n'));
    process.exitCode = 1;
    return;
  }

  console.log([
    'PASS: mediasoup pipeline files are an exact byte match.',
    `canonical: ${canonicalPath}`,
    `runtime: ${runtimePath}`,
    `SHA-256: ${canonicalHash}`,
    'exact byte match: PASS'
  ].join('\n'));
}

async function sync() {
  const canonical = await readRequired(canonicalPath, 'Canonical pipeline');
  await ensureRuntimeDirectory();
  await writeFile(runtimePath, canonical);

  const runtime = await readRequired(runtimePath, 'Runtime pipeline');
  if (!canonical.equals(runtime)) {
    throw new Error('Pipeline sync completed without an exact byte match.');
  }

  console.log([
    'PASS: mediasoup pipeline synchronized.',
    `canonical: ${canonicalPath}`,
    `runtime: ${runtimePath}`,
    `SHA-256: ${sha256(canonical)}`,
    'exact byte match: PASS'
  ].join('\n'));
}

const mode = process.argv[2];

try {
  if (mode === 'sync') {
    await sync();
  } else if (mode === 'verify') {
    await verify();
  } else {
    throw new Error(
      'Usage: node scripts/mediasoup-pipeline.mjs <sync|verify>'
    );
  }
} catch (error) {
  console.error(`ERROR: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}
