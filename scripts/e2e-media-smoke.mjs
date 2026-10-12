#!/usr/bin/env node

import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { constants as fsConstants } from 'node:fs';
import {
  access,
  appendFile,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rm,
  writeFile
} from 'node:fs/promises';
import { createServer } from 'node:http';
import { createConnection } from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import puppeteer from 'puppeteer';
import WebSocket from 'ws';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const ARTIFACT_DIR = path.join(ROOT, 'artifacts');
const REPORT_SUFFIX = process.env.SFU_E2E_REPORT_SUFFIX
  ? '-' + process.env.SFU_E2E_REPORT_SUFFIX.replaceAll(/[^a-z0-9_-]/gi, '-')
  : '';
const STOP_AFTER_PLI = process.env.SFU_E2E_STOP_AFTER_PLI === '1';
const TARGETED_SKIPPED_CHECKS = [
  'production baseline Consumer 1/2, multi-consumer and independence checks',
  'same-Consumer sequential layer-selection diagnostic',
  'production diagnostics and source visibility checks',
  'captured-source cleanup lifecycle',
  'producer page-close and reload cleanup cycles',
  'debug-mode E2E diagnostics'
];
const DEFAULT_INITIAL_AVAILABLE_OUTGOING_BITRATE_BPS = 1_000_000;
const JSON_REPORT_PATH = path.join(ARTIFACT_DIR, 'e2e-media-smoke' + REPORT_SUFFIX + '.json');
const TEXT_REPORT_PATH = path.join(ARTIFACT_DIR, 'e2e-media-smoke' + REPORT_SUFFIX + '.txt');
const PATTERN_TITLE = 'WEBRTC E2E TEST';
const SIGNALING_HOST = '127.0.0.1';
const SIGNALING_PORT = Number(process.env.PORT ?? 3000);
if (!Number.isInteger(SIGNALING_PORT) || SIGNALING_PORT < 1 || SIGNALING_PORT > 65535) {
  throw new Error('PORT must be an integer from 1 through 65535 for the media E2E.');
}
const SIGNALING_URL = 'http://' + SIGNALING_HOST + ':' + SIGNALING_PORT;
const HEALTH_URL = SIGNALING_URL + '/health';
const DEV_HOST = '127.0.0.1';
const DEV_PORT = 5173;
const BROWSER_APP_URL = 'https://' + DEV_HOST + ':' + DEV_PORT;
const PRODUCTION_APP_URL = BROWSER_APP_URL;
const DEV_APP_URL = BROWSER_APP_URL;
const PAGE_TIMEOUT_MS = 45_000;
const SERVER_TIMEOUT_MS = 60_000;
const MEDIA_SAMPLE_MS = 12_000;
const DEBUG_STATS_TIMEOUT_MS = 112_000;
const LOG_TAIL_CHARS = 18_000;
const RUNTIME_POLL_INTERVAL_MS = 150;
const PRODUCER_CHECKPOINT_MAX_MS = 10_000;
const DIAGNOSTIC_EVENTS = new Set([
  'serverStats',
  'pipelineStats',
  'transportState'
]);
const PRODUCER_CYCLE_CHECKPOINTS = [
  'cycleStarted',
  'patternPageCreated',
  'patternNavigationCompleted',
  'patternTitleMatched',
  'patternFramesAdvancing',
  'appPageCreated',
  'appNavigationCompleted',
  'startButtonFound',
  'captureSourceInventoryChecked',
  'startClicked',
  'getDisplayMediaResolved',
  'roleProducerAssigned',
  'captureTrackLive',
  'localVideoPlaying',
  'encodedChunkStarted',
  'healthProducerRegistered',
  'injectedChunkStarted',
  'injectedPacketStarted',
  'inputFpsStarted',
  'producerProgressConfirmed',
  'h264CodecEvidenceObserved',
  'cycleReady'
];
const PRODUCER_TIMING_CHECKPOINTS = [
  'startClicked',
  'getDisplayMediaResolved',
  'roleProducerAssigned',
  'captureTrackLive',
  'localVideoPlaying',
  'encodedChunkStarted',
  'healthProducerRegistered',
  'injectedChunkStarted',
  'injectedPacketStarted',
  'inputFpsStarted'
];

function createProducerCycleTrace() {
  return {
    status: 'not-run',
    startedAt: null,
    finishedAt: null,
    durationMs: null,
    producerStartDeadline: null,
    error: null,
    falseNegativeSuspected: false,
    classification: null,
    runtimeWaitDiagnostics: [],
    timingSanityWarnings: [],
    encoderOutputFrames: null,
    checkpoints: Object.fromEntries(PRODUCER_CYCLE_CHECKPOINTS.map(name => [
      name,
      {
        status: 'not-run',
        startedAt: null,
        finishedAt: null,
        durationMs: null,
        error: null
      }
    ])),
    sourceInventory: null,
    getDisplayMediaAudit: null,
    lastUiSnapshot: null,
    lastHealthSnapshot: null,
    progressSnapshots: null,
    pageAudits: []
  };
}

const report = {
  passed: false,
  execution: {
    mode: STOP_AFTER_PLI ? 'targeted-fresh-consumer-pli' : 'full-e2e',
    scope: STOP_AFTER_PLI
      ? 'production Producer and independent fresh-Consumer low/mid/high PLI correctness only'
      : 'full production and debug E2E',
    bweControl: {
      performed: false,
      baselineInitialAvailableOutgoingBitrateBps:
        DEFAULT_INITIAL_AVAILABLE_OUTGOING_BITRATE_BPS,
      effectiveInitialAvailableOutgoingBitrateBps:
        DEFAULT_INITIAL_AVAILABLE_OUTGOING_BITRATE_BPS,
      overrideScope: 'disabled; fixed at 1 Mbps'
    },
    fullE2ECompleted: false,
    completedScope: null,
    skippedChecks: STOP_AFTER_PLI ? [...TARGETED_SKIPPED_CHECKS] : []
  },
  timestamp: new Date().toISOString(),
  finishedAt: null,
  automation: {
    puppeteer: 'existing devDependency',
    chromeExecutable: null,
    chromeDetection: null,
    temporaryProfile: null,
    localCertificateAccepted: false,
    initialPageCount: null,
    browserConnectedAfterLaunch: null,
    browserDisconnected: false,
    browserDisconnectedAt: null,
    browserDisconnectedUnexpectedly: false,
    captureAutoSelectSwitch:
      '--auto-select-tab-capture-source-by-title=' + PATTERN_TITLE,
    sourceTitleRestriction: PATTERN_TITLE,
    desktopCaptureSwitchUsed: false,
    userInteractionRequired: false,
    headless: false
  },
  staticValidation: {
    pipeline: { status: 'not-run' },
    typecheck: { status: 'not-run' },
    productionBuild: { status: 'not-run' },
    productionBundle: { status: 'not-run' },
    sourceGuards: { status: 'not-run' }
  },
  production: {
    status: 'not-run',
    endpoint: {
      status: 'not-run',
      url: PRODUCTION_APP_URL,
      documentStatus: null,
      javascriptAssets: [],
      stylesheetAssets: [],
      proxiedHealth: null,
      websocketProxy: 'not-run',
      websocketUrls: []
    },
    producer: { status: 'not-run' },
    statsVisualizer: { status: 'not-run' },
    producerStatsDump: { status: 'not-run' },
    consumer1: { status: 'not-run' },
    consumerStatsDump: { status: 'not-run' },
    consumer2: { status: 'not-run' },
    consumerStatsFailureRecovery: { status: 'not-run' },
    consumerInboundRtpAudit: {
      status: 'not-run',
      simulcastOnNoLayerChange: { status: 'not-run' },
      simulcastOnLayerSequence: { status: 'not-run' },
      simulcastOff: { status: 'not-run' },
      multiConsumerIsolation: { status: 'not-run' }
    },
    multiConsumer: { status: 'not-run' },
    consumerIndependence: { status: 'not-run' },
    manualConsumerPreference: { status: 'not-run' },
    offMode: {
      status: 'not-run',
      producer: { status: 'not-run' },
      statsDump: { status: 'not-run' },
      consumer: { status: 'not-run' },
      layerControlRejection: { status: 'not-run' },
      cleanup: { status: 'not-run' }
    },
    e2eControlGuard: { status: 'not-run' },
    pliFir: {
      status: 'not-run',
      trigger: 'Consumer.requestKeyFrame via E2E-gated signaling action',
      coalescing: {
        status: 'not-run',
        verification: 'static source assertions',
        maximumAuditRequests: 16,
        onePendingObligationPerLayer: null,
        consumesAfterEncodeSuccess: null,
        retriesNextFrameOnFirstEncodeFailure: null,
        clearsAuditMapOnStop: null
      },
      byLayer: {
        low: {
          status: 'not-run', targetLayer: 'low', expectedSsrc: 12345678,
          consumerSession: null, initialLayerState: null, targetReachedAt: null,
          layerSelectionMs: null, layerSelection: null,
          requestKeyFrameReached: null, pliStatus: 'not-run', pliEvidence: null,
          latency: null, consumerMedia: null, cleanup: null
        },
        mid: {
          status: 'not-run', targetLayer: 'mid', expectedSsrc: 12345679,
          consumerSession: null, initialLayerState: null, targetReachedAt: null,
          layerSelectionMs: null, layerSelection: null,
          requestKeyFrameReached: null, pliStatus: 'not-run', pliEvidence: null,
          latency: null, consumerMedia: null, cleanup: null
        },
        high: {
          status: 'not-run', targetLayer: 'high', expectedSsrc: 12345680,
          consumerSession: null, initialLayerState: null, targetReachedAt: null,
          layerSelectionMs: null, layerSelection: null,
          requestKeyFrameReached: null, pliStatus: 'not-run', pliEvidence: null,
          latency: null, consumerMedia: null, cleanup: null
        }
      },
      failureClassification: null,
      serverAudit: null,
      fir: {
        staticSupport: true,
        runtimeObserved: null,
        runtimeStatus: 'not-run'
      },
      latency: null,
      productionOverhead: null,
      consumerPlaybackContinuous: null
    },
    layerSelectionDiagnostic: {
      status: 'not-run',
      mode: STOP_AFTER_PLI ? 'skipped-in-targeted-pli-mode' : 'same-consumer-low-mid-high',
      consumerSession: null,
      byLayer: {
        low: { status: 'not-run', targetReachedAt: null, layerSelectionMs: null },
        mid: { status: 'not-run', targetReachedAt: null, layerSelectionMs: null },
        high: { status: 'not-run', targetReachedAt: null, layerSelectionMs: null }
      },
      currentLayersSequence: [],
      availableOutgoingBitrateSamples: [],
      layerschangeSamples: [],
      mediaContinuity: null,
      classification: null
    },
    diagnostics: { status: 'not-run' },
    capturedSourceCleanup: { status: 'not-run' },
    producerPageCloseCleanup: {
      status: 'not-run',
      phase: 'not-run',
      closeActionExecuted: false,
      preCloseSnapshot: null,
      cleanupSamples: null
    },
    reloadCleanup: {
      status: 'not-run',
      phase: 'not-run',
      reloadActionExecuted: false,
      preReloadSnapshot: null,
      cleanupSamples: null
    },
    producerCycles: {
      initial: createProducerCycleTrace(),
      pageClose: createProducerCycleTrace(),
      reload: createProducerCycleTrace()
    }
  },
  debug: {
    status: 'not-run',
    endpoint: { status: 'not-run' },
    producer: { status: 'not-run' },
    consumer: { status: 'not-run' },
    diagnostics: { status: 'not-run' },
    producerCycle: createProducerCycleTrace()
  },
  cleanup: {
    browserClosed: false,
    childProcessesStopped: false,
    patternServerStopped: false,
    tempDirectoryRemoved: false
  },
  runtimeWaitDiagnostics: [],
  runtimeLogs: [],
  resultFiles: {
    json: path.relative(ROOT, JSON_REPORT_PATH),
    text: path.relative(ROOT, TEXT_REPORT_PATH)
  },
  errors: []
};

const runtime = {
  tempDir: null,
  patternServer: null,
  patternUrl: null,
  browser: null,
  browserProcess: null,
  pages: new Set(),
  pageAudits: [],
  pageAuditPages: new Map(),
  processes: [],
  cleanupStarted: false,
  cleanupPromise: null
};

const abortController = new AbortController();
let interruptedBy = null;

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function beginCycleCheckpoint(trace, name) {
  const checkpoint = trace?.checkpoints?.[name];
  if (!checkpoint || checkpoint.status !== 'not-run') {
    return checkpoint ?? null;
  }
  checkpoint.status = 'running';
  checkpoint.startedAt = new Date().toISOString();
  checkpoint.startedAtMs = Date.now();
  return checkpoint;
}

function finishCycleCheckpoint(trace, name, status, error = null, details = null) {
  const checkpoint = beginCycleCheckpoint(trace, name);
  if (!checkpoint) {
    return;
  }
  if (checkpoint.status === 'passed' && status !== 'passed') {
    return;
  }
  const finishedAtMs = Date.now();
  checkpoint.status = status;
  checkpoint.finishedAt = new Date(finishedAtMs).toISOString();
  checkpoint.durationMs = finishedAtMs - checkpoint.startedAtMs;
  delete checkpoint.startedAtMs;
  checkpoint.error = status === 'failed' ? errorMessage(error) : null;
  if (details) {
    Object.assign(checkpoint, details);
  }
}

async function withCycleCheckpoint(trace, name, action) {
  if (!trace) {
    return action();
  }
  beginCycleCheckpoint(trace, name);
  try {
    const result = await action();
    finishCycleCheckpoint(trace, name, 'passed');
    return result;
  } catch (error) {
    finishCycleCheckpoint(trace, name, 'failed', error);
    trace.status = 'failed';
    trace.error ??= errorMessage(error);
    throw error;
  }
}

function recordCycleCheckpoint(trace, name, condition, details = null) {
  if (!trace || !condition) {
    return;
  }
  const checkpoint = trace.checkpoints?.[name];
  if (!checkpoint) {
    return;
  }
  if (checkpoint.status === 'passed') {
    if (details) {
      Object.assign(checkpoint, details);
    }
    return;
  }
  beginCycleCheckpoint(trace, name);
  finishCycleCheckpoint(trace, name, 'passed', null, details);
}

function startProducerCycleTrace(trace, label) {
  if (!trace) {
    return;
  }
  const now = Date.now();
  trace.label = label;
  trace.status = 'running';
  trace.startedAt = new Date(now).toISOString();
  trace.finishedAt = null;
  trace.durationMs = null;
  trace.error = null;
  recordCycleCheckpoint(trace, 'cycleStarted', true);
}

function finishProducerCycleTrace(trace, error = null) {
  if (!trace) {
    return;
  }
  const now = Date.now();
  trace.status = error ? 'failed' : 'passed';
  trace.error = error ? errorMessage(error) : null;
  trace.finishedAt = new Date(now).toISOString();
  trace.durationMs = trace.startedAt ? now - Date.parse(trace.startedAt) : null;
  if (trace.producerStartDeadline) {
    trace.producerStartDeadline.finishedAt = trace.finishedAt;
    trace.producerStartDeadline.elapsedMs =
      now - Date.parse(trace.producerStartDeadline.startedAt);
    trace.producerStartDeadline.completedWithinDeadline =
      now <= Date.parse(trace.producerStartDeadline.deadlineAt);
  }
  trace.timingSanityWarnings = PRODUCER_TIMING_CHECKPOINTS
    .filter(name => Number(trace.checkpoints[name]?.durationMs) > PRODUCER_CHECKPOINT_MAX_MS)
    .map(name => ({
      checkpoint: name,
      status: trace.checkpoints[name].status,
      durationMs: trace.checkpoints[name].durationMs,
      thresholdMs: PRODUCER_CHECKPOINT_MAX_MS
    }));
}

function ensure(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function checkInterrupted() {
  if (interruptedBy) {
    throw new Error('Interrupted by ' + interruptedBy);
  }
}

function wait(ms) {
  checkInterrupted();
  return new Promise((resolve, reject) => {
    const signal = abortController.signal;
    const finish = () => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    const onAbort = () => {
      clearTimeout(timer);
      signal.removeEventListener('abort', onAbort);
      reject(new Error('Interrupted by ' + interruptedBy));
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

function rawDelay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function withTimeout(promise, timeoutMs, description) {
  let timer;
  try {
    return await Promise.race([
      Promise.resolve(promise),
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(description + ' timed out after ' + timeoutMs + 'ms')),
          timeoutMs
        );
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function withStatus(target, operation) {
  target.status = 'running';
  target.startedAt = new Date().toISOString();
  const started = Date.now();
  try {
    const result = await operation();
    if (target.status === 'running') {
      target.status = 'passed';
    }
    target.durationMs = Date.now() - started;
    target.finishedAt = new Date().toISOString();
    return result;
  } catch (error) {
    target.status = 'failed';
    target.error = errorMessage(error);
    target.durationMs = Date.now() - started;
    target.finishedAt = new Date().toISOString();
    throw error;
  }
}

function npmInvocation(args) {
  const npmExecPath = process.env.npm_execpath;
  if (npmExecPath && path.isAbsolute(npmExecPath)) {
    return {
      command: process.execPath,
      args: [npmExecPath, ...args],
      shell: false
    };
  }

  return {
    command: process.platform === 'win32' ? 'npm.cmd' : 'npm',
    args,
    shell: process.platform === 'win32'
  };
}

function appendProcessOutput(record, streamName, chunk) {
  const text = chunk.toString('utf8');
  record.tail = (record.tail + text).slice(-LOG_TAIL_CHARS);
  record.writeChain = record.writeChain
    .then(() => appendFile(record.logPath, '[' + streamName + '] ' + text))
    .catch(error => {
      record.logWriteError = errorMessage(error);
    });
}

function startProcess(label, command, args, options = {}) {
  checkInterrupted();
  const logPath = path.join(runtime.tempDir, 'logs', label.replaceAll(/[^a-z0-9_-]/gi, '-') + '.log');
  const env = { ...process.env, ...(options.env ?? {}) };
  const child = spawn(command, args, {
    cwd: ROOT,
    env,
    windowsHide: true,
    detached: process.platform !== 'win32',
    shell: options.shell ?? false,
    stdio: ['ignore', 'pipe', 'pipe']
  });

  const record = {
    label,
    command,
    args,
    logPath,
    child,
    exit: null,
    spawnError: null,
    tail: '',
    writeChain: Promise.resolve(),
    logWriteError: null,
    stopRequested: false,
    stoppedByHarness: false,
    donePromise: null
  };

  record.donePromise = new Promise(resolve => {
    let settled = false;
    child.once('error', error => {
      record.spawnError = error;
      if (!settled) {
        settled = true;
        resolve({ error });
      }
    });
    child.once('exit', (code, signal) => {
      record.exit = { code, signal };
      if (!settled) {
        settled = true;
        resolve({ code, signal });
      }
    });
  });

  child.stdout?.on('data', chunk => appendProcessOutput(record, 'stdout', chunk));
  child.stderr?.on('data', chunk => appendProcessOutput(record, 'stderr', chunk));
  runtime.processes.push(record);
  return record;
}

function startNpmScript(label, scriptName, args = [], env = {}) {
  const npmArgs = ['run', scriptName];
  if (args.length > 0) {
    npmArgs.push('--', ...args);
  }
  const invocation = npmInvocation(npmArgs);
  return startProcess(label, invocation.command, invocation.args, {
    shell: invocation.shell,
    env
  });
}

async function runToCompletion(record, timeoutMs = SERVER_TIMEOUT_MS) {
  let timer;
  const timeout = new Promise(resolve => {
    timer = setTimeout(() => resolve({ timeout: true }), timeoutMs);
  });
  const result = await Promise.race([record.donePromise, timeout]);
  clearTimeout(timer);
  if (result.timeout) {
    throw new Error(record.label + ' timed out. Log tail:\n' + record.tail);
  }
  await record.writeChain;
  if (record.spawnError) {
    throw new Error(
      'Could not start ' + record.label + ': ' + errorMessage(record.spawnError)
    );
  }
  if (record.exit?.code !== 0) {
    throw new Error(
      record.label +
        ' exited with code ' +
        String(record.exit?.code) +
        '. Log tail:\n' +
        record.tail
    );
  }
}

async function waitForProcessExit(record, timeoutMs) {
  try {
    return await withTimeout(
      record.donePromise,
      timeoutMs,
      record.label + ' process exit'
    );
  } catch {
    return null;
  }
}

async function runNodeCommand(label, scriptPath, args = []) {
  const record = startProcess(label, process.execPath, [scriptPath, ...args]);
  await runToCompletion(record, SERVER_TIMEOUT_MS);
  return record;
}

async function assertPortFree(port) {
  const isFree = await new Promise(resolve => {
    const socket = createConnection({ host: SIGNALING_HOST, port });
    const timeout = setTimeout(() => {
      socket.destroy();
      resolve(false);
    }, 1500);
    timeout.unref?.();
    socket.once('connect', () => {
      clearTimeout(timeout);
      socket.destroy();
      resolve(false);
    });
    socket.once('error', error => {
      clearTimeout(timeout);
      resolve(error.code === 'ECONNREFUSED');
    });
  });
  ensure(
    isFree,
    'Port ' +
      port +
      ' is already occupied. Stop the existing local service before running this isolated E2E harness.'
  );
}

async function fetchJson(url, timeoutMs = 1500) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) {
      throw new Error(url + ' returned HTTP ' + response.status);
    }
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

async function waitForCondition(
  readValue,
  description,
  timeoutMs = PAGE_TIMEOUT_MS,
  intervalMs = 300,
  absoluteDeadline = null
) {
  const deadline = absoluteDeadline ?? Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    checkInterrupted();
    try {
      const value = await readValue();
      if (value && Date.now() <= deadline) {
        return value;
      }
    } catch (error) {
      lastError = error;
    }
    const remaining = deadline - Date.now();
    if (remaining > 0) {
      await wait(Math.min(intervalMs, remaining));
    }
  }
  const suffix = lastError ? ' Last error: ' + errorMessage(lastError) : '';
  throw new Error('Timed out waiting for ' + description + '.' + suffix);
}

async function waitForHealth(record, predicate, description, timeoutMs = SERVER_TIMEOUT_MS) {
  const startedAt = Date.now();
  let lastHealthSnapshot = null;
  try {
    return await waitForCondition(async () => {
      if (record?.spawnError) {
        throw new Error(record.label + ' failed to start: ' + errorMessage(record.spawnError));
      }
      if (record?.exit) {
        throw new Error(
          record.label +
            ' exited early with code ' +
            String(record.exit.code) +
            '. Log tail:\n' +
            record.tail
        );
      }
      const health = await fetchJson(HEALTH_URL).catch(() => null);
      lastHealthSnapshot = health ?? lastHealthSnapshot;
      return health && predicate(health) ? health : null;
    }, description, timeoutMs, RUNTIME_POLL_INTERVAL_MS);
  } catch (error) {
    const finalHealthSnapshot = await fetchJson(HEALTH_URL).catch(() => lastHealthSnapshot);
    const conditionSatisfied = Boolean(
      finalHealthSnapshot && predicate(finalHealthSnapshot)
    );
    const diagnostic = recordRuntimeWaitDiagnostic({
      description,
      startedAt,
      finalSnapshot: null,
      finalHealthSnapshot,
      conditionSatisfied,
      error
    });
    throw new Error(
      description + ' (classification=' + diagnostic.classification + ')',
      { cause: error }
    );
  }
}

function makePatternHtml() {
  return [
    '<!doctype html>',
    '<html lang="en"><head><meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width, initial-scale=1">',
    '<title>' + PATTERN_TITLE + '</title>',
    '<style>',
    'html,body{margin:0;width:100%;height:100%;background:#101827;color:#fff;overflow:hidden}',
    'body{display:grid;place-items:center;font:16px system-ui,sans-serif}',
    'canvas{display:block;width:min(100vw,1280px);height:min(100vh,720px);background:#111827}',
    '</style></head><body>',
    '<canvas id="pattern" width="1280" height="720" aria-label="moving WebRTC test pattern"></canvas>',
    '<script>',
    'const canvas=document.querySelector("#pattern");',
    'const ctx=canvas.getContext("2d");',
    'const started=performance.now();',
    'let frame=0;',
    'function draw(now){',
    '  const elapsed=now-started;',
    '  const seconds=elapsed/1000;',
    '  const hue=Math.floor((seconds*43)%360);',
    '  const w=canvas.width,h=canvas.height;',
    '  ctx.fillStyle="hsl("+hue+",55%,13%)";ctx.fillRect(0,0,w,h);',
    '  ctx.fillStyle="rgba(255,255,255,.08)";',
    '  for(let x=0;x<w;x+=80){ctx.fillRect(x,0,1,h)}',
    '  for(let y=0;y<h;y+=80){ctx.fillRect(0,y,w,1)}',
    '  const box=150;',
    '  const travel=w-box;',
    '  const phase=(seconds*230)% (travel*2);',
    '  const x=phase<travel?phase:(travel*2-phase);',
    '  const y=180+Math.sin(seconds*2.1)*95;',
    '  ctx.fillStyle="hsl("+((hue+145)%360)+",88%,58%)";',
    '  ctx.fillRect(x,y,box,box);',
    '  ctx.fillStyle="#fff";',
    '  ctx.font="bold 58px system-ui,sans-serif";',
    '  ctx.fillText("WEBRTC E2E TEST",54,92);',
    '  ctx.font="30px ui-monospace,monospace";',
    '  ctx.fillText("Frame: "+frame,54,610);',
    '  ctx.fillText("Monotonic: "+performance.now().toFixed(1)+" ms",54,655);',
    '  ctx.fillText("Pattern phase: "+(seconds%10).toFixed(2)+" s",54,700);',
    '  document.documentElement.dataset.frame=String(frame);',
    '  frame+=1;',
    '  requestAnimationFrame(draw);',
    '}',
    'requestAnimationFrame(draw);',
    '</script></body></html>'
  ].join('\n');
}

async function startPatternServer() {
  const patternPath = path.join(runtime.tempDir, 'test-pattern.html');
  await writeFile(patternPath, makePatternHtml(), 'utf8');
  const server = createServer(async (request, response) => {
    if (request.url === '/favicon.ico') {
      response.writeHead(204).end();
      return;
    }
    if (request.url !== '/') {
      response.writeHead(404, { 'content-type': 'text/plain' }).end('not found');
      return;
    }
    try {
      const html = await readFile(patternPath);
      response.writeHead(200, {
        'content-type': 'text/html; charset=utf-8',
        'cache-control': 'no-store'
      });
      response.end(html);
    } catch (error) {
      response.writeHead(500, { 'content-type': 'text/plain' });
      response.end(errorMessage(error));
    }
  });

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, SIGNALING_HOST, resolve);
  });
  runtime.patternServer = server;
  const address = server.address();
  ensure(address && typeof address === 'object', 'Pattern server did not bind a TCP port.');
  runtime.patternUrl = 'http://' + SIGNALING_HOST + ':' + address.port + '/';
  return { url: runtime.patternUrl, file: patternPath };
}

async function findChromeExecutable() {
  const candidates = [];
  const addCandidate = (candidate, source) => {
    if (candidate) {
      const resolvedPath = path.resolve(candidate);
      if (!candidates.some(item => item.path === resolvedPath)) {
        candidates.push({ path: resolvedPath, source });
      }
    }
  };

  addCandidate(process.env.CHROME_PATH, 'CHROME_PATH');
  addCandidate(process.env.CHROMIUM_PATH, 'CHROMIUM_PATH');
  addCandidate(process.env.PUPPETEER_EXECUTABLE_PATH, 'PUPPETEER_EXECUTABLE_PATH');

  const envRoots = [
    process.env.ProgramFiles,
    process.env['ProgramFiles(x86)'],
    process.env.LOCALAPPDATA,
    process.env.PROGRAMFILES,
    process.env.PROGRAMFILES_X86
  ].filter(Boolean);
  for (const root of envRoots) {
    addCandidate(path.join(root, 'Google', 'Chrome', 'Application', 'chrome.exe'), 'Windows Chrome install');
    addCandidate(path.join(root, 'Chromium', 'Application', 'chrome.exe'), 'Windows Chromium install');
  }

  addCandidate(
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'standard Windows Chrome path'
  );
  addCandidate(
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    'standard x86 Windows Chrome path'
  );

  if (process.platform === 'darwin') {
    addCandidate(
      '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
      'macOS Chrome install'
    );
    addCandidate('/Applications/Chromium.app/Contents/MacOS/Chromium', 'macOS Chromium install');
  } else if (process.platform !== 'win32') {
    addCandidate('/usr/bin/google-chrome', 'system Chrome');
    addCandidate('/usr/bin/google-chrome-stable', 'system Chrome');
    addCandidate('/usr/bin/chromium', 'system Chromium');
    addCandidate('/usr/bin/chromium-browser', 'system Chromium');
  }

  try {
    addCandidate(puppeteer.executablePath(), 'Puppeteer-managed Chrome for Testing');
  } catch {
    // A system browser may be installed without Puppeteer's downloaded browser.
  }

  for (const candidate of candidates) {
    try {
      await access(candidate.path, fsConstants.F_OK);
      return candidate;
    } catch {
      // Keep trying the next known installation location.
    }
  }
  throw new Error(
    'No Chrome/Chromium executable was found. Set CHROME_PATH or CHROMIUM_PATH and retry.'
  );
}

async function launchTestBrowser() {
  const executable = await findChromeExecutable();
  const profileDir = path.join(runtime.tempDir, 'chrome-profile');
  const captureSwitch =
    '--auto-select-tab-capture-source-by-title=' + PATTERN_TITLE;
  const args = [
    captureSwitch,
    '--allow-insecure-localhost',
    '--disable-background-timer-throttling',
    '--disable-renderer-backgrounding',
    '--disable-backgrounding-occluded-windows',
    '--no-first-run',
    '--no-default-browser-check',
    '--window-size=1440,1000'
  ];

  const browser = await puppeteer.launch({
    executablePath: executable.path,
    headless: false,
    userDataDir: profileDir,
    defaultViewport: { width: 1440, height: 1000 },
    args,
    timeout: 30_000,
    dumpio: false,
    handleSIGINT: false,
    handleSIGTERM: false,
    handleSIGHUP: false
  });
  runtime.browser = browser;
  runtime.browserProcess = browser.process();
  report.automation.browserDisconnected = false;
  report.automation.browserDisconnectedUnexpectedly = false;
  browser.on('disconnected', () => {
    report.automation.browserDisconnected = true;
    report.automation.browserDisconnectedAt = new Date().toISOString();
    report.automation.browserDisconnectedUnexpectedly = !runtime.cleanupStarted;
  });
  report.automation.browserConnectedAfterLaunch = browser.connected;
  ensure(browser.connected, 'Puppeteer browser disconnected immediately after launch.');
  ensure(runtime.browserProcess, 'Puppeteer did not return an owned Chrome process.');
  const initialPages = await browser.pages();
  report.automation.initialPageCount = initialPages.length;
  ensure(initialPages.length >= 1, 'Chrome launched without an initial page.');

  const spawnArgs = runtime.browserProcess.spawnargs ?? [];
  ensure(
    spawnArgs.some(arg => arg === captureSwitch),
    'The exact test-tab capture selection switch was not passed to Chrome.'
  );
  const captureSelectionArgs = spawnArgs.filter(arg => arg.startsWith('--auto-select-'));
  ensure(
    captureSelectionArgs.length === 1 && captureSelectionArgs[0] === captureSwitch,
    'Chrome capture selection must use only the exact test-tab title switch.'
  );
  ensure(
    !spawnArgs.some(arg =>
      arg.startsWith('--auto-select-desktop-capture-source') ||
      arg.startsWith('--auto-select-window-capture-source') ||
      arg.startsWith('--use-fake-device-for-media-stream') ||
      arg.startsWith('--use-fake-ui-for-media-stream')
    ),
    'Desktop/window auto-selection and fake-media flags must never be enabled.'
  );
  ensure(
    spawnArgs.includes('--allow-insecure-localhost'),
    'The temporary E2E Chrome must accept the local self-signed HTTPS certificate.'
  );
  const userDataArg = spawnArgs.find(arg => arg.startsWith('--user-data-dir='));
  ensure(
    userDataArg && path.resolve(userDataArg.slice('--user-data-dir='.length)) === path.resolve(profileDir),
    'Chrome is not using the harness temporary profile.'
  );

  report.automation.chromeExecutable = executable.path;
  report.automation.chromeDetection = executable.source;
  report.automation.temporaryProfile = profileDir;
  report.automation.chromeVersion = await browser.version();
  report.automation.localCertificateAccepted = true;
  report.automation.userDataDirVerified = true;
  report.automation.captureSwitchVerified = true;
}

function attachPageAudit(page, label, mode) {
  const audit = {
    label,
    mode,
    consoleDebug: [],
    consoleErrors: [],
    pageErrors: [],
    failedRequests: [],
    websocketEvents: [],
    websocketEventCounts: {},
    websocketUrls: [],
    encodedChunkCodecs: [],
    displayCapture: null
  };
  page.on('console', message => {
    const entry = {
      type: message.type(),
      text: message.text(),
      url: message.location()?.url ?? null
    };
    if (entry.type === 'debug') {
      audit.consoleDebug.push(entry);
    }
    if (entry.type === 'error') {
      audit.consoleErrors.push(entry);
    }
  });
  page.on('pageerror', error => {
    audit.pageErrors.push(errorMessage(error));
  });
  page.on('requestfailed', request => {
    const url = request.url();
    if (url.startsWith('http://127.0.0.1:') || url.startsWith('https://127.0.0.1:')) {
      audit.failedRequests.push({
        url,
        error: request.failure()?.errorText ?? 'unknown request failure'
      });
    }
  });
  runtime.pageAudits.push(audit);
  return audit;
}

async function installWebSocketAudit(page) {
  await page.evaluateOnNewDocument(() => {
    const nativeWebSocket = window.WebSocket;
    const mediaAudit = {
      events: [],
      eventCounts: {},
      urls: [],
      sockets: [],
      keyframeRequestEvents: [],
      requestedKeyframeChunks: [],
      signalingRequests: [],
      pendingSignalingRequests: {},
      consumeResponses: [],
    encodedChunkCodecs: [],
    encodedChunkLayers: [],
    configuredEncoderCodecs: [],
    outputEncoderCodecs: [],
    encoderOutputFrames: 0,
    videoEncoderConstructors: 0,
      displayCapture: {
        interceptionInstalled: false,
        calls: 0,
        resolved: 0,
        rejected: 0,
        pending: false,
        requests: []
      }
    };
    Object.defineProperty(window, '__webrtcMediaAudit', {
      configurable: false,
      value: mediaAudit
    });

    class AuditedWebSocket extends nativeWebSocket {
      constructor(...args) {
        super(...args);
        mediaAudit.urls.push(String(args[0] ?? ''));
        mediaAudit.sockets.push(this);
        this.addEventListener('message', event => {
          if (typeof event.data !== 'string') {
            return;
          }
          try {
            const payload = JSON.parse(event.data);
            const pendingRequest = payload.id
              ? mediaAudit.pendingSignalingRequests[payload.id]
              : null;
            if (pendingRequest) {
              delete mediaAudit.pendingSignalingRequests[payload.id];
              if (pendingRequest.action === 'consume' && payload.ok === true) {
                const data = payload.data ?? {};
                const rtpParameters = data.rtpParameters ?? {};
                mediaAudit.consumeResponses.push({
                  consumerId: typeof data.id === 'string' ? data.id : null,
                  kind: typeof data.kind === 'string' ? data.kind : null,
                  receivedAt: new Date().toISOString(),
                  requestedAt: pendingRequest.requestedAt,
                  codecs: Array.isArray(rtpParameters.codecs)
                    ? rtpParameters.codecs.map(codec => ({
                        mimeType: typeof codec?.mimeType === 'string' ? codec.mimeType : null,
                        payloadType: typeof codec?.payloadType === 'number' ? codec.payloadType : null,
                        clockRate: typeof codec?.clockRate === 'number' ? codec.clockRate : null,
                        parameters: codec?.parameters && typeof codec.parameters === 'object'
                          ? Object.fromEntries(Object.entries(codec.parameters).filter(([, value]) =>
                              ['string', 'number', 'boolean'].includes(typeof value)
                            ))
                          : {}
                      }))
                    : [],
                  encodings: Array.isArray(rtpParameters.encodings)
                    ? rtpParameters.encodings.map(encoding => ({
                        ssrc: typeof encoding?.ssrc === 'number' ? encoding.ssrc : null,
                        rtxSsrc: typeof encoding?.rtx?.ssrc === 'number'
                          ? encoding.rtx.ssrc
                          : null,
                        rid: typeof encoding?.rid === 'string' ? encoding.rid : null,
                        scalabilityMode: typeof encoding?.scalabilityMode === 'string'
                          ? encoding.scalabilityMode
                          : null
                      }))
                    : []
                });
                if (mediaAudit.consumeResponses.length > 20) mediaAudit.consumeResponses.shift();
              }
            }
            if (typeof payload.event === 'string') {
              mediaAudit.events.push(payload.event);
              mediaAudit.eventCounts[payload.event] =
                (mediaAudit.eventCounts[payload.event] ?? 0) + 1;
              if (mediaAudit.events.length > 1000) {
                mediaAudit.events.shift();
              }
              if (
                payload.event === 'keyframeRequest' &&
                payload.data &&
                typeof payload.data === 'object'
              ) {
                mediaAudit.keyframeRequestEvents.push({
                  ...payload.data,
                  receivedAt: Date.now()
                });
                if (mediaAudit.keyframeRequestEvents.length > 100) {
                  mediaAudit.keyframeRequestEvents.shift();
                }
              }
            }
          } catch {
            // Ignore text frames outside the app's JSON signaling protocol.
          }
        });
      }

      send(data) {
        try {
          if (typeof data === 'string') {
            const request = JSON.parse(data);
            if (typeof request.id === 'string' && typeof request.action === 'string') {
              const pendingRequest = {
                action: request.action,
                requestedAt: new Date().toISOString()
              };
              mediaAudit.pendingSignalingRequests[request.id] = pendingRequest;
              if (request.action === 'setConsumerLayer' &&
                  typeof request.data?.layer === 'string') {
                const layerRequest = {
                  requestId: request.id,
                  layer: request.data.layer,
                  requestedAt: pendingRequest.requestedAt
                };
                mediaAudit.signalingRequests.push(layerRequest);
                if (mediaAudit.signalingRequests.length > 100) {
                  mediaAudit.signalingRequests.shift();
                }
              }
            }
          }
          let bytes;
          if (data instanceof ArrayBuffer) {
            bytes = new Uint8Array(data);
          } else if (ArrayBuffer.isView(data)) {
            bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
          }
          if (bytes && bytes.byteLength >= 4) {
            const headerLength = new DataView(
              bytes.buffer,
              bytes.byteOffset,
              bytes.byteLength
            ).getUint32(0);
            const headerEnd = headerLength + 4;
            if (headerLength > 0 && headerEnd <= bytes.byteLength) {
              const header = JSON.parse(
                new TextDecoder().decode(bytes.subarray(4, headerEnd))
              );
              if (header.event === 'encodedChunk') {
                if (typeof header.layer === 'string') {
                  mediaAudit.encodedChunkLayers.push(header.layer);
                  if (mediaAudit.encodedChunkLayers.length > 500) {
                    mediaAudit.encodedChunkLayers.shift();
                  }
                }
                const codec = header.metadata?.decoderConfig?.codec;
                if (typeof codec === 'string') {
                  mediaAudit.encodedChunkCodecs.push(codec);
                  if (mediaAudit.encodedChunkCodecs.length > 100) {
                    mediaAudit.encodedChunkCodecs.shift();
                  }
                }
                if (header.requestedKeyframeAudit) {
                  mediaAudit.requestedKeyframeChunks.push({
                    ...header.requestedKeyframeAudit,
                    layer: header.layer,
                    chunkType: header.type,
                    encodedTimestamp: header.timestamp,
                    observedAt: Date.now()
                  });
                  if (mediaAudit.requestedKeyframeChunks.length > 100) {
                    mediaAudit.requestedKeyframeChunks.shift();
                  }
                }
              }
            }
          }
        } catch {
          // Malformed or non-app binary frames should not affect WebSocket sends.
        }
        return super.send(data);
      }
    }

    window.WebSocket = AuditedWebSocket;

    const mediaDevices = navigator.mediaDevices;
    const nativeGetDisplayMedia = mediaDevices?.getDisplayMedia;
    if (typeof nativeGetDisplayMedia === 'function') {
      Object.defineProperty(mediaDevices, 'getDisplayMedia', {
        configurable: true,
        writable: true,
        value: function (...args) {
          const request = {
            call: mediaAudit.displayCapture.calls + 1,
            calledAt: new Date().toISOString(),
            status: 'pending',
            resolvedAt: null,
            rejectedAt: null,
            errorName: null,
            errorMessage: null,
            videoTrackCount: null,
            videoTracks: []
          };
          mediaAudit.displayCapture.calls += 1;
          mediaAudit.displayCapture.requests.push(request);
          mediaAudit.displayCapture.pending = true;

          const rejectRequest = error => {
            request.status = 'rejected';
            request.rejectedAt = new Date().toISOString();
            request.errorName = error?.name ?? null;
            request.errorMessage = error?.message ?? String(error);
            mediaAudit.displayCapture.rejected += 1;
            mediaAudit.displayCapture.pending =
              mediaAudit.displayCapture.requests.some(item => item.status === 'pending');
          };

          let capturePromise;
          try {
            capturePromise = Reflect.apply(nativeGetDisplayMedia, this, args);
          } catch (error) {
            rejectRequest(error);
            throw error;
          }

          capturePromise.then(stream => {
            try {
              request.status = 'resolved';
              request.resolvedAt = new Date().toISOString();
              const videoTracks = stream.getVideoTracks();
              request.videoTrackCount = videoTracks.length;
              request.videoTracks = videoTracks.map(track => {
                const trackState = {
                  kind: track.kind,
                  label: track.label,
                  readyState: track.readyState,
                  endedAt: track.readyState === 'ended' ? new Date().toISOString() : null
                };
                track.addEventListener('ended', () => {
                  trackState.readyState = track.readyState;
                  trackState.endedAt = new Date().toISOString();
                }, { once: true });
                return trackState;
              });
              mediaAudit.displayCapture.resolved += 1;
              mediaAudit.displayCapture.pending =
                mediaAudit.displayCapture.requests.some(item => item.status === 'pending');
            } catch (error) {
              request.auditError = error?.message ?? String(error);
            }
          }, rejectRequest).catch(error => {
            request.auditError = error?.message ?? String(error);
          });
          return capturePromise;
        }
      });
      mediaAudit.displayCapture.interceptionInstalled = true;
    }

    const nativeVideoEncoder = window.VideoEncoder;
    if (typeof nativeVideoEncoder === 'function') {
      class AuditedVideoEncoder extends nativeVideoEncoder {
        constructor(init) {
          mediaAudit.videoEncoderConstructors += 1;
          const output = init.output;
          super({
            ...init,
            output: (chunk, metadata) => {
              mediaAudit.encoderOutputFrames += 1;
              const codec = metadata?.decoderConfig?.codec;
              if (typeof codec === 'string') {
                mediaAudit.outputEncoderCodecs.push(codec);
                if (mediaAudit.outputEncoderCodecs.length > 100) {
                  mediaAudit.outputEncoderCodecs.shift();
                }
              }
              output(chunk, metadata);
            }
          });
        }

        configure(config) {
          if (typeof config?.codec === 'string') {
            mediaAudit.configuredEncoderCodecs.push(config.codec);
          }
          return super.configure(config);
        }
      }
      window.VideoEncoder = AuditedVideoEncoder;
    }
  });
}

async function syncWebSocketAudit(page, audit) {
  const current = await page.evaluate(() => {
    const value = window.__webrtcMediaAudit;
    return value
      ? {
          websocketEvents: value.events,
          websocketEventCounts: value.eventCounts,
          websocketUrls: value.urls,
          keyframeRequestEvents: value.keyframeRequestEvents,
          requestedKeyframeChunks: value.requestedKeyframeChunks,
          signalingRequests: value.signalingRequests,
          consumeResponses: value.consumeResponses,
          encodedChunkCodecs: value.encodedChunkCodecs,
          encodedChunkLayers: value.encodedChunkLayers,
          configuredEncoderCodecs: value.configuredEncoderCodecs,
          outputEncoderCodecs: value.outputEncoderCodecs,
          encoderOutputFrames: value.encoderOutputFrames,
          videoEncoderConstructors: value.videoEncoderConstructors,
          displayCapture: value.displayCapture
        }
      : null;
  });
  if (current) {
    Object.assign(audit, current);
  }
  return audit;
}

function assertBrowserWebSocketProxy(audit, description) {
  const appWebSocketUrls = (audit.websocketUrls ?? []).filter(url => {
    try {
      return new URL(url).pathname === '/ws';
    } catch {
      return false;
    }
  });
  const expectedOrigin = 'wss://' + DEV_HOST + ':' + DEV_PORT;
  ensure(
    appWebSocketUrls.some(url => url.startsWith(expectedOrigin + '/ws')),
    description + ' did not connect to the secure WebSocket gateway at ' +
      expectedOrigin + '/ws. Observed: ' + JSON.stringify(appWebSocketUrls)
  );
  ensure(
    appWebSocketUrls.every(url => url.startsWith(expectedOrigin + '/ws')),
    description + ' attempted a direct backend WebSocket connection. Observed: ' +
      JSON.stringify(appWebSocketUrls)
  );
  return appWebSocketUrls;
}

async function requestOverAuditedSocket(page, action, data) {
  return page.evaluate(({ action, data }) => {
    const mediaAudit = window.__webrtcMediaAudit;
    const socket = [...(mediaAudit?.sockets ?? [])]
      .reverse()
      .find(candidate => candidate.readyState === WebSocket.OPEN);
    if (!socket) {
      throw new Error('No open audited signaling WebSocket is available.');
    }

    const id = 'e2e-keyframe-' + Date.now() + '-' + Math.random().toString(16).slice(2);
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        socket.removeEventListener('message', onMessage);
        reject(new Error('Timed out waiting for E2E signaling response.'));
      }, 15_000);
      const onMessage = event => {
        if (typeof event.data !== 'string') {
          return;
        }
        let message;
        try {
          message = JSON.parse(event.data);
        } catch {
          return;
        }
        if (message.id !== id) {
          return;
        }
        clearTimeout(timeout);
        socket.removeEventListener('message', onMessage);
        if (!message.ok) {
          resolve({
            e2eOk: false,
            e2eError: message.error ?? 'E2E signaling request failed.',
            spatialLayerDiagnostics: message.spatialLayerDiagnostics ?? null,
            actionSentAt: Date.now()
          });
          return;
        }
        resolve({ e2eOk: true, ...message.data, actionSentAt: Date.now() });
      };
      socket.addEventListener('message', onMessage);
      socket.send(JSON.stringify({ id, action, data }));
    });
  }, { action, data });
}

async function waitForKeyframeAudit(page, requestId, description, timeoutMs = 15_000) {
  return waitForCondition(async () => {
    const current = await page.evaluate(() => {
      const mediaAudit = window.__webrtcMediaAudit;
      return {
        events: mediaAudit?.keyframeRequestEvents ?? [],
        chunks: mediaAudit?.requestedKeyframeChunks ?? []
      };
    });
    const event = current.events.find(item => item.requestId === requestId);
    const chunk = current.chunks.find(item =>
      item.requests?.some(request => request.requestId === requestId)
    );
    return event && chunk ? { event, chunk } : null;
  }, description, timeoutMs, RUNTIME_POLL_INTERVAL_MS);
}

async function captureProducerCycleEvidence(cycleTrace, cycleLabel) {
  if (!cycleTrace) {
    return;
  }
  const relevantAudits = runtime.pageAudits.filter(audit =>
    audit.label === cycleLabel + '-pattern' || audit.label === cycleLabel + '-producer'
  );
  const snapshots = [];
  for (const audit of relevantAudits) {
    const page = runtime.pageAuditPages.get(audit);
    if (page && !page.isClosed()) {
      try {
        await syncWebSocketAudit(page, audit);
      } catch {
        // Keep the last successfully captured audit if the page disconnected.
      }
    }
    let url = null;
    let title = null;
    if (page) {
      try {
        url = page.url();
      } catch {
        // The URL may no longer be available after a renderer disconnect.
      }
      if (!page.isClosed()) {
        try {
          title = await page.title();
        } catch {
          // Preserve the other page evidence if title retrieval fails.
        }
      }
    }
    snapshots.push({
      label: audit.label,
      mode: audit.mode,
      url,
      title,
      closed: page ? page.isClosed() : null,
      consoleErrors: audit.consoleErrors.slice(-20),
      pageErrors: audit.pageErrors.slice(-20),
      failedRequests: audit.failedRequests.slice(-20),
      websocketEventCounts: audit.websocketEventCounts,
      websocketUrls: audit.websocketUrls,
      displayCapture: audit.displayCapture,
      encoderOutputFrames: audit.encoderOutputFrames,
      videoEncoderConstructors: audit.videoEncoderConstructors,
      configuredEncoderCodecs: audit.configuredEncoderCodecs,
      outputEncoderCodecs: audit.outputEncoderCodecs,
      encodedChunkCodecs: audit.encodedChunkCodecs,
      encodedChunkLayers: audit.encodedChunkLayers
    });
    if (audit.label === cycleLabel + '-producer') {
      cycleTrace.getDisplayMediaAudit = audit.displayCapture;
      if (page && !page.isClosed()) {
        try {
          cycleTrace.lastUiSnapshot = await readUiSnapshot(page);
        } catch {
          // Preserve the previous UI snapshot if the renderer stopped responding.
        }
      }
    }
  }
  cycleTrace.pageAudits = snapshots;
  if (cycleTrace.lastHealthSnapshot === null && currentServerRecord) {
    try {
      cycleTrace.lastHealthSnapshot = await fetchJson(HEALTH_URL);
    } catch (error) {
      cycleTrace.lastHealthSnapshot = { error: errorMessage(error) };
    }
  }
}

async function newPage(label, mode) {
  checkInterrupted();
  let page;
  try {
    page = await runtime.browser.newPage();
  } catch (error) {
    throw new Error(
      'Failed to create ' + label + ' page: ' + errorMessage(error),
      { cause: error }
    );
  }
  runtime.pages.add(page);
  page.once('close', () => runtime.pages.delete(page));
  page.setDefaultTimeout(PAGE_TIMEOUT_MS);
  page.setDefaultNavigationTimeout(PAGE_TIMEOUT_MS);
  const audit = attachPageAudit(page, label, mode);
  runtime.pageAuditPages.set(audit, page);
  await installWebSocketAudit(page);
  return { page, audit };
}

async function installConsumerRtcStatsProbe(page, injectedFailureCall = null) {
  await page.evaluateOnNewDocument(failCallNumber => {
    const peerConnection = window.RTCPeerConnection;
    const prototype = peerConnection?.prototype;
    const descriptor = prototype && Object.getOwnPropertyDescriptor(prototype, 'getStats');
    if (!prototype || !descriptor || typeof descriptor.value !== 'function') {
      return;
    }

    const nativeGetStats = descriptor.value;
    const audit = {
      installed: true,
      failureInjectionCall: failCallNumber,
      injectedFailureObserved: false,
      calls: 0,
      resolvedCalls: 0,
      rejectedCalls: 0,
      inFlight: 0,
      maximumConcurrentCalls: 0,
      reportsDroppedFromBuffer: 0,
      reports: [],
      failures: []
    };
    Object.defineProperty(window, '__consumerRtcStatsProbe', {
      configurable: false,
      value: audit
    });

    Object.defineProperty(prototype, 'getStats', {
      ...descriptor,
      value: function (...args) {
        audit.calls += 1;
        const callNumber = audit.calls;
        audit.inFlight += 1;
        audit.maximumConcurrentCalls = Math.max(
          audit.maximumConcurrentCalls,
          audit.inFlight
        );
        const finish = () => {
          audit.inFlight = Math.max(0, audit.inFlight - 1);
        };
        if (failCallNumber === callNumber) {
          audit.injectedFailureObserved = true;
          audit.rejectedCalls += 1;
          audit.failures.push({
            callNumber,
            at: new Date().toISOString(),
            injected: true,
            name: 'E2EInjectedGetStatsFailure'
          });
          return Promise.reject(new Error('E2E injected getStats rejection')).finally(finish);
        }

        let nativeResult;
        try {
          nativeResult = Reflect.apply(nativeGetStats, this, args);
        } catch (error) {
          audit.rejectedCalls += 1;
          audit.failures.push({
            callNumber,
            at: new Date().toISOString(),
            injected: false,
            name: error?.name ?? 'Error'
          });
          finish();
          return Promise.reject(error);
        }

        return Promise.resolve(nativeResult).then(report => {
          const statsById = new Map([...report.values()]
            .filter(stat => typeof stat?.id === 'string')
            .map(stat => [stat.id, stat]));
          const inboundVideoStats = [...report.values()]
            .filter(stat => stat?.type === 'inbound-rtp' && stat?.kind === 'video')
            .map(stat => {
              const codec = typeof stat.codecId === 'string'
                ? statsById.get(stat.codecId)
                : null;
              return {
                type: typeof stat.type === 'string' ? stat.type : null,
                id: typeof stat.id === 'string' ? stat.id : null,
                timestamp: typeof stat.timestamp === 'number' ? stat.timestamp : null,
                kind: typeof stat.kind === 'string' ? stat.kind : null,
                ssrc: typeof stat.ssrc === 'number' ? stat.ssrc : null,
                codecId: typeof stat.codecId === 'string' ? stat.codecId : null,
                codec: codec?.type === 'codec' ? {
                  id: typeof codec.id === 'string' ? codec.id : null,
                  mimeType: typeof codec.mimeType === 'string' ? codec.mimeType : null,
                  payloadType: typeof codec.payloadType === 'number' ? codec.payloadType : null,
                  clockRate: typeof codec.clockRate === 'number' ? codec.clockRate : null,
                  sdpFmtpLine: typeof codec.sdpFmtpLine === 'string'
                    ? codec.sdpFmtpLine
                    : null
                } : null,
                transportId: typeof stat.transportId === 'string' ? stat.transportId : null,
                mid: typeof stat.mid === 'string' ? stat.mid : null,
                trackIdentifier: typeof stat.trackIdentifier === 'string'
                  ? stat.trackIdentifier
                  : null,
                rtxSsrc: typeof stat.rtxSsrc === 'number' ? stat.rtxSsrc : null,
                retransmittedPacketsReceived:
                  typeof stat.retransmittedPacketsReceived === 'number'
                    ? stat.retransmittedPacketsReceived
                    : null,
                packetsReceived: typeof stat.packetsReceived === 'number'
                  ? stat.packetsReceived : null,
                packetsLost: typeof stat.packetsLost === 'number'
                  ? stat.packetsLost : null,
                bytesReceived: typeof stat.bytesReceived === 'number'
                  ? stat.bytesReceived : null,
                framesReceived: typeof stat.framesReceived === 'number'
                  ? stat.framesReceived : null,
                framesDecoded: typeof stat.framesDecoded === 'number'
                  ? stat.framesDecoded : null,
                framesDropped: typeof stat.framesDropped === 'number'
                  ? stat.framesDropped : null,
                jitter: typeof stat.jitter === 'number' ? stat.jitter : null
              };
            });
          audit.resolvedCalls += 1;
          audit.reports.push({
            callNumber,
            completedAt: new Date().toISOString(),
            inboundVideoStats
          });
          if (audit.reports.length > 2000) {
            audit.reports.shift();
            audit.reportsDroppedFromBuffer += 1;
          }
          return report;
        }, error => {
          audit.rejectedCalls += 1;
          audit.failures.push({
            callNumber,
            at: new Date().toISOString(),
            injected: false,
            name: error?.name ?? 'Error'
          });
          throw error;
        }).finally(finish);
      }
    });
  }, injectedFailureCall);
}

async function readConsumerRtcStatsProbe(page) {
  return page.evaluate(() => {
    const audit = window.__consumerRtcStatsProbe;
    return audit ? JSON.parse(JSON.stringify(audit)) : null;
  });
}

async function readConsumerDiagnosticContext(page) {
  const signaling = await page.evaluate(() => {
    const audit = window.__webrtcMediaAudit;
    const remoteVideo = document.querySelector('#remoteVideo');
    const track = remoteVideo?.srcObject instanceof MediaStream
      ? remoteVideo.srcObject.getVideoTracks()[0]
      : null;
    return {
      consumeResponses: audit?.consumeResponses ?? [],
      signalingRequests: audit?.signalingRequests ?? [],
      consumerTrackId: track?.id ?? null,
      peerId: document.querySelector('#peerId')?.textContent ?? null
    };
  });
  return { ...signaling, rtcStatsProbe: await readConsumerRtcStatsProbe(page) };
}

async function loadVisualizerJson(page, filename, contents) {
  await page.evaluate(({ fileName, jsonText }) => {
    const input = document.querySelector('#stats-file');
    const transfer = new DataTransfer();
    transfer.items.add(new File([jsonText], fileName, { type: 'application/json' }));
    input.files = transfer.files;
    input.dispatchEvent(new Event('change', { bubbles: true }));
  }, { fileName: filename, jsonText: contents });
  await page.waitForFunction(() => {
    const dashboard = document.querySelector('#dashboard');
    const error = document.querySelector('#load-error');
    const renderer = document.querySelector('#renderer-status');
    return error && !error.hidden ||
      dashboard && !dashboard.hidden && (
        renderer?.textContent === 'Renderer: Chart.js' ||
        renderer?.textContent === 'Renderer: SVG (Chart.js unavailable)'
      );
  });
}

function createVisualizerMetadata(role, count, options = {}) {
  const start = Date.parse('2026-09-23T08:00:00.000Z');
  return {
    peerId: options.peerId ?? `synthetic-${role}`,
    role,
    measurementStartTime: new Date(start).toISOString(),
    dumpTime: new Date(start + (count + 1) * 1000).toISOString(),
    sampleIntervalMs: 1000,
    userAgent: 'E2E synthetic fixture',
    simulcastEnabled: options.simulcastEnabled ?? true,
    availableLayers: options.availableLayers ?? ['low', 'mid', 'high']
  };
}

function createSyntheticProducerDump(count, simulcastEnabled, options = {}) {
  const availableLayers = simulcastEnabled ? ['low', 'mid', 'high'] : ['high'];
  const producerStats = Array.from({ length: count }, (_, offset) => {
    const index = offset + 1;
    const timestamp = new Date(
      Date.parse('2026-09-23T08:00:00.000Z') + index * 1000
    ).toISOString();
    const layers = Object.fromEntries(availableLayers.map((layer, layerIndex) => {
      const value = layerIndex + 1;
      const spike = index === Math.ceil(count / 2) ? 9000 : 300 * value;
      return [layer, {
        targetBitrateBps: 250000 * value,
        encodedFps: 30 + value,
        encodedChunksDelta: 30 + value,
        encodedChunksTotal: index * (30 + value),
        encodedPayloadBytesDelta: 40000 * value,
        encodedPayloadBytesTotal: index * 40000 * value,
        encodedPayloadBitrateKbps: spike,
        keyFramesDelta: index % 60 === 1 ? 1 : 0,
        keyFramesTotal: Math.ceil(index / 60),
        deltaFramesDelta: index % 60 === 1 ? 29 : 30,
        deltaFramesTotal: index - Math.ceil(index / 60),
        encodeQueueSize: index % 8,
        width: 320 * value,
        height: 180 * value
      }];
    }));
    return {
      timestamp,
      elapsedMs: 1000,
      sourceFps: options.gapSource && [3, 4].includes(index)
        ? null
        : index === 1 ? 0 : 30,
      sourceFramesDelta: 30,
      sourceFramesTotal: index * 30,
      layers,
      socketBufferedAmountBytes: index % 4,
      browserJsHeapUsedBytes: null
    };
  });
  return {
    schemaVersion: 1,
    metadata: createVisualizerMetadata('producer', count, {
      ...options,
      simulcastEnabled,
      availableLayers
    }),
    producerStats,
    consumerStats: []
  };
}

function createSyntheticConsumerDump(count, options = {}) {
  const consumerStats = Array.from({ length: count }, (_, offset) => {
    const index = offset + 1;
    return {
      timestamp: new Date(
        Date.parse('2026-09-23T08:00:00.000Z') + index * 1000
      ).toISOString(),
      elapsedMs: 1000,
      packetsReceived: index * 30,
      packetsLost: options.negativePacketsLost && index === 2
        ? -2
        : index % 200 === 0 ? 1 : 0,
      maxJitterMs: index % 40,
      framesReceived: index * 30,
      framesDecoded: index * 29,
      framesDropped: index % 500 === 0 ? 1 : 0,
      bytesReceived: index * 64000,
      receivedFps: [3, 4].includes(index) ? null : 30,
      decodedFps: 29,
      receivedMediaBitrateKbps: index === Math.ceil(count / 2) ? 9000 : 512,
      browserJsHeapUsedBytes: null
    };
  });
  return {
    schemaVersion: 1,
    metadata: createVisualizerMetadata('consumer', count, {
      simulcastEnabled: true,
      availableLayers: ['low', 'mid', 'high']
    }),
    producerStats: [],
    consumerStats
  };
}

async function readVisualizerSnapshot(page) {
  return page.evaluate(() => ({
    title: document.querySelector('#dump-title')?.textContent ?? '',
    metrics: document.querySelector('#metrics')?.textContent ?? '',
    metadata: document.querySelector('#metadata')?.textContent ?? '',
    renderer: document.querySelector('#renderer-status')?.textContent ?? '',
    chartCount: document.querySelectorAll('#charts .chart-card').length,
    canvasCount: document.querySelectorAll('#charts canvas').length,
    svgCount: document.querySelectorAll('#charts svg').length,
    peerNodeInjected: Boolean(document.querySelector('#metadata img')),
    xssExecuted: window.__statsXss === 1,
    errorVisible: !document.querySelector('#load-error')?.hidden,
    sourceGapPaths: document.querySelectorAll(
      '#charts polyline[data-series-label="Source FPS"]'
    ).length,
    negativePacketsLostVisible: [...document.querySelectorAll('#charts .chart-card')]
      .some(card => card.querySelector('h4')?.textContent?.includes('Packets received and lost') &&
        card.querySelector('svg')?.textContent?.includes('-2'))
  }));
}

async function verifyStatsVisualizer(appUrl) {
  const { page } = await newPage('stats-visualizer-smoke', 'production');
  const pageErrors = [];
  let chartAssetUrl = null;
  page.on('pageerror', error => pageErrors.push(error.message));
  page.on('console', message => {
    if (message.type() === 'error') pageErrors.push(message.text());
  });
  page.on('request', request => {
    if (/\/assets\/chartjs-[^/]+\.js(?:\?.*)?$/.test(request.url())) {
      chartAssetUrl = request.url();
    }
  });

  try {
    const baseUrl = appUrl.replace(/\/$/, '');
    const response = await page.goto(
      baseUrl + '/stats-visualizer.html',
      { waitUntil: 'domcontentloaded' }
    );
    ensure(response?.status() === 200, 'Production stats visualizer did not return HTTP 200.');
    await page.waitForSelector('#stats-file');
    await page.waitForFunction(() =>
      document.documentElement.dataset.statsVisualizerReady === 'true'
    );

    const xssPeerId = '<img src=x onerror="window.__statsXss=1">';
    const producerFixture = createSyntheticProducerDump(6, true, {
      peerId: xssPeerId,
      gapSource: true
    });
    const consumerFixture = createSyntheticConsumerDump(6);
    const producerJson = JSON.stringify(producerFixture);
    const consumerJson = JSON.stringify(consumerFixture);
    await loadVisualizerJson(page, 'producer-small.json', producerJson);
    const producerRender = await readVisualizerSnapshot(page);
    ensure(producerRender.title.includes('Producer'), 'Producer fixture did not render in the Visualizer.');
    ensure(producerRender.metrics.includes('Source FPS'), 'Producer dashboard did not show Source FPS.');
    ensure(producerRender.chartCount === 5 && producerRender.canvasCount === 5,
      'Chart.js did not render all Producer charts.');
    ensure(producerRender.renderer === 'Renderer: Chart.js', 'Chart.js is not the production default renderer.');
    ensure(
      producerRender.metadata.includes(xssPeerId) &&
        !producerRender.peerNodeInjected && !producerRender.xssExecuted,
      'Visualizer treated uploaded Producer metadata as HTML.'
    );
    ensure(!producerRender.errorVisible, 'Valid Producer dump showed an error.');

    await loadVisualizerJson(page, 'consumer-small.json', consumerJson);
    const consumerRender = await readVisualizerSnapshot(page);
    ensure(consumerRender.title.includes('Consumer'), 'Consumer fixture did not render in the Visualizer.');
    ensure(
      consumerRender.metrics.includes('Received FPS') &&
        consumerRender.metrics.includes('Decoded FPS'),
      'Consumer dashboard did not show Received FPS and Decoded FPS.'
    );
    ensure(consumerRender.chartCount === 6 && consumerRender.canvasCount === 6,
      'Chart.js did not render all Consumer charts.');
    ensure(consumerRender.renderer === 'Renderer: Chart.js', 'Consumer did not use Chart.js.');
    ensure(!consumerRender.errorVisible, 'Valid Consumer dump showed an error.');

    await loadVisualizerJson(page, 'producer-repeat.json', producerJson);
    const repeatRender = await readVisualizerSnapshot(page);
    ensure(repeatRender.chartCount === 5 && repeatRender.canvasCount === 5,
      'Repeated Producer → Consumer → Producer loading left stale charts behind.');

    const largeRuns = [];
    for (const count of [1800, 3600, 4200]) {
      const producerDump = createSyntheticProducerDump(count, true);
      const producerContents = JSON.stringify(producerDump);
      const producerStart = Date.now();
      await loadVisualizerJson(page, `producer-on-${count}.json`, producerContents);
      const producerLarge = await readVisualizerSnapshot(page);
      const producerRenderMs = Date.now() - producerStart;
      ensure(producerLarge.renderer === 'Renderer: Chart.js' && producerLarge.canvasCount === 5,
        `Producer Simulcast ON ${count}-sample Chart.js render failed.`);
      ensure(producerLarge.metrics.includes('LOW encoded FPS') &&
        producerLarge.metrics.includes('MID encoded FPS') &&
        producerLarge.metrics.includes('HIGH encoded FPS'),
      `Producer Simulcast ON ${count}-sample render omitted an active layer.`);
      largeRuns.push({
        role: 'producer', simulcast: 'on', sampleCount: count,
        jsonBytes: Buffer.byteLength(producerContents),
        renderer: producerLarge.renderer, renderMs: producerRenderMs,
        charts: producerLarge.chartCount
      });

      const consumerDump = createSyntheticConsumerDump(count);
      const consumerContents = JSON.stringify(consumerDump);
      const consumerStart = Date.now();
      await loadVisualizerJson(page, `consumer-${count}.json`, consumerContents);
      const consumerLarge = await readVisualizerSnapshot(page);
      const consumerRenderMs = Date.now() - consumerStart;
      ensure(consumerLarge.renderer === 'Renderer: Chart.js' && consumerLarge.canvasCount === 6,
        `Consumer ${count}-sample Chart.js render failed.`);
      ensure(consumerLarge.metrics.includes('Packets received') &&
        consumerLarge.metrics.includes('Cumulative bytes received'),
      `Consumer ${count}-sample render omitted expected metrics.`);
      largeRuns.push({
        role: 'consumer', sampleCount: count,
        jsonBytes: Buffer.byteLength(consumerContents),
        renderer: consumerLarge.renderer, renderMs: consumerRenderMs,
        charts: consumerLarge.chartCount
      });
    }

    const offContents = JSON.stringify(createSyntheticProducerDump(4200, false));
    const offStart = Date.now();
    await loadVisualizerJson(page, 'producer-off-4200.json', offContents);
    const offRender = await readVisualizerSnapshot(page);
    ensure(offRender.renderer === 'Renderer: Chart.js' && offRender.canvasCount === 5,
      'Producer Simulcast OFF large file did not render with Chart.js.');
    ensure(offRender.metrics.includes('HIGH encoded FPS') &&
      !offRender.metrics.includes('LOW encoded FPS') &&
      !offRender.metrics.includes('MID encoded FPS'),
    'Producer Simulcast OFF visualizer showed inactive layer metrics.');
    largeRuns.push({
      role: 'producer', simulcast: 'off', sampleCount: 4200,
      jsonBytes: Buffer.byteLength(offContents),
      renderer: offRender.renderer, renderMs: Date.now() - offStart,
      charts: offRender.chartCount
    });

    await loadVisualizerJson(page, 'malformed.json', '{');
    const invalidJsonError = await page.$eval('#load-error', element => ({
      hidden: element.hidden,
      text: element.textContent ?? ''
    }));
    ensure(!invalidJsonError.hidden && /not valid JSON/i.test(invalidJsonError.text),
      'Malformed JSON did not produce a user-visible validation error.');

    await loadVisualizerJson(page, 'unsupported.json', JSON.stringify({ schemaVersion: 2 }));
    const unsupportedSchemaError = await page.$eval('#load-error', element => ({
      hidden: element.hidden,
      text: element.textContent ?? ''
    }));
    ensure(!unsupportedSchemaError.hidden && /unsupported stats schema/i.test(unsupportedSchemaError.text),
      'Unsupported schema did not produce a user-visible validation error.');

    await loadVisualizerJson(page, 'legacy.json', JSON.stringify({ metadata: {}, producerStats: [] }));
    const legacyError = await page.$eval('#load-error', element => element.textContent ?? '');
    ensure(/unsupported stats schema/i.test(legacyError), 'Legacy schema was not rejected explicitly.');

    const invalidTimeDump = createSyntheticProducerDump(2, true);
    invalidTimeDump.producerStats[1].timestamp = 'not-a-timestamp';
    await loadVisualizerJson(page, 'invalid-time.json', JSON.stringify(invalidTimeDump));
    const invalidTimeError = await page.$eval('#load-error', element => element.textContent ?? '');
    ensure(/timestamps must be valid, unique, and increasing/i.test(invalidTimeError),
      'Invalid sample time was not rejected instead of fabricating an X coordinate.');

    ensure(pageErrors.length === 0,
      'Chart.js normal-path or large-file smoke logged browser errors: ' + pageErrors.join(' | '));
    ensure(chartAssetUrl !== null, 'The production Chart.js module chunk request was not observed.');

    const fallback = await verifyStatsVisualizerFallback(baseUrl, chartAssetUrl);
    return {
      status: 'passed',
      url: baseUrl + '/stats-visualizer.html',
      httpStatus: response.status(),
      producerSampleCount: producerFixture.producerStats.length,
      producerChartCount: producerRender.chartCount,
      consumerSampleCount: consumerFixture.consumerStats.length,
      consumerChartCount: consumerRender.chartCount,
      renderer: 'Chart.js',
      chartModuleUrl: chartAssetUrl,
      largeRuns,
      repeatedProducerConsumerProducerLoad: true,
      malformedJsonRejected: true,
      unsupportedSchemaRejected: true,
      legacySchemaRejected: true,
      invalidTimestampRejected: true,
      uploadedStringsRenderedAsText: true,
      browserErrors: pageErrors,
      fallback
    };
  } finally {
    if (!page.isClosed()) {
      await page.close().catch(() => {});
    }
  }
}

async function verifyStatsVisualizerFallback(appUrl, chartAssetUrl) {
  const { page } = await newPage('stats-visualizer-svg-fallback', 'production');
  let blockedChartRequests = 0;
  const pageErrors = [];
  page.on('pageerror', error => pageErrors.push(error.message));
  await page.setRequestInterception(true);
  page.on('request', request => {
    if (request.url() === chartAssetUrl) {
      blockedChartRequests += 1;
      void request.abort('failed').catch(() => {});
    } else {
      void request.continue().catch(() => {});
    }
  });

  try {
    const response = await page.goto(appUrl + '/stats-visualizer.html', {
      waitUntil: 'domcontentloaded'
    });
    ensure(response?.status() === 200, 'SVG fallback page did not return HTTP 200.');
    await page.waitForSelector('#stats-file');
    const producerFixture = createSyntheticProducerDump(6, true, { gapSource: true });
    await loadVisualizerJson(page, 'fallback-producer.json', JSON.stringify(producerFixture));
    const producer = await readVisualizerSnapshot(page);
    ensure(producer.renderer === 'Renderer: SVG (Chart.js unavailable)',
      'Chart.js module failure did not select the SVG renderer.');
    ensure(producer.svgCount === 5 && producer.canvasCount === 0,
      'Producer SVG fallback did not render all charts.');
    ensure(producer.metrics.includes('Source FPS') && producer.metadata.toLowerCase().includes('peer'),
      'Producer fallback lost its metrics or metadata.');
    ensure(producer.sourceGapPaths === 2,
      'SVG fallback joined Source FPS points across a null measurement gap.');
    await loadVisualizerJson(page, 'fallback-consumer.json', JSON.stringify(
      createSyntheticConsumerDump(6, { negativePacketsLost: true })
    ));
    const consumer = await readVisualizerSnapshot(page);
    ensure(consumer.renderer === 'Renderer: SVG (Chart.js unavailable)',
      'Consumer did not retain SVG fallback after Chart.js failed.');
    ensure(consumer.svgCount === 6 && consumer.canvasCount === 0,
      'Consumer SVG fallback did not render all charts.');
    ensure(consumer.metrics.includes('Received FPS') && consumer.metadata.toLowerCase().includes('peer'),
      'Consumer fallback lost its metrics or metadata.');
    ensure(consumer.negativePacketsLostVisible,
      'SVG fallback clipped the valid negative packetsLost sample.');
    await loadVisualizerJson(page, 'fallback-producer-repeat.json', JSON.stringify(producerFixture));
    const repeated = await readVisualizerSnapshot(page);
    ensure(repeated.svgCount === 5 && repeated.canvasCount === 0,
      'Repeated SVG fallback loads left stale charts behind.');
    ensure(blockedChartRequests >= 1, 'The E2E did not block the Chart.js module request.');
    ensure(pageErrors.length === 0,
      'A caught Chart.js load failure caused an uncaught page error: ' + pageErrors.join(' | '));
    return {
      status: 'passed',
      failureMode: 'Chart.js module request blocked',
      blockedChartRequests,
      producerSvgCharts: producer.svgCount,
      consumerSvgCharts: consumer.svgCount,
      nullGapSegments: producer.sourceGapPaths,
      repeatedLoad: true,
      uncaughtPageErrors: pageErrors
    };
  } finally {
    if (!page.isClosed()) {
      await page.close().catch(() => {});
    }
  }
}

async function downloadStatsDumpFromPage(page, label, progress = null) {
  const downloadPath = path.join(runtime.tempDir, 'downloads', label);
  let phase = 'create-download-directory';
  await mkdir(downloadPath, { recursive: true });
  let client;
  try {
    phase = 'create-download-session';
    if (progress) progress.phase = phase;
    client = await page.target().createCDPSession();
    phase = 'allow-browser-download';
    if (progress) progress.phase = phase;
    await client.send('Browser.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath
    });
    phase = 'locate-enabled-button';
    if (progress) progress.phase = phase;
    await client.send('DOM.enable');
    const { root } = await client.send('DOM.getDocument', { depth: 1 });
    const { nodeId } = await client.send('DOM.querySelector', {
      nodeId: root.nodeId,
      selector: '#dumpStatsButton'
    });
    ensure(nodeId > 0, label + ' Dump Stats button was not found in the browser DOM.');
    const { attributes } = await client.send('DOM.getAttributes', { nodeId });
    ensure(!attributes.filter((value, index) => index % 2 === 0).includes('disabled'),
      label + ' Dump Stats button was disabled after the session started.');
    const { model } = await client.send('DOM.getBoxModel', { nodeId });
    const content = model.content;
    const x = content.filter((value, index) => index % 2 === 0)
      .reduce((sum, value) => sum + value, 0) / 4;
    const y = content.filter((value, index) => index % 2 === 1)
      .reduce((sum, value) => sum + value, 0) / 4;
    phase = 'dispatch-dump-button-click';
    if (progress) progress.phase = phase;
    await client.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y });
    await client.send('Input.dispatchMouseEvent', {
      type: 'mousePressed', x, y, button: 'left', clickCount: 1
    });
    await client.send('Input.dispatchMouseEvent', {
      type: 'mouseReleased', x, y, button: 'left', clickCount: 1
    });
    phase = 'wait-for-json-file';
    if (progress) progress.phase = phase;
    const filename = await waitForCondition(async () => {
      const files = await readdir(downloadPath);
      return files.find(file => file.endsWith('.json') && !file.endsWith('.crdownload')) ?? null;
    }, label + ' JSON stats download', 15_000, 150);
    phase = 'read-downloaded-json';
    if (progress) progress.phase = phase;
    const dump = JSON.parse(await readFile(path.join(downloadPath, filename), 'utf8'));
    return { filename, dump };
  } catch (error) {
    if (progress) progress.phase = phase;
    throw new Error(label + ' stats download failed during ' + phase + ': ' + errorMessage(error), {
      cause: error
    });
  } finally {
    await client?.detach().catch(() => {});
  }
}

function verifyProducerStatsDump(download, simulcastEnabled) {
  const { dump, filename } = download;
  ensure(dump.schemaVersion === 1, 'Downloaded Producer dump is not schemaVersion 1.');
  ensure(dump.metadata?.role === 'producer', 'Downloaded Producer dump has the wrong role.');
  ensure(dump.metadata?.collectionStatus === 'recording' &&
    dump.metadata?.measurementEndTime === undefined,
  'Active Producer dump does not report the collection lifecycle correctly.');
  ensure(dump.metadata?.simulcastEnabled === simulcastEnabled,
    'Producer dump does not match the requested Simulcast mode.');
  const layers = simulcastEnabled ? ['low', 'mid', 'high'] : ['high'];
  ensure(JSON.stringify(dump.metadata.availableLayers) === JSON.stringify(layers),
    'Producer dump has the wrong active-layer list.');
  ensure(Array.isArray(dump.producerStats) && dump.producerStats.length >= 2,
    'Producer dump did not contain multiple runtime samples.');
  const last = dump.producerStats.at(-1);
  ensure(Number(last?.sourceFramesTotal) > 0 && Number(last?.sourceFps) > 0,
    'Producer dump did not record positive source-frame statistics.');
  const recordedLayers = Object.keys(last?.layers ?? {}).sort();
  ensure(JSON.stringify(recordedLayers) === JSON.stringify([...layers].sort()),
    'Producer dump contains missing or inactive layer samples.');
  const layerTotals = layers.map(layer => Number(last.layers[layer]?.encodedChunksTotal));
  ensure(layerTotals.every(total => total > 0), 'Producer layer sent-chunk counters did not advance.');
  for (const layer of layers) {
    const layerProgressed = dump.producerStats.some(sample =>
      Number(sample.layers?.[layer]?.encodedChunksDelta) > 0
    );
    ensure(layerProgressed,
      `Producer ${layer.toUpperCase()} layer had no encoded-chunk progress in the downloaded samples.`);
  }
  return {
    filename,
    sampleCount: dump.producerStats.length,
    sourceFramesTotal: last.sourceFramesTotal,
    sourceFps: last.sourceFps,
    availableLayers: layers,
    cumulativeChunksByLayer: Object.fromEntries(
      layers.map((layer, index) => [layer, layerTotals[index]])
    )
  };
}

function verifyConsumerStatsDump(download) {
  const { dump, filename } = download;
  ensure(dump.schemaVersion === 1, 'Downloaded Consumer dump is not schemaVersion 1.');
  ensure(dump.metadata?.role === 'consumer', 'Downloaded Consumer dump has the wrong role.');
  ensure(dump.metadata?.collectionStatus === 'recording' &&
    dump.metadata?.measurementEndTime === undefined,
  'Active Consumer dump does not report the collection lifecycle correctly.');
  ensure(Array.isArray(dump.consumerStats) && dump.consumerStats.length >= 2,
    'Consumer dump did not contain multiple runtime samples.');
  const samples = dump.consumerStats;
  ensure(samples.some(sample => Number(sample.packetsReceived) > 0),
    'Consumer dump did not record received packets.');
  ensure(samples.some(sample => Number(sample.bytesReceived) > 0),
    'Consumer dump did not record cumulative received bytes.');
  ensure(samples.some(sample => Number(sample.receivedFps) > 0) &&
      samples.some(sample => Number(sample.decodedFps) > 0),
    'Consumer dump did not record actual Received FPS and Decoded FPS.');
  return {
    filename,
    sampleCount: samples.length,
    lastPacketsReceived: samples.at(-1)?.packetsReceived ?? null,
    lastFramesDecoded: samples.at(-1)?.framesDecoded ?? null,
    lastReceivedFps: samples.at(-1)?.receivedFps ?? null,
    lastDecodedFps: samples.at(-1)?.decodedFps ?? null,
    lastReceivedMediaBitrateKbps: samples.at(-1)?.receivedMediaBitrateKbps ?? null
  };
}

function verifyConsumerRuntimeStats(download, probe, ui) {
  const samples = download.dump.consumerStats;
  ensure(probe?.installed === true, 'Consumer RTCPeerConnection.getStats probe was not installed.');
  ensure(probe.rejectedCalls === 0, 'Consumer 1 unexpectedly rejected getStats during runtime comparison.');
  ensure(probe.calls === probe.resolvedCalls && probe.resolvedCalls === probe.reports.length,
    'Consumer 1 getStats probe did not capture every successful report.');
  ensure(probe.maximumConcurrentCalls === 1,
    'Consumer 1 issued overlapping getStats requests.');
  ensure(probe.calls >= samples.length && probe.calls - samples.length <= 1,
    `Consumer 1 getStats call/sample count diverged: ${probe.calls} calls, ${samples.length} dump samples.`);

  const reports = [...probe.reports].sort((left, right) => left.callNumber - right.callNumber);
  const matched = matchSamplesToRtcReports(samples, reports, 'Consumer 1');
  for (let index = 1; index < matched.length; index += 1) {
    ensure(matched[index].report.callNumber === matched[index - 1].report.callNumber + 1,
      'Consumer 1 has an unmatched successful RTCStatsReport between JSON samples.');
  }

  let comparedCounterFields = 0;
  let comparedRateFields = 0;
  const inboundCounts = [];
  const consumerMediaObjectCounts = [];
  let probatorObjectsObserved = 0;
  const uniqueIds = new Set();
  const uniqueSsrcs = new Set();
  const observedCodecs = new Map();
  const observedTrackIdentifiers = new Set();
  const codecReferences = { present: 0, resolved: 0, unresolved: 0 };
  const timestampSpreadsMs = [];
  let rtxAssociationObserved = false;
  for (let index = 0; index < matched.length; index += 1) {
    const rawCurrent = matched[index].report.inboundVideoStats;
    const rawPrevious = index > 0 ? matched[index - 1].report.inboundVideoStats : null;
    const current = rawCurrent.filter(stat => !isMediasoupRtpProbator(stat));
    const previous = rawPrevious?.filter(stat => !isMediasoupRtpProbator(stat)) ?? null;
    inboundCounts.push(rawCurrent.length);
    consumerMediaObjectCounts.push(current.length);
    probatorObjectsObserved += rawCurrent.filter(isMediasoupRtpProbator).length;
    const reportTimestamps = rawCurrent
      .map(stat => stat.timestamp)
      .filter(value => typeof value === 'number' && Number.isFinite(value));
    if (reportTimestamps.length > 0) {
      timestampSpreadsMs.push(
        Math.max(...reportTimestamps) - Math.min(...reportTimestamps)
      );
    }
    for (const stat of rawCurrent) {
      if (stat.id) uniqueIds.add(stat.id);
      if (typeof stat.ssrc === 'number') uniqueSsrcs.add(stat.ssrc);
      if (typeof stat.trackIdentifier === 'string') {
        observedTrackIdentifiers.add(stat.trackIdentifier);
      }
      if (typeof stat.codecId === 'string') {
        codecReferences.present += 1;
        if (stat.codec?.id === stat.codecId) {
          codecReferences.resolved += 1;
        } else {
          codecReferences.unresolved += 1;
        }
      }
      if (stat.codec?.id) {
        observedCodecs.set(stat.codec.id, stat.codec);
      }
      rtxAssociationObserved ||= stat.rtxSsrc !== null ||
        stat.retransmittedPacketsReceived !== null;
    }
    const expected = calculateExpectedRtcSample(current, previous);
    const actual = samples[matched[index].sampleIndex];
    for (const field of [
      'elapsedMs', 'packetsReceived', 'packetsLost', 'maxJitterMs',
      'framesReceived', 'framesDecoded', 'framesDropped', 'bytesReceived'
    ]) {
      ensureCloseOrNull(actual[field], expected[field], 1e-7,
        `Consumer JSON ${field} differs from its matched RTCStatsReport at sample ${matched[index].sampleIndex}.`);
      comparedCounterFields += 1;
    }
    for (const field of ['receivedFps', 'decodedFps', 'receivedMediaBitrateKbps']) {
      ensureCloseOrNull(actual[field], expected[field], 1e-7,
        `Consumer JSON ${field} differs from matched RTCStatsReport deltas at sample ${matched[index].sampleIndex}.`);
      comparedRateFields += 1;
    }
  }

  ensure(probatorObjectsObserved > 0,
    'Consumer 1 raw RTCStats reports did not expose the mediasoup RTP probator needed to verify media filtering.');
  ensure(consumerMediaObjectCounts.every(count => count === 1),
    'Consumer 1 media aggregation did not select exactly its single observed video RTP object after probator filtering.');
  ensure(codecReferences.unresolved === 0,
    'Consumer 1 had a codecId that could not be resolved to a codec object in the same RTCStatsReport.');

  const uiCandidates = samples.slice(-2);
  ensure(uiCandidates.some(sample => matchesDisplayedRate(ui.receivedFps, sample.receivedFps)) &&
    uiCandidates.some(sample => matchesDisplayedRate(ui.decodedFps, sample.decodedFps)),
  'Consumer displayed FPS does not match either of the most recent JSON samples.');

  return {
    runtimeAudit: {
      status: 'passed',
      source: 'single existing RTCPeerConnection.getStats call observed by an E2E-only wrapper',
      getStatsCallCount: probe.calls,
      resolvedReportCount: probe.resolvedCalls,
      maximumConcurrentCalls: probe.maximumConcurrentCalls,
      jsonSampleCount: samples.length,
      inboundVideoObjectCountByPoll: inboundCounts,
      consumerMediaObjectCountByPoll: consumerMediaObjectCounts,
      mediasoupRtpProbatorObjectsObserved: probatorObjectsObserved,
      distinctInboundStatIdCount: uniqueIds.size,
      distinctInboundStatIds: [...uniqueIds],
      distinctInboundSsrcs: [...uniqueSsrcs],
      observedCodecs: [...observedCodecs.values()],
      observedTrackIdentifiers: [...observedTrackIdentifiers],
      codecReferences,
      withinReportTimestampSpreadMs: timestampSpreadsMs.length > 0 ? {
        maximum: Math.max(...timestampSpreadsMs),
        median: medianNumber(timestampSpreadsMs)
      } : null,
      rtxAssociatedCountersObserved: rtxAssociationObserved,
      rawCounterAndTimestampFieldsCompared: comparedCounterFields,
      derivedRateFieldsCompared: comparedRateFields,
      uiMatchesRecentJson: true
    }
  };
}

function calculateExpectedRtcSample(current, previous) {
  const empty = {
    elapsedMs: null,
    packetsReceived: null,
    packetsLost: null,
    maxJitterMs: null,
    framesReceived: null,
    framesDecoded: null,
    framesDropped: null,
    bytesReceived: null,
    receivedFps: null,
    decodedFps: null,
    receivedMediaBitrateKbps: null
  };
  if (current.length === 0) return empty;
  const ids = current.map(stat => stat.id);
  if (ids.some(id => typeof id !== 'string' || !id) || new Set(ids).size !== ids.length) {
    return empty;
  }

  const result = {
    ...empty,
    packetsReceived: sumRtcField(current, 'packetsReceived'),
    packetsLost: sumRtcField(current, 'packetsLost', true),
    maxJitterMs: maxRtcJitterMs(current),
    framesReceived: sumRtcField(current, 'framesReceived'),
    framesDecoded: sumRtcField(current, 'framesDecoded'),
    framesDropped: sumRtcField(current, 'framesDropped'),
    bytesReceived: sumRtcField(current, 'bytesReceived')
  };
  if (!previous || previous.length !== current.length) return result;

  const previousById = new Map(previous.map(stat => [stat.id, stat]));
  const currentById = new Map(current.map(stat => [stat.id, stat]));
  if ([...currentById.keys()].some(id => !previousById.has(id))) return result;
  const elapsed = [];
  for (const stat of current) {
    const before = previousById.get(stat.id);
    const elapsedMs = stat.timestamp - before.timestamp;
    if (!Number.isFinite(elapsedMs) || elapsedMs <= 0) return result;
    elapsed.push(elapsedMs);
  }
  result.elapsedMs = medianNumber(elapsed);
  result.receivedFps = sumRtcRate(current, previousById, 'framesReceived', 1000);
  result.decodedFps = sumRtcRate(current, previousById, 'framesDecoded', 1000);
  result.receivedMediaBitrateKbps = sumRtcRate(current, previousById, 'bytesReceived', 8);
  return result;
}

function isMediasoupRtpProbator(stat) {
  return stat?.mid === 'probator' || stat?.trackIdentifier === 'probator';
}

function sumRtcField(stats, field, allowNegative = false) {
  if (stats.length === 0) return null;
  let total = 0;
  for (const stat of stats) {
    const value = stat[field];
    if (typeof value !== 'number' || !Number.isFinite(value) || (!allowNegative && value < 0)) {
      return null;
    }
    total += value;
  }
  return Number.isFinite(total) ? total : null;
}

function maxRtcJitterMs(stats) {
  if (stats.length === 0 || stats.some(stat =>
    typeof stat.jitter !== 'number' || !Number.isFinite(stat.jitter) || stat.jitter < 0
  )) return null;
  return Math.max(...stats.map(stat => stat.jitter)) * 1000;
}

function sumRtcRate(current, previousById, field, multiplier) {
  if (current.length === 0) return null;
  let total = 0;
  for (const stat of current) {
    const before = previousById.get(stat.id);
    const value = stat[field];
    const priorValue = before?.[field];
    const elapsedMs = stat.timestamp - before?.timestamp;
    if (
      !before ||
      typeof value !== 'number' || !Number.isFinite(value) || value < 0 ||
      typeof priorValue !== 'number' || !Number.isFinite(priorValue) || priorValue < 0 ||
      value < priorValue || !Number.isFinite(elapsedMs) || elapsedMs <= 0
    ) return null;
    total += (value - priorValue) * multiplier / elapsedMs;
  }
  return Number.isFinite(total) ? total : null;
}

function medianNumber(values) {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
}

function ensureCloseOrNull(actual, expected, tolerance, message) {
  if (expected === null) {
    ensure(actual === null, `${message} Expected null, got ${String(actual)}.`);
    return;
  }
  ensure(typeof actual === 'number' && Number.isFinite(actual) &&
    Math.abs(actual - expected) <= Math.max(tolerance, Math.abs(expected) * tolerance),
  `${message} Expected ${String(expected)}, got ${String(actual)}.`);
}

function matchesDisplayedRate(displayValue, measuredValue) {
  if (measuredValue === null) return displayValue === null;
  return typeof displayValue === 'number' &&
    Math.abs(displayValue - Number(measuredValue.toFixed(1))) <= 0.051;
}

function matchSamplesToRtcReports(samples, reports, label) {
  const unmatched = new Set(reports.map((_, index) => index));
  return samples.map((sample, sampleIndex) => {
    let selectedIndex = -1;
    let selectedDistance = Number.POSITIVE_INFINITY;
    for (const reportIndex of unmatched) {
      const distance = Math.abs(
        Date.parse(sample.timestamp) - Date.parse(reports[reportIndex].completedAt)
      );
      if (distance < selectedDistance) {
        selectedIndex = reportIndex;
        selectedDistance = distance;
      }
    }
    ensure(selectedIndex >= 0 && selectedDistance <= 750,
      `${label} sample ${sampleIndex} could not be matched to its raw RTCStatsReport within 750 ms.`);
    unmatched.delete(selectedIndex);
    return { sampleIndex, report: reports[selectedIndex] };
  });
}

function buildConsumerInboundScenario(probe, context, startTimeMs, endTimeMs) {
  const reports = (probe?.reports ?? [])
    .filter(report => {
      const time = Date.parse(report.completedAt);
      return Number.isFinite(time) && time >= startTimeMs && time <= endTimeMs;
    })
    .sort((left, right) => left.callNumber - right.callNumber);
  const consumeResponse = context?.consumeResponses?.at(-1) ?? null;
  const encodings = consumeResponse?.encodings ?? [];
  const parameterCodecs = consumeResponse?.codecs ?? [];
  const observationsById = new Map();
  const timestampSpreads = [];
  const observedObjectCounts = [];

  for (const report of reports) {
    const stats = report.inboundVideoStats ?? [];
    observedObjectCounts.push(stats.length);
    const timestamps = stats
      .map(stat => stat.timestamp)
      .filter(value => typeof value === 'number' && Number.isFinite(value));
    if (timestamps.length > 0) {
      timestampSpreads.push(Math.max(...timestamps) - Math.min(...timestamps));
    }
    for (const stat of stats) {
      if (typeof stat.id !== 'string' || !stat.id) continue;
      const observations = observationsById.get(stat.id) ?? [];
      observations.push({ callNumber: report.callNumber, completedAt: report.completedAt, stat });
      observationsById.set(stat.id, observations);
    }
  }

  const statTimelines = [...observationsById.entries()].map(([statId, observations]) => {
    const first = observations[0].stat;
    const last = observations.at(-1).stat;
    const deltas = Object.fromEntries([
      'framesReceived',
      'framesDecoded',
      'bytesReceived',
      'packetsReceived',
      'packetsLost',
      'retransmittedPacketsReceived'
    ].map(field => [field, counterDelta(observations, field)]));
    const ssrcs = [...new Set(observations
      .map(item => item.stat.ssrc)
      .filter(value => typeof value === 'number'))];
    const codecObjects = [...new Map(observations
      .map(item => item.stat.codec)
      .filter(codec => codec && typeof codec.id === 'string')
      .map(codec => [codec.id, codec])).values()];
    const trackIdentifiers = [...new Set(observations
      .map(item => item.stat.trackIdentifier)
      .filter(value => typeof value === 'string'))];
    const primarySsrcMatches = ssrcs.filter(ssrc =>
      encodings.some(encoding => encoding.ssrc === ssrc)
    );
    const rtxSsrcMatches = ssrcs.filter(ssrc =>
      encodings.some(encoding => encoding.rtxSsrc === ssrc)
    );
    const rtxSsrcValues = [...new Set(observations
      .map(item => item.stat.rtxSsrc)
      .filter(value => typeof value === 'number'))];
    const rtxFieldMatchesConsumerRtpParameters = rtxSsrcValues.filter(ssrc =>
      encodings.some(encoding => encoding.rtxSsrc === ssrc)
    );
    const codecMatchesConsumerParameters = codecObjects.map(codec => ({
      codecId: codec.id,
      mimeType: codec.mimeType,
      payloadType: codec.payloadType,
      clockRate: codec.clockRate,
      matched: parameterCodecs.some(parameterCodec =>
        typeof codec.mimeType === 'string' &&
        parameterCodec.mimeType?.toLowerCase() === codec.mimeType.toLowerCase() &&
        parameterCodec.payloadType === codec.payloadType &&
        parameterCodec.clockRate === codec.clockRate
      )
    }));
    const trackIdentifierMatchesConsumerTrack = context?.consumerTrackId
      ? trackIdentifiers.includes(context.consumerTrackId)
      : null;
    const mediaCountersAdvanced = [
      deltas.framesReceived.value,
      deltas.framesDecoded.value
    ].some(value => typeof value === 'number' && value > 0);
    const retransmissionCounterAdvanced =
      typeof deltas.retransmittedPacketsReceived.value === 'number' &&
      deltas.retransmittedPacketsReceived.value > 0;
    const networkCountersAdvanced = [
      deltas.packetsReceived.value,
      deltas.bytesReceived.value
    ].some(value => typeof value === 'number' && value > 0);
    const seenInFinalReport = observations.at(-1).callNumber === reports.at(-1)?.callNumber;
    const isProbator = observations.some(item => isMediasoupRtpProbator(item.stat));
    let classification = 'UNKNOWN';
    if (isProbator) {
      classification = 'mediasoup-client RTP probator; not Consumer media and excluded from aggregation';
    } else if (rtxSsrcMatches.length > 0) {
      classification = retransmissionCounterAdvanced
        ? 'Consumer RTP RTX SSRC match; retransmitted-packet counter advanced'
        : 'Consumer RTP RTX SSRC match; actual retransmission not observed';
    } else if (primarySsrcMatches.length > 0) {
      classification = mediaCountersAdvanced
        ? 'Consumer RTP primary SSRC match; video-frame counters advanced'
        : networkCountersAdvanced
          ? 'Consumer RTP primary SSRC match; RTP counters advanced but frame activity was not established'
          : !seenInFinalReport
            ? 'Consumer RTP primary SSRC match; inactive/stale candidate (no counter movement and absent from final report)'
            : 'Consumer RTP primary SSRC match; activity not established in this window';
    } else if (trackIdentifierMatchesConsumerTrack && mediaCountersAdvanced) {
      classification = 'same Consumer track with frame-counter activity; SSRC mapping UNKNOWN';
    }

    return {
      statId,
      type: first.type,
      kind: first.kind,
      firstSeenAt: observations[0].completedAt,
      lastSeenAt: observations.at(-1).completedAt,
      firstCallNumber: observations[0].callNumber,
      lastCallNumber: observations.at(-1).callNumber,
      observedReports: observations.length,
      observationCoverage: reports.length > 0 ? observations.length / reports.length : null,
      seenInFinalReport,
      isMediasoupRtpProbator: isProbator,
      includedInConsumerAggregation: !isProbator,
      ssrcs,
      codecIds: [...new Set(observations
        .map(item => item.stat.codecId)
        .filter(value => typeof value === 'string'))],
      codecs: codecObjects,
      codecMatchesConsumerParameters,
      transportIds: [...new Set(observations
        .map(item => item.stat.transportId)
        .filter(value => typeof value === 'string'))],
      mids: [...new Set(observations
        .map(item => item.stat.mid)
        .filter(value => typeof value === 'string'))],
      trackIdentifiers,
      trackIdentifierMatchesConsumerTrack,
      rtxSsrcValues,
      rtxFieldMatchesConsumerRtpParameters,
      primarySsrcMatchesConsumerRtpParameters: primarySsrcMatches,
      rtxSsrcMatchesConsumerRtpParameters: rtxSsrcMatches,
      deltas,
      counterResetObserved: Object.fromEntries(Object.entries(deltas)
        .map(([field, value]) => [field, value.resetObserved])),
      classification
    };
  });

  const layerRequests = (context?.signalingRequests ?? []).filter(request =>
    Date.parse(request.requestedAt) >= startTimeMs &&
    Date.parse(request.requestedAt) <= endTimeMs
  );
  return {
    status: reports.length >= 2 && Boolean(consumeResponse?.consumerId)
      ? 'captured'
      : 'incomplete',
    window: {
      startedAt: new Date(startTimeMs).toISOString(),
      endedAt: new Date(endTimeMs).toISOString(),
      durationMs: Math.max(0, endTimeMs - startTimeMs)
    },
    consumer: {
      peerId: context?.peerId ?? null,
      consumerId: consumeResponse?.consumerId ?? null,
      kind: consumeResponse?.kind ?? null,
      trackId: context?.consumerTrackId ?? null,
      rtpParameters: consumeResponse
        ? { codecs: parameterCodecs, encodings }
        : null
    },
    getStats: {
      callCount: reports.length,
      inboundVideoObjectCountByPoll: observedObjectCounts,
      droppedProbeReports: probe?.reportsDroppedFromBuffer ?? null,
      layerSelectionRequestsInWindow: layerRequests
    },
    timestampSpreadMs: timestampSpreads.length > 0 ? {
      maximumWithinReport: Math.max(...timestampSpreads),
      medianWithinReport: medianNumber(timestampSpreads)
    } : null,
    statTimelines,
    rawReports: reports.map(report => ({
      callNumber: report.callNumber,
      completedAt: report.completedAt,
      inboundVideoStats: report.inboundVideoStats
    }))
  };
}

function counterDelta(observations, field) {
  const values = observations.map(item => item.stat[field]);
  const numericValues = values.filter(value =>
    typeof value === 'number' && Number.isFinite(value)
  );
  let resetObserved = false;
  for (let index = 1; index < numericValues.length; index += 1) {
    if (numericValues[index] < numericValues[index - 1]) resetObserved = true;
  }
  const allValuesPresent = values.length >= 2 && numericValues.length === values.length;
  if (!allValuesPresent || resetObserved) {
    return {
      value: null,
      first: numericValues[0] ?? null,
      last: numericValues.at(-1) ?? null,
      resetObserved,
      unavailableReason: resetObserved
        ? 'counter-reset-observed'
        : 'counter-missing-or-insufficient-observations'
    };
  }
  return {
    value: numericValues.at(-1) - numericValues[0],
    first: numericValues[0],
    last: numericValues.at(-1),
    resetObserved: false,
    unavailableReason: null
  };
}

function matchesTimestamp(sample, timestamp) {
  return Number.isFinite(Date.parse(sample.timestamp)) &&
    Number.isFinite(Date.parse(timestamp)) &&
    Math.abs(Date.parse(sample.timestamp) - Date.parse(timestamp)) <= 500;
}

async function verifyInjectedConsumerStatsFailure(page) {
  await waitForCondition(async () => {
    const probe = await readConsumerRtcStatsProbe(page);
    return probe?.injectedFailureObserved === true &&
      probe.inFlight === 0 &&
      probe.reports.filter(report => report.callNumber > probe.failureInjectionCall).length >= 2
      ? probe
      : null;
  }, 'Consumer getStats failure and two successful recovery reports', 8_000, 100);

  const download = await downloadStatsDumpFromPage(page, 'consumer-getstats-failure-recovery');
  const probe = await readConsumerRtcStatsProbe(page);
  const samples = download.dump.consumerStats;
  const injectedFailure = probe?.failures.find(failure => failure.injected);
  ensure(injectedFailure && probe.injectedFailureObserved,
    'Consumer 2 did not exercise the E2E-only getStats rejection.');
  ensure(probe.rejectedCalls === 1 && probe.calls === probe.resolvedCalls + 1,
    'Consumer 2 getStats failure injection changed the poll count or failed unexpectedly.');
  ensure(probe.maximumConcurrentCalls === 1,
    'Consumer 2 issued overlapping getStats requests during failure recovery.');

  let failureIndex = -1;
  let nearestFailureDistance = Number.POSITIVE_INFINITY;
  for (let index = 0; index < samples.length; index += 1) {
    const distance = Math.abs(Date.parse(samples[index].timestamp) - Date.parse(injectedFailure.at));
    if (distance < nearestFailureDistance) {
      failureIndex = index;
      nearestFailureDistance = distance;
    }
  }
  ensure(failureIndex >= 0 && nearestFailureDistance <= 500,
    'Rejected getStats did not produce a nearby JSON sample.');
  const failedSample = samples[failureIndex];
  ensure([
    failedSample.elapsedMs,
    failedSample.packetsReceived,
    failedSample.packetsLost,
    failedSample.maxJitterMs,
    failedSample.framesReceived,
    failedSample.framesDecoded,
    failedSample.framesDropped,
    failedSample.bytesReceived,
    failedSample.receivedFps,
    failedSample.decodedFps,
    failedSample.receivedMediaBitrateKbps
  ].every(value => value === null),
  'Rejected getStats was not represented by a fully null Consumer sample.');

  const recoveredReports = probe.reports
    .filter(report => report.callNumber > injectedFailure.callNumber)
    .sort((left, right) => left.callNumber - right.callNumber);
  ensure(recoveredReports.length >= 2, 'Consumer did not recover through two later reports.');
  const firstRecovery = findNearestSample(samples, recoveredReports[0].completedAt);
  const secondRecovery = findNearestSample(samples, recoveredReports[1].completedAt);
  ensure(firstRecovery.index > failureIndex && firstRecovery.distance <= 500,
    'The first successful post-failure report has no matching JSON sample.');
  ensure(firstRecovery.sample.receivedFps === null &&
    firstRecovery.sample.decodedFps === null &&
    firstRecovery.sample.receivedMediaBitrateKbps === null,
  'The first successful post-failure report was not used only as a new baseline.');
  ensure(secondRecovery.index > firstRecovery.index && secondRecovery.distance <= 500 &&
    Number(secondRecovery.sample.receivedFps) > 0 &&
    Number(secondRecovery.sample.decodedFps) > 0 &&
    Number(secondRecovery.sample.receivedMediaBitrateKbps) > 0,
  'Consumer FPS and bitrate did not recover after the new baseline.');

  return {
    status: 'passed',
    injectedCallNumber: injectedFailure.callNumber,
    failureSampleIndex: failureIndex,
    firstRecoverySampleIndex: firstRecovery.index,
    secondRecoverySampleIndex: secondRecovery.index,
    nullFailureSampleRecorded: true,
    firstRecoveryUsedAsBaseline: true,
    nextIntervalRatesRecovered: true,
    maximumConcurrentCalls: probe.maximumConcurrentCalls,
    callCount: probe.calls,
    successfulReports: probe.resolvedCalls,
    rejectionCount: probe.rejectedCalls
  };
}

function findNearestSample(samples, timestamp) {
  let index = -1;
  let distance = Number.POSITIVE_INFINITY;
  for (let current = 0; current < samples.length; current += 1) {
    const nextDistance = Math.abs(Date.parse(samples[current].timestamp) - Date.parse(timestamp));
    if (nextDistance < distance) {
      index = current;
      distance = nextDistance;
    }
  }
  return { index, distance, sample: index >= 0 ? samples[index] : null };
}

async function openPatternPage(label, mode, cycleTrace = null) {
  const { page, audit } = await withCycleCheckpoint(
    cycleTrace,
    'patternPageCreated',
    () => newPage(label, mode)
  );
  await withCycleCheckpoint(cycleTrace, 'patternNavigationCompleted', () =>
    page.goto(runtime.patternUrl, { waitUntil: 'domcontentloaded' })
  );
  await withCycleCheckpoint(cycleTrace, 'patternTitleMatched', () =>
    page.waitForFunction(
      expected => document.title === expected,
      { timeout: PAGE_TIMEOUT_MS },
      PATTERN_TITLE
    )
  );
  const frameCounter = await withCycleCheckpoint(
    cycleTrace,
    'patternFramesAdvancing',
    async () => {
      const before = await page.evaluate(() =>
        Number(document.documentElement.dataset.frame ?? '-1')
      );
      await wait(800);
      const after = await page.evaluate(() =>
        Number(document.documentElement.dataset.frame ?? '-1')
      );
      ensure(after > before, 'The local test-pattern canvas is not advancing frames.');
      return { before, after };
    }
  );
  return { page, audit, frameCounter };
}

async function openAppPage(url, label, mode, cycleTrace = null) {
  const { page, audit } = await withCycleCheckpoint(
    cycleTrace,
    'appPageCreated',
    () => newPage(label, mode)
  );
  try {
    if (/consumer/i.test(label)) {
      await installConsumerRtcStatsProbe(
        page,
        label === 'prod-consumer-2' ? 4 : null
      );
    }
    await withCycleCheckpoint(cycleTrace, 'appNavigationCompleted', () =>
      page.goto(url, { waitUntil: 'domcontentloaded' })
    );
    await withCycleCheckpoint(cycleTrace, 'startButtonFound', async () => {
      await page.waitForSelector('#startButton', { visible: true });
      await page.waitForSelector('#dumpStatsButton');
      await page.waitForSelector('#roleState');
      ensure(
        await page.$eval('#dumpStatsButton', button => button.disabled),
        label + ' Dump Stats button was enabled before the session started.'
      );
    });
    return { page, audit };
  } catch (error) {
    if (!page.isClosed()) {
      await page.close().catch(() => {});
    }
    throw error;
  }
}

async function recordCaptureSourceInventory(cycleTrace, cycleLabel) {
  const pages = await runtime.browser.pages();
  const pageInventory = await Promise.all(pages.map(async page => {
    let title = null;
    let titleError = null;
    try {
      title = await page.title();
    } catch (error) {
      titleError = errorMessage(error);
    }
    return {
      url: page.url(),
      title,
      closed: page.isClosed(),
      titleError
    };
  }));
  const sourcePageCount = pageInventory.filter(page =>
    !page.closed && page.title === PATTERN_TITLE
  ).length;
  cycleTrace.sourceInventory = {
    capturedAt: new Date().toISOString(),
    expectedTitle: PATTERN_TITLE,
    sourcePageCount,
    pages: pageInventory
  };
  ensure(
    sourcePageCount === 1,
    cycleLabel + ': expected exactly one open capture source titled "' +
      PATTERN_TITLE + '", found ' + sourcePageCount
  );
  return cycleTrace.sourceInventory;
}

async function readDisplayCaptureAudit(page) {
  return page.evaluate(() => window.__webrtcMediaAudit?.displayCapture ?? null);
}

async function waitForProducerPageCondition(
  page,
  checkpointName,
  condition,
  deadline,
  cycleLabel,
  failureDescription,
  cycleTrace
) {
  return waitForUiSnapshotCondition(
    page,
    condition,
    failureDescription + ' within the shared ' + PAGE_TIMEOUT_MS + 'ms producer-start deadline',
    {
      deadline,
      cycleTrace,
      checkpointName,
      cycleLabel,
      intervalMs: RUNTIME_POLL_INTERVAL_MS
    }
  );
}

async function clickWithinDeadline(page, selector, deadline, description) {
  const remaining = deadline - Date.now();
  ensure(remaining > 0, description + ' deadline expired before the click.');
  await page.click(selector, { timeout: remaining });
  ensure(Date.now() <= deadline, description + ' exceeded its absolute deadline.');
}

function findH264CodecEvidence(audit) {
  const isH264 = codec => /^avc1\.[0-9a-f]{6}$/i.test(codec);
  const outputCodec = audit.outputEncoderCodecs.find(isH264);
  if (outputCodec) {
    return { codec: outputCodec, source: 'VideoEncoder output decoderConfig metadata' };
  }
  const transmittedCodec = audit.encodedChunkCodecs.find(isH264);
  if (transmittedCodec) {
    return { codec: transmittedCodec, source: 'encodedChunk decoderConfig metadata' };
  }
  if (audit.outputEncoderCodecs.length === 0 &&
      audit.encodedChunkCodecs.length === 0 &&
      audit.encoderOutputFrames > 0) {
    const configuredCodec = audit.configuredEncoderCodecs.find(isH264);
    if (configuredCodec) {
      return {
        codec: configuredCodec,
        source: 'successful VideoEncoder.configure plus emitted frames'
      };
    }
  }
  return null;
}

async function waitForDisplayCaptureResolution(page, deadline, cycleTrace, cycleLabel) {
  const waitStartedAt = Date.now();
  const initialAudit = await readDisplayCaptureAudit(page);
  if (!initialAudit?.interceptionInstalled) {
    throw new Error(cycleLabel + ': getDisplayMedia audit wrapper was not installed.');
  }

  const timeout = deadline - Date.now();
  let audit;
  try {
    if (timeout <= 0) {
      throw new Error('Producer-start deadline expired before getDisplayMedia resolved.');
    }
    audit = await waitForCondition(async () => {
      const observed = await readDisplayCaptureAudit(page);
      const latestRequest = observed?.requests?.at(-1);
      const captureResolved = observed?.calls >= 1 &&
        observed.resolved >= 1 &&
        observed.pending === false &&
        latestRequest?.status === 'resolved';
      return captureResolved || latestRequest?.status === 'rejected'
        ? observed
        : null;
    }, cycleLabel + ' getDisplayMedia to resolve or reject', timeout, RUNTIME_POLL_INTERVAL_MS, deadline);
  } catch (error) {
    audit = await readDisplayCaptureAudit(page).catch(() => initialAudit);
    cycleTrace.getDisplayMediaAudit = audit;
    const latestRequest = audit?.requests?.at(-1);
    const captureResolved = (
      audit?.calls >= 1 &&
      audit.resolved >= 1 &&
      audit.pending === false &&
      latestRequest?.status === 'resolved'
    );
    if (captureResolved) {
      const resolvedBeforeDeadline = Date.parse(latestRequest.resolvedAt) <= deadline;
      recordRuntimeWaitDiagnostic({
        cycleTrace,
        checkpointName: 'getDisplayMediaResolved',
        cycleLabel,
        description: cycleLabel + ': Node polling did not observe getDisplayMedia resolution',
        startedAt: waitStartedAt,
        finalSnapshot: null,
        finalAuditSnapshot: audit,
        conditionSatisfied: true,
        error
      });
      throw new Error(
        cycleLabel + ': Node-side getDisplayMedia polling timed out although the audit ' +
          'shows resolution (classification=harness-polling-false-negative, resolvedBeforeDeadline=' +
          String(resolvedBeforeDeadline) + ')',
        { cause: error }
      );
    }
    if (latestRequest?.status === 'rejected') {
      recordRuntimeWaitDiagnostic({
        cycleTrace,
        checkpointName: 'getDisplayMediaResolved',
        cycleLabel,
        description: cycleLabel + ': getDisplayMedia rejected',
        startedAt: waitStartedAt,
        finalSnapshot: null,
        finalAuditSnapshot: audit,
        conditionSatisfied: false,
        error
      });
      throw new Error(
        cycleLabel + ': getDisplayMedia rejected (' +
          String(latestRequest.errorName ?? 'Error') + ': ' +
          String(latestRequest.errorMessage ?? 'no error message') + ')',
        { cause: error }
      );
    }
    recordRuntimeWaitDiagnostic({
      cycleTrace,
      checkpointName: 'getDisplayMediaResolved',
      cycleLabel,
      description: cycleLabel + ': getDisplayMedia did not resolve within the shared deadline',
      startedAt: waitStartedAt,
      finalSnapshot: null,
      finalAuditSnapshot: audit,
      conditionSatisfied: false,
      error
    });
    throw new Error(
      cycleLabel + ': getDisplayMedia did not resolve within the shared ' +
        PAGE_TIMEOUT_MS + 'ms producer-start deadline (calls=' +
        String(audit?.calls ?? 0) + ', resolved=' + String(audit?.resolved ?? 0) +
        ', pending=' + String(audit?.pending ?? false) + ')',
      { cause: error }
    );
  }

  cycleTrace.getDisplayMediaAudit = audit;
  const latestRequest = audit?.requests?.at(-1);
  if (latestRequest?.status === 'rejected') {
    throw new Error(
      cycleLabel + ': getDisplayMedia rejected (' +
        String(latestRequest.errorName ?? 'Error') + ': ' +
        String(latestRequest.errorMessage ?? 'no error message') + ')'
    );
  }
  if (
    audit.calls < 1 || audit.resolved < 1 || audit.pending !== false ||
    !latestRequest || latestRequest.status !== 'resolved'
  ) {
    throw new Error(cycleLabel + ': getDisplayMedia returned without a resolved capture request.');
  }
  return audit;
}

async function startAs(
  page,
  expectedRole,
  cycleTrace = null,
  cycleLabel = 'Producer',
  pageAudit = null,
  sharedDeadline = null
) {
  if (expectedRole === 'producer' && cycleTrace) {
    const deadline = sharedDeadline ?? Date.now() + PAGE_TIMEOUT_MS;
    await withCycleCheckpoint(cycleTrace, 'startClicked', () =>
      clickWithinDeadline(
        page,
        '#startButton',
        deadline,
        cycleLabel + ': Start click'
      )
    );
    const captureAudit = await withCycleCheckpoint(
      cycleTrace,
      'getDisplayMediaResolved',
      () => waitForDisplayCaptureResolution(page, deadline, cycleTrace, cycleLabel)
    );
    recordCycleCheckpoint(cycleTrace, 'getDisplayMediaResolved', true, {
      calls: captureAudit.calls,
      resolved: captureAudit.resolved,
      rejected: captureAudit.rejected,
      pending: captureAudit.pending
    });
    const roleSnapshot = await withCycleCheckpoint(cycleTrace, 'roleProducerAssigned', () =>
      waitForProducerPageCondition(
        page,
        'roleProducerAssigned',
        snapshot => snapshot.role === 'producer',
        deadline,
        cycleLabel,
        'producer role was not assigned',
        cycleTrace
      )
    );
    recordCycleCheckpoint(cycleTrace, 'roleProducerAssigned', true, {
      role: roleSnapshot.role,
      peerId: roleSnapshot.peerId
    });
    const captureSnapshot = await withCycleCheckpoint(cycleTrace, 'captureTrackLive', () =>
      waitForProducerPageCondition(
        page,
        'captureTrackLive',
        snapshot => snapshot.localVideo.tracks.some(track => track.readyState === 'live'),
        deadline,
        cycleLabel,
        'capture track never became live',
        cycleTrace
      )
    );
    recordCycleCheckpoint(cycleTrace, 'captureTrackLive', true, {
      tracks: captureSnapshot.localVideo.tracks
    });
    const localVideoSnapshot = await withCycleCheckpoint(cycleTrace, 'localVideoPlaying', () =>
      waitForProducerPageCondition(
        page,
        'localVideoPlaying',
        snapshot => snapshot.localVideo.hasStream && snapshot.localVideo.readyState >= 2,
        deadline,
        cycleLabel,
        'local video never reached current data',
        cycleTrace
      )
    );
    recordCycleCheckpoint(cycleTrace, 'localVideoPlaying', true, {
      localVideo: localVideoSnapshot.localVideo
    });
    const encodedSnapshot = await withCycleCheckpoint(cycleTrace, 'encodedChunkStarted', () =>
      waitForProducerPageCondition(
        page,
        'encodedChunkStarted',
        snapshot => Number(snapshot.encodedChunks) > 0,
        deadline,
        cycleLabel,
        'encoded chunk output never started',
        cycleTrace
      )
    );
    let encoderAuditError = null;
    if (pageAudit) {
      try {
        await syncWebSocketAudit(page, pageAudit);
      } catch (error) {
        encoderAuditError = errorMessage(error);
      }
    }
    const encoderOutputFrames = pageAudit?.encoderOutputFrames ?? null;
    cycleTrace.encoderOutputFrames = encoderOutputFrames;
    recordCycleCheckpoint(cycleTrace, 'encodedChunkStarted', true, {
      encodedChunks: encodedSnapshot.encodedChunks,
      encoderOutputFrames,
      encoderAuditError
    });
    const statsSnapshot = await readUiSnapshot(page);
    ensure(statsSnapshot.measurementStatus?.startsWith('Recording') &&
      statsSnapshot.dumpStatsDisabled === false,
    cycleLabel + ': Producer stats recording did not start with the media session.');
    return;
  }

  const deadline = sharedDeadline ?? Date.now() + PAGE_TIMEOUT_MS;
  await clickWithinDeadline(
    page,
    '#startButton',
    deadline,
    cycleLabel + ': Start click'
  );
  await waitForUiSnapshotCondition(
    page,
    snapshot => snapshot.role === expectedRole,
    expectedRole + ' role assignment within the shared ' + PAGE_TIMEOUT_MS + 'ms start deadline',
    { deadline, cycleLabel }
  );
  if (expectedRole === 'producer') {
    await waitForUiSnapshotCondition(
      page,
      snapshot => snapshot.localVideo.tracks.some(track => track.readyState === 'live') &&
        snapshot.localVideo.readyState >= 2 && snapshot.encodedChunks > 0,
      'Producer capture track, local video, and encoded chunks within the shared ' +
        PAGE_TIMEOUT_MS + 'ms start deadline',
      { deadline, cycleLabel }
    );
  } else {
    await waitForUiSnapshotCondition(
      page,
      snapshot => snapshot.consumerState === 'receiving' &&
        snapshot.remoteVideo.hasStream &&
        snapshot.remoteVideo.readyState >= 2 &&
        snapshot.remoteVideo.width > 0 &&
        snapshot.remoteVideo.height > 0 &&
        !snapshot.remoteVideo.paused,
      'Consumer receiving video playback within the shared ' +
        PAGE_TIMEOUT_MS + 'ms start deadline',
      { deadline, cycleLabel }
    );
  }
  const statsSnapshot = await readUiSnapshot(page);
  ensure(statsSnapshot.measurementStatus?.startsWith('Recording') &&
    statsSnapshot.dumpStatsDisabled === false,
  cycleLabel + ': Stats recording was not available after session start.');
}

async function readUiSnapshot(page) {
  return page.evaluate(() => {
    const text = selector =>
      document.querySelector(selector)?.textContent?.trim() ?? null;
    const number = selector => {
      const value = Number(text(selector));
      return Number.isFinite(value) ? value : null;
    };
    const localVideo = document.querySelector('#localVideo');
    const remoteVideo = document.querySelector('#remoteVideo');
    const localTracks = localVideo?.srcObject?.getVideoTracks?.() ?? [];
    const remoteTracks = remoteVideo?.srcObject?.getVideoTracks?.() ?? [];
    const log = document.querySelector('#log');
    const playback = remoteVideo?.getVideoPlaybackQuality?.();
    return {
      socketState: text('#socketState'),
      role: text('#roleState'),
      peerId: text('#peerId'),
      consumerState: text('#consumerState'),
      consumerCount: number('#consumerCount'),
      encodedChunks: number('#chunkCount'),
      packetCount: number('#packetCount'),
      rtpReceived: number('#rtpReceived'),
      framesDecoded: number('#framesDecoded'),
      packetCount: number('#packetCount'),
      inputFps: {
        source: number('#sfuInputFps'),
        low: number('#lowInputFps'),
        mid: number('#midInputFps'),
        high: number('#highInputFps')
      },
      receivedFps: number('#receivedFps'),
      decodedFps: number('#decodedFps'),
      measurementStatus: text('#measurementStatus'),
      dumpStatsDisabled: document.querySelector('#dumpStatsButton')?.disabled ?? true,
      simulcastState: text('#simulcastState'),
      producerSettingsHidden: document.querySelector('#producerSettings')?.hidden ?? true,
      simulcastToggleChecked: document.querySelector('#simulcastToggle')?.checked ?? null,
      simulcastToggleDisabled: document.querySelector('#simulcastToggle')?.disabled ?? null,
      consumerLayerControlsHidden: document.querySelector('#consumerLayerControls')?.hidden ?? true,
      consumerLayerStatus: document.querySelector('#consumerLayerStatus')?.value ??
        text('#consumerLayerStatus'),
      consumerLayerButtons: Object.fromEntries(
        ['low', 'mid', 'high'].map(layer => {
          const button = document.querySelector(`#consumerLayer${layer[0].toUpperCase()}${layer.slice(1)}`);
          return [layer, {
            disabled: button?.disabled ?? true,
            pressed: button?.getAttribute('aria-pressed') === 'true'
          }];
        })
      ),
      localVideo: {
        hasStream: Boolean(localVideo?.srcObject),
        readyState: localVideo?.readyState ?? null,
        width: localVideo?.videoWidth ?? 0,
        height: localVideo?.videoHeight ?? 0,
        tracks: localTracks.map(track => ({
          readyState: track.readyState,
          kind: track.kind,
          label: track.label
        }))
      },
      remoteVideo: {
        hasStream: Boolean(remoteVideo?.srcObject),
        readyState: remoteVideo?.readyState ?? null,
        width: remoteVideo?.videoWidth ?? 0,
        height: remoteVideo?.videoHeight ?? 0,
        paused: remoteVideo?.paused ?? true,
        currentTime: remoteVideo?.currentTime ?? 0,
        totalVideoFrames: playback?.totalVideoFrames ?? null,
        tracks: remoteTracks.map(track => ({
          readyState: track.readyState,
          kind: track.kind,
          muted: track.muted
        }))
      },
      documentVisibilityState: document.visibilityState,
      documentHasFocus: document.hasFocus(),
      renderObserver: window.__sfuSoakRenderObserver
        ? structuredClone(window.__sfuSoakRenderObserver)
        : null,
      logHidden: log?.hidden ?? true,
      logText: log?.textContent ?? ''
    };
  });
}

function recordRuntimeWaitDiagnostic({
  cycleTrace = null,
  checkpointName = null,
  cycleLabel = null,
  description,
  startedAt,
  finalSnapshot,
  finalHealthSnapshot = null,
  finalAuditSnapshot = null,
  conditionSatisfied,
  error
}) {
  const diagnostic = {
    cycleLabel,
    checkpoint: checkpointName,
    description,
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: new Date().toISOString(),
    falseNegativeSuspected: conditionSatisfied,
    classification: conditionSatisfied
      ? 'harness-polling-false-negative'
      : 'runtime-condition-not-observed',
    lastUiSnapshot: finalSnapshot ?? null,
    lastHealthSnapshot: finalHealthSnapshot,
    lastAuditSnapshot: finalAuditSnapshot,
    error: errorMessage(error)
  };
  report.runtimeWaitDiagnostics.push(diagnostic);
  if (cycleTrace) {
    cycleTrace.runtimeWaitDiagnostics.push(diagnostic);
    cycleTrace.lastUiSnapshot = finalSnapshot ?? cycleTrace.lastUiSnapshot;
    cycleTrace.lastHealthSnapshot = finalHealthSnapshot ?? cycleTrace.lastHealthSnapshot;
    cycleTrace.classification = diagnostic.classification;
    if (conditionSatisfied) {
      cycleTrace.falseNegativeSuspected = true;
    }
    const checkpoint = cycleTrace.checkpoints?.[checkpointName];
    if (checkpoint) {
      checkpoint.falseNegativeSuspected = conditionSatisfied;
      checkpoint.classification = diagnostic.classification;
      checkpoint.lastUiSnapshot = finalSnapshot ?? null;
      checkpoint.lastHealthSnapshot = finalHealthSnapshot;
      checkpoint.lastAuditSnapshot = finalAuditSnapshot;
    }
  }
  return diagnostic;
}

async function waitForUiSnapshotCondition(
  page,
  condition,
  description,
  {
    timeoutMs = PAGE_TIMEOUT_MS,
    intervalMs = RUNTIME_POLL_INTERVAL_MS,
    deadline = null,
    cycleTrace = null,
    checkpointName = null,
    cycleLabel = null
  } = {}
) {
  const startedAt = Date.now();
  const absoluteDeadline = deadline ?? startedAt + timeoutMs;
  const remaining = Math.max(0, absoluteDeadline - Date.now());
  let lastSnapshot = null;
  const readSnapshot = async () => {
    lastSnapshot = await readUiSnapshot(page);
    if (cycleTrace) {
      cycleTrace.lastUiSnapshot = lastSnapshot;
    }
    return lastSnapshot;
  };

  try {
    return await waitForCondition(async () => {
      const snapshot = await readSnapshot();
      return condition(snapshot) ? snapshot : null;
    }, description, remaining, intervalMs, absoluteDeadline);
  } catch (error) {
    const finalSnapshot = await readSnapshot().catch(() => lastSnapshot);
    const conditionSatisfied = Boolean(
      finalSnapshot && condition(finalSnapshot)
    );
    const diagnostic = recordRuntimeWaitDiagnostic({
      cycleTrace,
      checkpointName,
      cycleLabel,
      description,
      startedAt,
      finalSnapshot,
      conditionSatisfied,
      error
    });
    throw new Error(
      (cycleLabel ? cycleLabel + ': ' : '') + description +
        ' (classification=' + diagnostic.classification + ')',
      { cause: error }
    );
  }
}

async function captureVideoHash(page) {
  const rect = await page.$eval('#remoteVideo', video => {
    const bounds = video.getBoundingClientRect();
    return {
      x: Math.floor(bounds.x),
      y: Math.floor(bounds.y),
      width: Math.ceil(bounds.width),
      height: Math.ceil(bounds.height)
    };
  });
  ensure(rect.width > 0 && rect.height > 0, 'Remote video element has no visible area.');
  const image = await page.screenshot({
    type: 'png',
    clip: rect,
    captureBeyondViewport: false
  });
  return {
    sha256: createHash('sha256').update(image).digest('hex'),
    width: rect.width,
    height: rect.height
  };
}

async function waitForConsumerReady(page, description) {
  return waitForUiSnapshotCondition(
    page,
    snapshot => (
      snapshot.role === 'consumer' &&
      snapshot.consumerState === 'receiving' &&
      snapshot.remoteVideo.hasStream &&
      snapshot.remoteVideo.readyState >= 2 &&
      snapshot.remoteVideo.width > 0 &&
      snapshot.remoteVideo.height > 0 &&
      !snapshot.remoteVideo.paused &&
      snapshot.rtpReceived > 0 &&
      snapshot.framesDecoded > 0 &&
      snapshot.receivedFps > 0 &&
      snapshot.decodedFps > 0
    ),
    description,
    { timeoutMs: PAGE_TIMEOUT_MS, intervalMs: RUNTIME_POLL_INTERVAL_MS }
  );
}

async function measureConsumer(page, label, durationMs = MEDIA_SAMPLE_MS) {
  const first = await waitForConsumerReady(page, label + ' media and FPS');
  const frameHashes = [await captureVideoHash(page)];
  const interval = Math.floor(durationMs / 2);
  await wait(interval);
  const middle = await readUiSnapshot(page);
  frameHashes.push(await captureVideoHash(page));
  await wait(durationMs - interval);
  const last = await readUiSnapshot(page);
  frameHashes.push(await captureVideoHash(page));

  ensure(last.role === 'consumer', label + ' lost Consumer role during sampling.');
  ensure(last.consumerState === 'receiving', label + ' stopped receiving.');
  ensure(
    last.rtpReceived > first.rtpReceived,
    label + ' RTP received count did not increase.'
  );
  ensure(
    last.framesDecoded > first.framesDecoded,
    label + ' decoded frame count did not increase.'
  );
  ensure(last.receivedFps > 0, label + ' Received FPS is not positive.');
  ensure(last.decodedFps > 0, label + ' Decoded FPS is not positive.');
  ensure(
    new Set(frameHashes.map(frame => frame.sha256)).size >= 2,
    label + ' remote video pixels did not change between screenshots.'
  );

  return {
    status: 'passed',
    durationMs,
    initial: first,
    middle,
    final: last,
    deltas: {
      rtpReceived: last.rtpReceived - first.rtpReceived,
      framesDecoded: last.framesDecoded - first.framesDecoded
    },
    movingFrameVerification: {
      passed: true,
      hashes: frameHashes
    }
  };
}

async function waitWithinDeadline(durationMs, deadline, description) {
  const remaining = deadline - Date.now();
  if (remaining < durationMs) {
    throw new Error(
      description + ' needs ' + durationMs + 'ms but only ' +
        Math.max(0, remaining) + 'ms remain in the shared ' + PAGE_TIMEOUT_MS + 'ms deadline.'
    );
  }
  await wait(durationMs);
  if (Date.now() > deadline) {
    throw new Error(description + ' exceeded the shared ' + PAGE_TIMEOUT_MS + 'ms deadline.');
  }
}

async function assertProducerProgress(
  page,
  serverRecord,
  cycleTrace = null,
  cycleLabel = 'Producer',
  producerDeadline = Date.now() + PAGE_TIMEOUT_MS,
  expectedSimulcastEnabled = true
) {
  const progressCheckpointNames = [
    'healthProducerRegistered',
    'injectedChunkStarted',
    'injectedPacketStarted',
    'inputFpsStarted'
  ];
  for (const name of progressCheckpointNames) {
    beginCycleCheckpoint(cycleTrace, name);
  }

  const isProducerProgressReady = (ui, health) => {
    if (!ui || !health) {
      return false;
    }
    const fps = health.inputFps ?? {};
    const expectedLayers = expectedSimulcastEnabled
      ? ['low', 'mid', 'high']
      : ['high'];
    const expectedFps = expectedSimulcastEnabled
      ? Number(fps.low) > 0 && Number(fps.mid) > 0 && Number(fps.high) > 0
      : Number(fps.low) === 0 && Number(fps.mid) === 0 && Number(fps.high) > 0;
    return (
      ui.role === 'producer' &&
      ui.localVideo.hasStream &&
      ui.localVideo.tracks.some(track => track.readyState === 'live') &&
      ui.encodedChunks > 0 &&
      Boolean(health.producerPeerId) &&
      Boolean(health.producerId) &&
      health.injectedChunks > 0 &&
      health.injectedPackets > 0 &&
      Number(fps.source) > 0 &&
      expectedFps &&
      health.simulcastEnabled === expectedSimulcastEnabled &&
      JSON.stringify(health.activeLayers) === JSON.stringify(expectedLayers) &&
      health.producerType === (expectedSimulcastEnabled ? 'simulcast' : 'simple')
    );
  };

  let lastUiSnapshot = null;
  let lastHealthSnapshot = null;
  const progressWaitStartedAt = Date.now();
  let ready;
  try {
    ready = await withCycleCheckpoint(
      cycleTrace,
      'producerProgressConfirmed',
      async () => {
        try {
          return await waitForCondition(async () => {
            const [ui, health] = await Promise.all([
              readUiSnapshot(page),
              fetchJson(HEALTH_URL)
            ]);
            if (cycleTrace) {
              cycleTrace.lastUiSnapshot = ui;
              cycleTrace.lastHealthSnapshot = health;
            }
            lastUiSnapshot = ui;
            lastHealthSnapshot = health;
            const fps = health.inputFps ?? {};
            recordCycleCheckpoint(
              cycleTrace,
              'healthProducerRegistered',
              Boolean(health.producerPeerId && health.producerId),
              { producerPeerId: health.producerPeerId ?? null, producerId: health.producerId ?? null }
            );
            recordCycleCheckpoint(
              cycleTrace,
              'injectedChunkStarted',
              health.injectedChunks > 0,
              { injectedChunks: health.injectedChunks }
            );
            recordCycleCheckpoint(
              cycleTrace,
              'injectedPacketStarted',
              health.injectedPackets > 0,
              { injectedPackets: health.injectedPackets }
            );
            recordCycleCheckpoint(
              cycleTrace,
              'inputFpsStarted',
              Number(fps.source) > 0 &&
                (expectedSimulcastEnabled
                  ? Number(fps.low) > 0 && Number(fps.mid) > 0 && Number(fps.high) > 0
                  : Number(fps.low) === 0 && Number(fps.mid) === 0 && Number(fps.high) > 0),
              { inputFps: fps }
            );
            return isProducerProgressReady(ui, health)
              ? { ui, health }
              : null;
          },
          cycleLabel + ': Producer capture, injected RTP, and input FPS',
          Math.max(0, producerDeadline - Date.now()),
          RUNTIME_POLL_INTERVAL_MS,
          producerDeadline);
        } catch (error) {
          const [uiResult, healthResult] = await Promise.allSettled([
            readUiSnapshot(page),
            fetchJson(HEALTH_URL)
          ]);
          if (uiResult.status === 'fulfilled') {
            lastUiSnapshot = uiResult.value;
          }
          if (healthResult.status === 'fulfilled') {
            lastHealthSnapshot = healthResult.value;
          }
          if (cycleTrace) {
            cycleTrace.lastUiSnapshot = lastUiSnapshot ?? cycleTrace.lastUiSnapshot;
            cycleTrace.lastHealthSnapshot = lastHealthSnapshot ?? cycleTrace.lastHealthSnapshot;
          }
          const conditionSatisfied = isProducerProgressReady(
            lastUiSnapshot,
            lastHealthSnapshot
          );
          const diagnostic = recordRuntimeWaitDiagnostic({
            cycleTrace,
            checkpointName: 'producerProgressConfirmed',
            cycleLabel,
            description: cycleLabel + ': Producer capture, injected RTP, and input FPS',
            startedAt: progressWaitStartedAt,
            finalSnapshot: lastUiSnapshot,
            finalHealthSnapshot: lastHealthSnapshot,
            conditionSatisfied,
            error
          });
          throw new Error(
            cycleLabel + ': producer progress was not confirmed within the shared ' +
              PAGE_TIMEOUT_MS + 'ms producer-start deadline (classification=' +
              diagnostic.classification + '; last UI and health snapshots are attached: ' +
              errorMessage(error) + ')',
            { cause: error }
          );
        }
      }
    );
  } catch (error) {
    for (const name of progressCheckpointNames) {
      if (cycleTrace?.checkpoints?.[name]?.status === 'running') {
        finishCycleCheckpoint(
          cycleTrace,
          name,
          'failed',
          new Error(cycleLabel + ': corresponding Producer health signal was not observed')
        );
      }
    }
    throw error;
  }

  cycleTrace.lastUiSnapshot = ready.ui;
  cycleTrace.lastHealthSnapshot = ready.health;
  cycleTrace.progressSnapshots = { ready };

  await waitWithinDeadline(2200, producerDeadline, cycleLabel + ' first Producer stability sample');
  const first = {
    ui: await readUiSnapshot(page),
    health: await fetchJson(HEALTH_URL)
  };
  cycleTrace.lastUiSnapshot = first.ui;
  cycleTrace.lastHealthSnapshot = first.health;
  cycleTrace.progressSnapshots.first = first;
  await waitWithinDeadline(2200, producerDeadline, cycleLabel + ' second Producer stability sample');
  const second = {
    ui: await readUiSnapshot(page),
    health: await fetchJson(HEALTH_URL)
  };
  cycleTrace.lastUiSnapshot = second.ui;
  cycleTrace.lastHealthSnapshot = second.health;
  cycleTrace.progressSnapshots.second = second;
  ensure(second.ui.encodedChunks > first.ui.encodedChunks, 'Producer encoded chunk counter did not increase.');
  ensure(
    second.health.injectedChunks > first.health.injectedChunks,
    'SFU injected chunk counter did not increase.'
  );
  ensure(
    second.health.injectedPackets > first.health.injectedPackets,
    'SFU injected RTP packet counter did not increase.'
  );
  ensure(
    second.health.producerPeerId === ready.health.producerPeerId &&
      second.health.producerId === ready.health.producerId,
    'Producer identity changed while capture was active.'
  );
  ensure(!serverRecord.exit, 'Production/debug server exited while Producer was active.');

  return {
    status: 'passed',
    role: ready.ui.role,
    peerId: ready.ui.peerId,
    captureTrackState: ready.ui.localVideo.tracks[0]?.readyState ?? null,
    first: { ui: first.ui, health: first.health },
    second: { ui: second.ui, health: second.health },
    deltas: {
      encodedChunks: second.ui.encodedChunks - first.ui.encodedChunks,
      injectedChunks: second.health.injectedChunks - first.health.injectedChunks,
      injectedPackets: second.health.injectedPackets - first.health.injectedPackets
    },
    inputFps: second.health.inputFps,
    simulcastEnabled: second.health.simulcastEnabled,
    activeLayers: second.health.activeLayers,
    producerType: second.health.producerType
  };
}

async function assertConsumerCount(expected) {
  const health = await waitForHealth(
    currentServerRecord,
    value => value.consumers === expected,
    'health consumer count ' + expected,
    12_000
  );
  return health;
}

async function waitForEmptyRoom(record, description) {
  return waitForHealth(
    record,
    health =>
      !health.producerPeerId && !health.producerId && health.consumers === 0,
    description,
    12_000
  );
}

async function runCleanupSeries(options) {
  const before = await fetchJson(HEALTH_URL);
  if (options.beforeCloseAction) {
    await options.beforeCloseAction(before);
  }
  const samples = [{ offsetMs: 0, health: before }];
  options.onSample?.(samples[0]);
  const closeActionExecuted = true;
  await options.closeAction();
  let elapsed = 0;
  for (const offsetMs of [2000, 5000, 10000]) {
    await wait(offsetMs - elapsed);
    elapsed = offsetMs;
    const sample = { offsetMs, health: await fetchJson(HEALTH_URL) };
    samples.push(sample);
    options.onSample?.(sample);
  }

  const t2 = samples[1].health;
  const t5 = samples[2].health;
  const t10 = samples[3].health;
  const identityRemovedByT2 = !t2.producerPeerId && !t2.producerId;
  const consumerCountSettledByT2 =
    options.expectedConsumerCountAfterClose === undefined ||
    t2.consumers === options.expectedConsumerCountAfterClose;
  const countersStoppedFromT2 = t5.injectedChunks === t2.injectedChunks &&
    t10.injectedChunks === t5.injectedChunks &&
    t5.injectedPackets === t2.injectedPackets &&
    t10.injectedPackets === t5.injectedPackets;
  const consumerClosedSnapshot = options.consumerPage
    ? await waitForUiSnapshotCondition(
        options.consumerPage,
        state => state.consumerState === 'producer closed',
        'Consumer producerClosed state',
        { timeoutMs: 5000, intervalMs: RUNTIME_POLL_INTERVAL_MS }
      ).catch(() => null)
    : null;
  const consumerClosed = consumerClosedSnapshot?.consumerState ?? null;
  const trackEndedSnapshot = options.producerPage
    ? await waitForUiSnapshotCondition(
        options.producerPage,
        state => state.localVideo.tracks.some(track => track.readyState === 'ended'),
        'Producer capture track ended',
        { timeoutMs: 5000, intervalMs: RUNTIME_POLL_INTERVAL_MS }
      ).catch(() => null)
    : null;
  const trackEnded = options.producerPage
    ? Boolean(
        trackEndedSnapshot?.localVideo.tracks.some(track => track.readyState === 'ended')
      )
    : null;

  return {
    status: identityRemovedByT2 && countersStoppedFromT2 &&
      (!options.consumerPage || consumerClosed === 'producer closed') &&
      (options.requireTrackEnded !== true || trackEnded === true) &&
      consumerCountSettledByT2
      ? 'passed'
      : 'failed',
    closeAction: options.closeActionName,
    closeActionExecuted,
    samples,
    identityRemovedByT2,
    expectedConsumerCountAfterClose:
      options.expectedConsumerCountAfterClose ?? null,
    observedConsumerCountAtT2: t2.consumers,
    consumerCountSettledByT2,
    chunksStableFromT2: t2.injectedChunks === t5.injectedChunks &&
      t5.injectedChunks === t10.injectedChunks,
    packetsStableFromT2: t2.injectedPackets === t5.injectedPackets &&
      t5.injectedPackets === t10.injectedPackets,
    countersStoppedFromT2,
    consumerClosed,
    captureTrackEnded: trackEnded,
    serverAlive: !currentServerRecord.exit
  };
}

function isInsideDebugGuard(source, marker) {
  const markerIndex = source.indexOf(marker);
  if (markerIndex < 0) {
    return false;
  }
  const guardPattern = /if\s*\(\s*IS_DEBUG\s*\)\s*\{/g;
  let match;
  while ((match = guardPattern.exec(source)) !== null && match.index < markerIndex) {
    const openBrace = source.indexOf('{', match.index);
    let depth = 1;
    for (let index = openBrace + 1; index < source.length; index += 1) {
      if (source[index] === '{') {
        depth += 1;
      } else if (source[index] === '}') {
        depth -= 1;
        if (depth === 0) {
          if (markerIndex < index) {
            return true;
          }
          break;
        }
      }
    }
  }
  return false;
}

async function auditSourceGuards() {
  const serverSource = await readFile(path.join(ROOT, 'src', 'server.ts'), 'utf8');
  const clientSource = await readFile(path.join(ROOT, 'src', 'client.ts'), 'utf8');
  const consumerStatsSource = await readFile(path.join(ROOT, 'src', 'stats', 'consumerStats.ts'), 'utf8');
  const statsDumpSource = await readFile(path.join(ROOT, 'src', 'stats', 'statsDump.ts'), 'utf8');
  const visualizerSource = await readFile(path.join(ROOT, 'src', 'stats', 'stats-visualizer.ts'), 'utf8');
  const chartDataSource = await readFile(path.join(ROOT, 'src', 'stats', 'statsChartData.ts'), 'utf8');
  const chartJsSource = await readFile(path.join(ROOT, 'src', 'stats', 'statsChartJs.ts'), 'utf8');
  const viteConfigSource = await readFile(path.join(ROOT, 'vite.config.ts'), 'utf8');
  const packageJson = JSON.parse(await readFile(path.join(ROOT, 'package.json'), 'utf8'));
  const gitignoreSource = await readFile(path.join(ROOT, '.gitignore'), 'utf8');
  const pipelineSource = await readFile(
    path.join(ROOT, 'docs', 'custom_code', 'webCodecsPipeline.js'),
    'utf8'
  );
  const encodeFrameStart = clientSource.indexOf('async function encodeFrame(frame: VideoFrame)');
  const encodeFrameEnd = clientSource.indexOf('function createEncoderConfig(', encodeFrameStart);
  const sourceFrameIncrementPositions = [...clientSource.matchAll(/\bsourceFramesTotal\s*\+\+\s*;/g)]
    .map(match => match.index);
  const sourceFrameIncrementPosition = sourceFrameIncrementPositions[0] ?? -1;
  const layerFanoutPosition = clientSource.indexOf(
    'for (const layer of layerStates) {',
    sourceFrameIncrementPosition
  );
  const producerTraceSetup = serverSource.indexOf("enableTraceEvent(['pli', 'fir'])");
  const pendingEncodeSuccessGuard =
    /try\s*\{\s*layer\.encoder\.encode\(processedFrame,\s*\{\s*keyFrame\s*\}\);\s*\}\s*catch\s*\(error\)\s*\{\s*if\s*\(requestedKeyframe\)\s*\{\s*layer\.requestedKeyframesByTimestamp\.delete\(processedFrame\.timestamp\);\s*const previousFailures\s*=\s*requestedKeyframeEncodeFailures\.get\(layer\.id\)\s*\?\?\s*0;\s*if\s*\(previousFailures\s*===\s*0\)\s*\{\s*requestedKeyframeEncodeFailures\.set\(layer\.id,\s*1\);\s*throw new RequestedKeyframeEncodeRetryError\(layer\.id,\s*error\);\s*\}\s*requestedKeyframeEncodeFailures\.delete\(layer\.id\);\s*\}\s*throw error;\s*\}\s*if\s*\(requestedKeyframe\)\s*\{\s*requestedKeyframeEncodeFailures\.delete\(layer\.id\);\s*if\s*\(pendingKeyframeRequests\.get\(layer\.id\)\s*===\s*requestedKeyframe\)\s*\{\s*pendingKeyframeRequests\.delete\(layer\.id\);\s*\}\s*\}/.test(clientSource);
  const guards = {
    traceMonitoringDebugGuard: isInsideDebugGuard(
      serverSource,
      "enableTraceEvent(['rtp', 'keyframe', 'pli', 'fir'])"
    ),
    producerFeedbackTraceProductionEnabled:
      producerTraceSetup >= 0 && !isInsideDebugGuard(serverSource, "enableTraceEvent(['pli', 'fir'])"),
    producerFeedbackLoggingDebugGuard: isInsideDebugGuard(
      serverSource,
      '[simulcast] forwarded ${trace.type.toUpperCase()}'
    ),
    clientKeyframeLoggingDebugGuard: isInsideDebugGuard(
      clientSource,
      'forcing requested keyframe'
    ),
    e2eKeyframeControlEnvironmentGated:
      /SFU_E2E_MEDIA_ENABLED\s*=\s*process\.env\.SFU_E2E_MEDIA\s*===\s*['"]1['"]/.test(serverSource) &&
      /if\s*\(!SFU_E2E_MEDIA_ENABLED\)/.test(serverSource),
    e2eLayerSelectionControlEnvironmentGated:
      serverSource.includes("action === 'e2eSelectConsumerLayer'") &&
      /e2eSelectConsumerLayer[\s\S]{0,200}if\s*\(!SFU_E2E_MEDIA_ENABLED\)/.test(serverSource),
    e2eConsumerAuditEnvironmentGated:
      /SFU_E2E_MEDIA_ENABLED\s*\?\s*\{\s*e2eConsumerAudit:\s*snapshotE2EConsumerAudit\(\)\s*\}/.test(serverSource),
    e2eStallDiagnosticsEnvironmentGated:
      /action === 'e2eCaptureMediaDiagnostics'[\s\S]{0,200}if\s*\(!SFU_E2E_MEDIA_ENABLED\)/.test(serverSource) &&
      /function captureE2EMediaDiagnostics\([\s\S]{0,500}producerStatsPromise/.test(serverSource),
    fixedInitialAvailableOutgoingBitrate:
      /initialAvailableOutgoingBitrate:\s*1_000_000/.test(serverSource) &&
      !/SFU_E2E_INITIAL_AVAILABLE_OUTGOING_BITRATE/.test(serverSource),
    directProducerPliFirFeedbackOnly:
      /\{\s*type:\s*['"]nack['"],\s*parameter:\s*['"]pli['"]\s*\}/.test(pipelineSource) &&
      /\{\s*type:\s*['"]ccm['"],\s*parameter:\s*['"]fir['"]\s*\}/.test(pipelineSource) &&
      !/\{\s*type:\s*['"]nack['"]\s*\}/.test(pipelineSource) &&
      !/\{\s*type:\s*['"]nack['"]\s*,\s*parameter:\s*['"]['"]\s*\}/.test(pipelineSource),
    clientLayeredKeyframeRequestHandling:
      clientSource.includes("message.event === 'keyframeRequest'") &&
      /periodicKeyframe\s*\|\|\s*resizeKeyframe\s*\|\|\s*requestedKeyframe/.test(clientSource) &&
      clientSource.includes('requestedKeyframeAudit'),
    onePendingKeyframeObligationPerLayer:
      /const pendingKeyframeRequests\s*=\s*new Map<SimulcastLayer, PendingKeyframeRequest>/.test(clientSource) &&
      /const requestedKeyframe\s*=\s*pendingKeyframeRequests\.get\(layer\.id\)/.test(clientSource),
    boundedPendingKeyframeAuditRequests:
      /MAX_PENDING_KEYFRAME_AUDIT_REQUESTS\s*=\s*16/.test(clientSource) &&
      /slice\(\s*-MAX_PENDING_KEYFRAME_AUDIT_REQUESTS\s*\)/.test(clientSource),
    pendingRequestConsumedAfterSuccessfulEncode: pendingEncodeSuccessGuard,
    requestedKeyframeEncodeFailureRetriesNextFrame:
      clientSource.includes('instanceof RequestedKeyframeEncodeRetryError') &&
      clientSource.includes('throw new RequestedKeyframeEncodeRetryError') &&
      /logError\(\s*error\.message\s*\);\s*continue\s*;/.test(clientSource),
    requestedKeyframeAuditMapCleanup:
      clientSource.includes('requestedKeyframesByTimestamp.delete(chunk.timestamp)') &&
      clientSource.includes('requestedKeyframesByTimestamp.clear()'),
    suffixedE2eReportsIgnored:
      gitignoreSource.includes('/artifacts/e2e-media-smoke*.json') &&
      gitignoreSource.includes('/artifacts/e2e-media-smoke*.txt') &&
      !/^\s*\/artifacts\/\s*$/m.test(gitignoreSource),
    layersChangeDebugGuard: isInsideDebugGuard(
      serverSource,
      '[simulcast] ${peer.id} current layers'
    ),
    serverStatsPumpDebugGuard: isInsideDebugGuard(serverSource, 'startStatsPump(socket, peer)'),
    pipelineStatsDebugGuard: isInsideDebugGuard(serverSource, "event: 'pipelineStats'"),
    transportStateDebugGuard: isInsideDebugGuard(serverSource, "event: 'transportState'"),
    workerDebugLevelSwitch:
      /logLevel:\s*IS_DEBUG\s*\?\s*['"]debug['"]\s*:\s*['"]warn['"]/.test(serverSource),
    clientLogBoxModeSwitch: /logBox\.hidden\s*=\s*!IS_DEBUG/.test(clientSource),
    h264EncoderConfig: /codec:\s*['"]avc1\.42E01F['"]/.test(clientSource),
    h264RtpPipeline: /mimeType:\s*['"]video\/H264['"]/.test(pipelineSource),
    producerModeQuery: clientSource.includes('simulcast=${simulcastEnabled ? \'1\' : \'0\'}') &&
      serverSource.includes('parseSimulcastPreference(request.url)'),
    activeEncodingSelection: serverSource.includes('activeProducerEncodings') &&
      serverSource.includes('encodings: activeProducerEncodings'),
    simpleProducerOffPath: serverSource.includes(': [HIGH_ENCODING]') &&
      serverSource.includes("producerType: producer?.type ?? null"),
    productionConsumerLayerAction:
      serverSource.includes("action === 'setConsumerLayer'") &&
      /setConsumerLayer[\s\S]{0,900}setPreferredLayers/.test(serverSource),
    consumerLayerOffGuard:
      /action === 'setConsumerLayer'[\s\S]{0,450}producerSimulcastEnabled !== true/.test(serverSource),
    activeTraceAndChunkValidation:
      /activeProducerEncodings\.find\(candidate => candidate\.ssrc === ssrc\)/.test(serverSource) &&
      /Encoded chunk layer .*not active for the current Producer mode/.test(serverSource),
    producerCleanupResetsMode:
      /producerSimulcastEnabled = undefined;[\s\S]{0,120}activeProducerEncodings = \[\]/.test(serverSource),
    producerToggleUi:
      /id="simulcastToggle"/.test(await readFile(path.join(ROOT, 'index.html'), 'utf8')) &&
      clientSource.includes('simulcastToggle.checked'),
    consumerLayerUi:
      /id="consumerLayerLow"/.test(await readFile(path.join(ROOT, 'index.html'), 'utf8')) &&
      clientSource.includes("'setConsumerLayer'"),
    statsDumpSchemaV1: /schemaVersion:\s*1/.test(statsDumpSource),
    consumerStatsReusesSingleGetStatsPoll:
      (clientSource.match(/recvTransport\.getStats\(\)/g) ?? []).length === 1 &&
      !statsDumpSource.includes('.getStats('),
    statsRetentionLimitsAndRoleScopedAppend:
      /MAX_STATS_SAMPLES\s*=\s*4200/.test(statsDumpSource) &&
      /MAX_STATS_DURATION_MS\s*=\s*70\s*\*\s*60\s*\*\s*1000/.test(statsDumpSource) &&
      statsDumpSource.includes('appendProducerSample') &&
      statsDumpSource.includes('appendConsumerSample') &&
      statsDumpSource.includes('collectionStatus') &&
      statsDumpSource.includes('measurementEndTime'),
    noDirectStatsArrayPush:
      !clientSource.includes('producerStats.push') &&
      !clientSource.includes('consumerStats.push'),
    consumerPollCanStopAndIgnoresLateResults:
      /const intervalId = window\.setInterval/.test(clientSource) &&
      clientSource.includes('window.clearInterval(intervalId)') &&
      /\.then\(report => \{\s*if \(!canApplyConsumerStatsResult\(stopped, statsDumpSession === session\)\)\s*\{\s*return;/.test(clientSource) &&
      /\.catch\(\(\) => \{\s*if \(canApplyConsumerStatsResult\(stopped, statsDumpSession === session\)\)/.test(clientSource) &&
      /function canApplyConsumerStatsResult\([\s\S]{0,160}return !stopped && isCurrentSession;/.test(consumerStatsSource),
    socketAndTrackCloseStopCollection:
      /ws\.addEventListener\('close',[\s\S]{0,180}stopActiveStatsCollection\(\)/.test(clientSource) &&
      /addEventListener\(\s*'ended',[\s\S]{0,180}stopActiveStatsCollection\(\)/.test(clientSource) &&
      /producerClosed'[\s\S]{0,180}stopActiveStatsCollection\(\)/.test(clientSource),
    producerPipelineEndStopsCollection:
      /finally\s*\{[\s\S]{0,500}stopActiveStatsCollection\(\)/.test(clientSource),
    sampleLimitIsUserVisible:
      /Recording stopped: sample\/time limit reached\. Download the collected statistics\./.test(clientSource) &&
      /Samples:.*MAX_STATS_SAMPLES/.test(clientSource) &&
      /id="measurementStatus"[^>]*aria-live="polite"/.test(
        await readFile(path.join(ROOT, 'index.html'), 'utf8')
      ),
    sourceFramesCountedOnce:
      encodeFrameStart >= 0 &&
      encodeFrameEnd > sourceFrameIncrementPosition &&
      sourceFrameIncrementPositions.length === 1 &&
      sourceFrameIncrementPosition >= encodeFrameStart &&
      sourceFrameIncrementPosition < encodeFrameEnd &&
      layerFanoutPosition > sourceFrameIncrementPosition &&
      layerFanoutPosition < encodeFrameEnd,
    sentPayloadCountersFollowSocketSend:
      clientSource.indexOf('socket.send(packet)') >= 0 &&
      clientSource.indexOf('counters.chunks++') > clientSource.indexOf('socket.send(packet)'),
    visualizerUsesSafeDomAndNoPacketFpsFallback:
      !visualizerSource.includes('innerHTML') &&
      !/packetsReceived[\s\S]{0,180}\/\s*elapsed/.test(visualizerSource),
    localChartJsDefaultAndSvgFallback:
      visualizerSource.includes("await import('./statsChartJs.js')") &&
      visualizerSource.includes('renderSvgChart(charts, definition)') &&
      visualizerSource.includes('Renderer: SVG (Chart.js unavailable)') &&
      chartJsSource.includes("from 'chart.js/auto'") &&
      chartJsSource.includes('parsing: false') &&
      chartJsSource.includes('spanGaps: false'),
    chartJsExactPackageAndLocalChunk:
      packageJson.dependencies?.['chart.js'] === '4.5.1' &&
      viteConfigSource.includes("return 'chartjs'"),
    renderDecimationUsesActualTimeAndKeepsGapMarkers:
      chartDataSource.includes('createElapsedSeconds') &&
      chartDataSource.includes('decimateChartSeries') &&
      !visualizerSource.includes('Math.max(...') &&
      visualizerSource.includes('point.y === null') &&
      chartDataSource.includes('selected.add(minimumIndex)') &&
      chartDataSource.includes('selected.add(maximumIndex'),
    retentionAndChartTestsAreInNpmTest:
      packageJson.scripts?.test?.includes('test:stats') &&
      packageJson.scripts?.['test:stats']?.includes('stats-retention.test.mjs'),
    productionVisualizerBuildEntry:
      viteConfigSource.includes("'stats-visualizer.html'") &&
      viteConfigSource.includes("'index.html'"),
    fixedStrictBrowserPort:
      (viteConfigSource.match(/port:\s*5173/g) ?? []).length === 2 &&
      (viteConfigSource.match(/strictPort:\s*true/g) ?? []).length === 2,
    browserProxyUsesBackendPort:
      viteConfigSource.includes('process.env.PORT ?? 3000') &&
      viteConfigSource.includes("'/ws'") && viteConfigSource.includes("'/health'"),
    gracefulShutdownSignalHandlers:
      /process\.once\('SIGINT',[\s\S]{0,100}shutdown\('SIGINT'\)/.test(serverSource) &&
      /process\.once\('SIGTERM',[\s\S]{0,100}shutdown\('SIGTERM'\)/.test(serverSource) &&
      serverSource.includes('const webSocketServerClosed = closeWebSocketServer()') &&
      serverSource.includes('const httpServerClosed = closeHttpServer()') &&
      serverSource.includes('await Promise.all([webSocketServerClosed, httpServerClosed])')
  };
  const allPass = Object.values(guards).every(Boolean);
  report.staticValidation.sourceGuards = {
    status: allPass ? 'passed' : 'failed',
    checks: guards
  };
  const coalescingPass =
    guards.onePendingKeyframeObligationPerLayer &&
    guards.boundedPendingKeyframeAuditRequests &&
    guards.pendingRequestConsumedAfterSuccessfulEncode &&
    guards.requestedKeyframeEncodeFailureRetriesNextFrame &&
    guards.requestedKeyframeAuditMapCleanup;
  Object.assign(report.production.pliFir.coalescing, {
    status: coalescingPass ? 'passed' : 'failed',
    verification: 'static source assertions',
    maximumAuditRequests: 16,
    onePendingObligationPerLayer: guards.onePendingKeyframeObligationPerLayer,
    consumesAfterEncodeSuccess: guards.pendingRequestConsumedAfterSuccessfulEncode,
    retriesNextFrameOnFirstEncodeFailure: guards.requestedKeyframeEncodeFailureRetriesNextFrame,
    clearsAuditMapOnStop: guards.requestedKeyframeAuditMapCleanup
  });
  ensure(allPass, 'One or more expected debug/H.264 source guards are missing.');
  ensure(coalescingPass, 'One or more keyframe coalescing source guards are missing.');
  return guards;
}

async function runStaticPreparation() {
  await withStatus(report.staticValidation.pipeline, async () => {
    const invocation = npmInvocation(['run', 'verify-mediasoup-pipeline']);
    const processRecord = startProcess(
      'pipeline-verify',
      invocation.command,
      invocation.args,
      { shell: invocation.shell }
    );
    await runToCompletion(processRecord);
    report.staticValidation.pipeline.outputTail = processRecord.tail;
  });

  await withStatus(report.staticValidation.typecheck, async () => {
    const tscPath = path.join(ROOT, 'node_modules', 'typescript', 'bin', 'tsc');
    await access(tscPath, fsConstants.F_OK);
    const processRecord = await runNodeCommand('typescript-typecheck', tscPath, ['--noEmit']);
    report.staticValidation.typecheck.outputTail = processRecord.tail;
  });

  await withStatus(report.staticValidation.productionBuild, async () => {
    const invocation = npmInvocation(['run', 'build']);
    const processRecord = startProcess(
      'production-build',
      invocation.command,
      invocation.args,
      { shell: invocation.shell }
    );
    await runToCompletion(processRecord);
    report.staticValidation.productionBuild.outputTail = processRecord.tail;
  });

  await withStatus(report.staticValidation.productionBundle, async () => {
    const builtHtml = await readFile(path.join(ROOT, 'dist', 'index.html'), 'utf8');
    const builtVisualizer = await readFile(
      path.join(ROOT, 'dist', 'stats-visualizer.html'),
      'utf8'
    );
    const builtAssets = await readdir(path.join(ROOT, 'dist', 'assets'));
    const chartJsAsset = builtAssets.find(asset => /^chartjs-[^/]+\.js$/.test(asset));
    ensure(
      !builtHtml.includes('/src/client.ts') && /\/assets\/index-[^"]+\.js/.test(builtHtml),
      'Production dist/index.html does not reference the built client bundle.'
    );
    ensure(
      builtVisualizer.includes('stats-file') &&
        builtVisualizer.includes('dashboard') &&
        /\/assets\/statsVisualizer-[^"]+\.js/.test(builtVisualizer),
      'Production dist/stats-visualizer.html does not include the Visualizer UI and built module.'
    );
    ensure(chartJsAsset,
      'Production build does not contain the locally bundled Chart.js chunk.');
    report.staticValidation.productionBundle.clientAsset =
      builtHtml.match(/\/assets\/index-[^"]+\.js/)?.[0] ?? null;
    report.staticValidation.productionBundle.statsVisualizer = {
      html: 'dist/stats-visualizer.html',
      javascriptAsset:
        builtVisualizer.match(/\/assets\/statsVisualizer-[^"]+\.js/)?.[0] ?? null,
      chartJsAsset: chartJsAsset ? '/assets/' + chartJsAsset : null
    };
  });

  await withStatus(report.staticValidation.sourceGuards, auditSourceGuards);
}

async function startProducerCycle(
  mode,
  appUrl,
  testLabel,
  cycleTrace = createProducerCycleTrace(),
  options = {}
) {
  startProducerCycleTrace(cycleTrace, testLabel);
  let cycleError = null;
  let pattern = null;
  let app = null;
  try {
    pattern = await openPatternPage(testLabel + '-pattern', mode, cycleTrace);
    cycleTrace.checkpoints.patternFramesAdvancing.before = pattern.frameCounter.before;
    cycleTrace.checkpoints.patternFramesAdvancing.after = pattern.frameCounter.after;
    app = await openAppPage(
      appUrl,
      testLabel + '-producer',
      mode,
      cycleTrace
    );
    const logState = await app.page.$eval('#log', element => ({
      hidden: element.hidden,
      text: element.textContent ?? ''
    }));
    if (mode === 'production') {
      ensure(logState.hidden, 'Production #log is visible before Start.');
    }

    await withCycleCheckpoint(cycleTrace, 'captureSourceInventoryChecked', () =>
      recordCaptureSourceInventory(cycleTrace, testLabel)
    );
    const producerStartStartedAt = Date.now();
    const producerDeadline = producerStartStartedAt + PAGE_TIMEOUT_MS;
    cycleTrace.producerStartDeadline = {
      startedAt: new Date(producerStartStartedAt).toISOString(),
      deadlineAt: new Date(producerDeadline).toISOString(),
      timeoutMs: PAGE_TIMEOUT_MS
    };
    if (options.simulcastEnabled !== undefined) {
      await app.page.$eval(
        '#simulcastToggle',
        (element, enabled) => {
          element.checked = Boolean(enabled);
        },
        options.simulcastEnabled
      );
    }
    await startAs(
      app.page,
      'producer',
      cycleTrace,
      testLabel,
      app.audit,
      producerDeadline,
      options.simulcastEnabled ?? true
    );
    const producerMetrics = await assertProducerProgress(
      app.page,
      currentServerRecord,
      cycleTrace,
      testLabel,
      producerDeadline,
      options.simulcastEnabled ?? true
    );
    await syncWebSocketAudit(app.page, app.audit);
    const producerWebSocketUrls = assertBrowserWebSocketProxy(
      app.audit,
      testLabel + ' Producer'
    );
    if (mode === 'production') {
      report.production.endpoint.websocketProxy = 'passed';
      report.production.endpoint.websocketUrls = producerWebSocketUrls;
    }
    const codecWaitStartedAt = Date.now();
    let codecEvidence;
    try {
      codecEvidence = await withCycleCheckpoint(
        cycleTrace,
        'h264CodecEvidenceObserved',
        () => waitForCondition(async () => {
          await syncWebSocketAudit(app.page, app.audit);
          return findH264CodecEvidence(app.audit);
        },
        testLabel + ' outgoing H.264 (avc1) WebCodecs metadata',
        Math.max(0, producerDeadline - Date.now()),
        RUNTIME_POLL_INTERVAL_MS,
        producerDeadline)
      );
    } catch (error) {
      await syncWebSocketAudit(app.page, app.audit).catch(() => {});
      const finalEvidence = findH264CodecEvidence(app.audit);
      const diagnostic = recordRuntimeWaitDiagnostic({
        cycleTrace,
        checkpointName: 'h264CodecEvidenceObserved',
        cycleLabel: testLabel,
        description: testLabel + ': outgoing H.264 codec evidence',
        startedAt: codecWaitStartedAt,
        finalSnapshot: cycleTrace.lastUiSnapshot,
        finalAuditSnapshot: {
          encoderOutputFrames: app.audit.encoderOutputFrames,
          configuredEncoderCodecs: app.audit.configuredEncoderCodecs,
          outputEncoderCodecs: app.audit.outputEncoderCodecs,
          encodedChunkCodecs: app.audit.encodedChunkCodecs
        },
        conditionSatisfied: Boolean(finalEvidence),
        error
      });
      throw new Error(
        testLabel + ': outgoing H.264 codec evidence did not arrive within the shared ' +
          PAGE_TIMEOUT_MS + 'ms producer-start deadline (classification=' +
          diagnostic.classification + ')',
        { cause: error }
      );
    }
    producerMetrics.configuredEncoderCodec = app.audit.configuredEncoderCodecs[0] ?? null;
    producerMetrics.actualEncoderCodec = codecEvidence.codec;
    producerMetrics.codecEvidenceSource = codecEvidence.source;
    producerMetrics.encoderOutputFrames = app.audit.encoderOutputFrames;
    producerMetrics.outputEncoderCodecs = app.audit.outputEncoderCodecs;
    producerMetrics.encodedChunkCodecs = app.audit.encodedChunkCodecs;
    cycleTrace.getDisplayMediaAudit = app.audit.displayCapture;
    recordCycleCheckpoint(cycleTrace, 'h264CodecEvidenceObserved', true, codecEvidence);
    cycleTrace.timingSanityWarnings = PRODUCER_TIMING_CHECKPOINTS
      .filter(name => Number(cycleTrace.checkpoints[name]?.durationMs) > PRODUCER_CHECKPOINT_MAX_MS)
      .map(name => ({
        checkpoint: name,
        status: cycleTrace.checkpoints[name].status,
        durationMs: cycleTrace.checkpoints[name].durationMs,
        thresholdMs: PRODUCER_CHECKPOINT_MAX_MS
      }));
    ensure(
      cycleTrace.timingSanityWarnings.length === 0,
      testLabel + ': Producer runtime checkpoints exceeded the ' +
        PRODUCER_CHECKPOINT_MAX_MS + 'ms timing sanity limit: ' +
        cycleTrace.timingSanityWarnings.map(item =>
          item.checkpoint + '=' + item.durationMs + 'ms'
        ).join(', ')
    );
    ensure(
      Date.now() <= producerDeadline,
      testLabel + ': Producer start deadline expired before cycleReady.'
    );
    await captureProducerCycleEvidence(cycleTrace, testLabel);
    ensure(
      Date.now() <= producerDeadline,
      testLabel + ': Producer start deadline expired while recording cycle evidence.'
    );
    recordCycleCheckpoint(cycleTrace, 'cycleReady', true);
    return {
      patternPage: pattern.page,
      patternAudit: pattern.audit,
      producerPage: app.page,
      producerAudit: app.audit,
      producerMetrics,
      initialLog: logState,
      patternFrames: pattern.frameCounter
    };
  } catch (error) {
    cycleError = error;
    await captureProducerCycleEvidence(cycleTrace, testLabel);
    throw error;
  } finally {
    finishProducerCycleTrace(cycleTrace, cycleError);
  }
}

async function startConsumerPage(mode, appUrl, label) {
  let app = null;
  try {
    app = await openAppPage(appUrl, label, mode);
    await startAs(app.page, 'consumer', null, label, app.audit);
    await syncWebSocketAudit(app.page, app.audit);
    const consumerWebSocketUrls = assertBrowserWebSocketProxy(
      app.audit,
      label + ' Consumer'
    );
    if (mode === 'production') {
      report.production.endpoint.websocketProxy = 'passed';
      report.production.endpoint.websocketUrls.push(...consumerWebSocketUrls);
      report.production.endpoint.websocketUrls = [
        ...new Set(report.production.endpoint.websocketUrls)
      ];
    }
    return app;
  } catch (error) {
    if (app && !app.page.isClosed()) {
      await app.page.close().catch(() => {});
    }
    throw error;
  }
}

function findE2EConsumerAudit(health, peerId) {
  ensure(
    Array.isArray(health.e2eConsumerAudit),
    'E2E Consumer audit was not present in the E2E-enabled health payload.'
  );
  const consumer = health.e2eConsumerAudit.find(item => item.peerId === peerId);
  ensure(consumer, `E2E Consumer audit did not contain peer ${String(peerId)}.`);
  return consumer;
}

function processLogHasDebug(record) {
  return Boolean(record?.tail && /\[debug\]/i.test(record.tail));
}

function diagnosticEvents(audit) {
  return Object.entries(audit.websocketEventCounts)
    .filter(([event, count]) => DIAGNOSTIC_EVENTS.has(event) && count > 0)
    .map(([event, count]) => ({ event, count }));
}

async function waitForWebSocketEvent(
  page,
  audit,
  eventName,
  description,
  timeoutMs = PAGE_TIMEOUT_MS
) {
  const startedAt = Date.now();
  try {
    return await waitForCondition(async () => {
      await syncWebSocketAudit(page, audit);
      return audit.websocketEventCounts[eventName] ?? 0;
    }, description, timeoutMs, RUNTIME_POLL_INTERVAL_MS);
  } catch (error) {
    await syncWebSocketAudit(page, audit).catch(() => {});
    const count = audit.websocketEventCounts[eventName] ?? 0;
    const diagnostic = recordRuntimeWaitDiagnostic({
      checkpointName: eventName,
      description,
      startedAt,
      finalSnapshot: null,
      finalAuditSnapshot: {
        event: eventName,
        count,
        eventCounts: audit.websocketEventCounts
      },
      conditionSatisfied: count > 0,
      error
    });
    throw new Error(
      description + ' (classification=' + diagnostic.classification + ')',
      { cause: error }
    );
  }
}

async function waitForNodeWebSocketMessage(socket, predicate, description, timeoutMs = 10_000) {
  return new Promise((resolve, reject) => {
    let done = false;
    const finish = (error, message) => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timer);
      socket.off('message', onMessage);
      socket.off('error', onError);
      if (error) {
        reject(error);
      } else {
        resolve(message);
      }
    };
    const onMessage = data => {
      let message;
      try {
        message = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (predicate(message)) {
        finish(null, message);
      }
    };
    const onError = error => finish(error);
    const timer = setTimeout(
      () => finish(new Error('Timed out waiting for ' + description + '.')),
      timeoutMs
    );
    socket.on('message', onMessage);
    socket.on('error', onError);
  });
}

async function verifyE2eControlDisabledByDefault() {
  await assertPortFree(SIGNALING_PORT);
  const server = startNpmScript(
    'server-e2e-control-disabled',
    'server:prod',
    [],
    {
      PORT: String(SIGNALING_PORT),
      SFU_DEBUG: '0',
      SFU_E2E_MEDIA: '0',
      NODE_ENV: 'production'
    }
  );
  currentServerRecord = server;
  let socket;
  try {
    const normalModeHealth = await waitForHealth(
      server,
      health => health.ok === true,
      'control-guard /health'
    );
    ensure(
      !Object.hasOwn(normalModeHealth, 'e2eConsumerAudit'),
      'Normal production health payload exposed E2E Consumer audit data.'
    );
    socket = new WebSocket('ws://' + SIGNALING_HOST + ':' + SIGNALING_PORT + '/ws');
    const roleAssigned = waitForNodeWebSocketMessage(
      socket,
      message => message.event === 'roleAssigned',
      'normal-mode role assignment'
    );
    await new Promise((resolve, reject) => {
      socket.once('open', resolve);
      socket.once('error', reject);
    });
    await roleAssigned;

    const requestId = 'normal-mode-e2e-control-guard';
    const responsePromise = waitForNodeWebSocketMessage(
      socket,
      message => message.id === requestId,
      'normal-mode E2E-control rejection'
    );
    socket.send(JSON.stringify({
      id: requestId,
      action: 'e2eRequestKeyFrame',
      data: { layer: 'low' }
    }));
    const response = await responsePromise;
    ensure(
      response.ok === false && /E2E media controls are disabled/.test(response.error ?? ''),
      'Normal production mode accepted or mishandled the E2E keyframe action.'
    );
    return {
      action: 'e2eRequestKeyFrame',
      accepted: false,
      error: response.error,
      e2eConsumerAuditInHealth: Object.hasOwn(normalModeHealth, 'e2eConsumerAudit')
    };
  } finally {
    if (socket && socket.readyState !== WebSocket.CLOSED) {
      socket.close();
    }
    await stopProcess(server);
    if (currentServerRecord === server) {
      currentServerRecord = null;
    }
  }
}

async function runFreshConsumerPliLayerTest(layer, producerPage, producerAudit) {
  const target = report.production.pliFir.byLayer[layer];
  const expectedSsrc = { low: 12345678, mid: 12345679, high: 12345680 }[layer];
  const expectedSpatialLayer = { low: 0, mid: 1, high: 2 }[layer];
  let consumerPage = null;
  let peerId = null;
  let baselineConsumerCount = null;
  let operationError = null;
  let cleanupError = null;

  try {
    await withStatus(target, async () => {
      target.failureStage = 'fresh-consumer-setup';
      const baselineHealth = await fetchJson(HEALTH_URL);
      baselineConsumerCount = baselineHealth.consumers;
      await assertConsumerCount(baselineConsumerCount);

      const creationStartedAt = Date.now();
      const freshConsumer = await startConsumerPage(
        'production',
        PRODUCTION_APP_URL,
        `pli-${layer}-fresh-consumer`
      );
      consumerPage = freshConsumer.page;
      const initialUi = await waitForConsumerReady(
        consumerPage,
        `${layer} fresh Consumer media and FPS`
      );
      peerId = initialUi.peerId;

      const healthAfterStart = await assertConsumerCount(baselineConsumerCount + 1);
      const consumerAudit = findE2EConsumerAudit(healthAfterStart, peerId);
      ensure(
        consumerAudit.consumerId && consumerAudit.transportId &&
          Number.isFinite(consumerAudit.createdAt),
        `${layer} fresh Consumer ID, transport ID, or creation time was not audited.`
      );
      const priorSessions = Object.entries(report.production.pliFir.byLayer)
        .filter(([otherLayer, result]) =>
          otherLayer !== layer && result.consumerSession
        )
        .map(([, result]) => result.consumerSession);
      ensure(
        priorSessions.every(session =>
          session.peerId !== consumerAudit.peerId &&
          session.consumerId !== consumerAudit.consumerId &&
          session.transportId !== consumerAudit.transportId
        ),
        `${layer} PLI test reused a previous Consumer peer, Consumer, or transport.`
      );
      target.consumerSession = {
        peerId: consumerAudit.peerId,
        consumerId: consumerAudit.consumerId,
        transportId: consumerAudit.transportId,
        consumerCreatedAt: new Date(consumerAudit.createdAt).toISOString(),
        consumerCreatedAtEpochMs: consumerAudit.createdAt,
        pageReadyAt: new Date().toISOString(),
        pageReadyElapsedMs: Date.now() - creationStartedAt,
        expectedConsumerCountWhileOpen: baselineConsumerCount + 1
      };
      target.initialLayerState = {
        preferredLayers: consumerAudit.preferredLayers,
        currentLayers: consumerAudit.currentLayers
      };
      target.initialMedia = {
        consumerState: initialUi.consumerState,
        rtpReceived: initialUi.rtpReceived,
        framesDecoded: initialUi.framesDecoded,
        receivedFps: initialUi.receivedFps,
        decodedFps: initialUi.decodedFps
      };

      const before = await readUiSnapshot(consumerPage);
      const request = await requestOverAuditedSocket(
        consumerPage,
        'e2eRequestKeyFrame',
        { layer }
      );
      const layerSelection = request.layerSelection ??
        request.spatialLayerDiagnostics ?? null;
      target.layerSelection = layerSelection;
      target.layerSelectionMs = layerSelection?.transitionElapsedMs ?? null;
      target.targetReachedAtEpochMs = layerSelection?.targetReachedAt ?? null;
      target.targetReachedAt = layerSelection?.targetReachedAt
        ? new Date(layerSelection.targetReachedAt).toISOString()
        : null;
      target.requestKeyFrameReached = request.e2eOk === true &&
        request.requestKeyFrameReached === true;
      if (!request.e2eOk) {
        target.failureStage = layerSelection?.stage ?? 'e2eRequestKeyFrame';
        target.pliStatus = 'not-reached';
        report.production.pliFir.failureClassification =
          layerSelection?.stage === 'waitForConsumerSpatialLayer'
            ? `FRESH_${layer.toUpperCase()}_LAYER_SELECTION_FAILURE`
            : `FRESH_${layer.toUpperCase()}_REQUEST_FAILURE`;
        throw new Error(
          request.e2eError ?? `${layer} fresh Consumer E2E request failed.`
        );
      }
      ensure(
        request.layer === layer && request.spatialLayer === expectedSpatialLayer,
        `${layer} fresh Consumer request returned the wrong target layer.`
      );
      ensure(
        layerSelection?.result === 'reached' &&
          layerSelection.finalCurrentLayers?.spatialLayer === expectedSpatialLayer,
        `${layer} fresh Consumer did not reach the target spatial layer.`
      );
      ensure(
        target.requestKeyFrameReached,
        `${layer} fresh Consumer response did not confirm consumer.requestKeyFrame().`
      );
      target.requestId = request.requestId;
      target.requestedAt = request.requestedAt;
      target.failureStage = 'pli-feedback-evidence';

      const observations = await waitForKeyframeAudit(
        producerPage,
        request.requestId,
        `${layer} fresh Consumer Producer PLI/FIR and requested WebCodecs key chunk`
      );
      await syncWebSocketAudit(producerPage, producerAudit);
      const health = await fetchJson(HEALTH_URL);
      const feedbackAudit = health.keyframeFeedback;
      ensure(feedbackAudit, 'E2E Producer feedback audit was not available.');
      const trace = feedbackAudit.traces.find(
        item => item.requestId === request.requestId
      );
      ensure(trace, `${layer} Producer PLI/FIR trace was not recorded.`);
      ensure(
        trace.traceType === 'pli' || trace.traceType === 'fir',
        `${layer} Producer trace has an unexpected type: ${String(trace.traceType)}.`
      );
      ensure(trace.ssrc === expectedSsrc, `${layer} feedback used the wrong SSRC.`);
      ensure(trace.layer === layer, `${layer} SSRC mapped to ${String(trace.layer)}.`);

      const browserEvent = observations.event;
      const encodedKeyframe = observations.chunk;
      const matchedRequest = encodedKeyframe.requests?.find(
        item => item.requestId === request.requestId
      );
      const pliEvidence = {
        traceType: trace.traceType,
        ssrc: trace.ssrc,
        mappedLayer: trace.layer,
        browserEventLayer: browserEvent.layer,
        browserEventReason: browserEvent.reason,
        encodedLayer: encodedKeyframe.layer,
        chunkType: encodedKeyframe.chunkType,
        keyframeOption: encodedKeyframe.keyframeOption,
        requestConsumed: Boolean(matchedRequest),
        periodicKeyframe: encodedKeyframe.periodicKeyframe,
        resizeKeyframe: encodedKeyframe.resizeKeyframe
      };
      target.pliEvidence = pliEvidence;
      ensure(browserEvent.layer === layer, `${layer} browser event targeted the wrong layer.`);
      ensure(
        browserEvent.reason === trace.traceType,
        `${layer} browser event reason differs from the Producer trace.`
      );
      ensure(
        matchedRequest,
        `${layer} WebCodecs output did not consume the requested keyframe flag.`
      );
      ensure(
        encodedKeyframe.layer === layer,
        `${layer} keyframe output came from another encoder.`
      );
      ensure(
        encodedKeyframe.chunkType === 'key' &&
          encodedKeyframe.keyframeOption === true,
        `${layer} requested encode did not produce a WebCodecs key chunk.`
      );

      const requestToTraceMs = trace.traceAt - request.requestedAt;
      const traceToBrowserMs = browserEvent.receivedAt - trace.traceAt;
      const browserToEncodedMs = encodedKeyframe.outputAt - browserEvent.receivedAt;
      const requestToEncodedMs = encodedKeyframe.outputAt - request.requestedAt;
      ensure(
        [requestToTraceMs, traceToBrowserMs, browserToEncodedMs, requestToEncodedMs]
          .every(value => Number.isFinite(value) && value >= 0),
        `${layer} PLI feedback timestamps were not monotonic.`
      );
      target.latency = {
        layerSelectionMs: target.layerSelectionMs,
        requestToTraceMs,
        traceToBrowserMs,
        browserToEncodedMs,
        requestToEncodedMs
      };

      target.failureStage = 'consumer-media-progress';
      let after;
      await waitForCondition(async () => {
        after = await readUiSnapshot(consumerPage);
        return after.rtpReceived > before.rtpReceived &&
          after.framesDecoded > before.framesDecoded &&
          after.receivedFps > 0 && after.decodedFps > 0;
      }, `${layer} fresh Consumer RTP and decode progress`, 8_000, 200);
      target.consumerMedia = {
        before: {
          rtpReceived: before.rtpReceived,
          framesDecoded: before.framesDecoded,
          receivedFps: before.receivedFps,
          decodedFps: before.decodedFps
        },
        after: {
          rtpReceived: after.rtpReceived,
          framesDecoded: after.framesDecoded,
          receivedFps: after.receivedFps,
          decodedFps: after.decodedFps,
          consumerState: after.consumerState
        },
        rtpDelta: after.rtpReceived - before.rtpReceived,
        decodedFrameDelta: after.framesDecoded - before.framesDecoded,
        continued: true
      };
      target.traceType = trace.traceType;
      target.ssrc = trace.ssrc;
      target.mappedLayer = trace.layer;
      target.browserReceivedAt = browserEvent.receivedAt;
      target.encodeRequestedAt = encodedKeyframe.encodeRequestedAt;
      target.encodedAt = encodedKeyframe.outputAt;
      target.encodedTimestamp = encodedKeyframe.encodedTimestamp;
      target.pliStatus = 'passed';
      target.failureStage = null;
    });
  } catch (error) {
    operationError = error;
    target.failureStage ??= target.layerSelection?.stage ?? 'freshConsumerSetupOrPliEvidence';
    if (target.pliStatus === 'not-run') {
      target.pliStatus = target.requestKeyFrameReached ? 'failed' : 'not-reached';
    } else if (target.pliStatus === 'running') {
      target.pliStatus = 'failed';
    }
    if (!report.production.pliFir.failureClassification) {
      report.production.pliFir.failureClassification =
        `FRESH_${layer.toUpperCase()}_PLI_OR_MEDIA_FAILURE`;
    }
  } finally {
    let pageClosed = consumerPage === null;
    if (consumerPage) {
      try {
        if (!consumerPage.isClosed()) {
          await consumerPage.close();
        }
        pageClosed = consumerPage.isClosed();
      } catch (error) {
        cleanupError = error;
      }
    }

    try {
      if (baselineConsumerCount === null) {
        baselineConsumerCount = (await fetchJson(HEALTH_URL)).consumers;
      }
      const cleanupHealth = await assertConsumerCount(baselineConsumerCount);
      const consumerStillPresent = peerId !== null &&
        cleanupHealth.e2eConsumerAudit?.some(item => item.peerId === peerId);
      target.cleanup = {
        pageClosed,
        expectedConsumerCount: baselineConsumerCount,
        observedConsumerCount: cleanupHealth.consumers,
        consumerCountRestored: cleanupHealth.consumers === baselineConsumerCount,
        consumerRemovedFromAudit: !consumerStillPresent
      };
      if (
        !pageClosed ||
        cleanupHealth.consumers !== baselineConsumerCount ||
        consumerStillPresent
      ) {
        throw new Error(`${layer} fresh Consumer cleanup did not restore the baseline.`);
      }
      target.cleanup.status = 'passed';
    } catch (error) {
      cleanupError = cleanupError ?? error;
      target.cleanup ??= { status: 'failed' };
      target.cleanup.status = 'failed';
      target.cleanup.error = errorMessage(error);
    }
  }

  if (operationError || cleanupError) {
    target.status = 'failed';
    if (cleanupError && !report.production.pliFir.failureClassification) {
      report.production.pliFir.failureClassification =
        `FRESH_${layer.toUpperCase()}_CONSUMER_CLEANUP_FAILURE`;
    }
    target.error = [operationError, cleanupError]
      .filter(Boolean)
      .map(errorMessage)
      .join('; cleanup: ');
    throw new Error(target.error, { cause: operationError ?? cleanupError });
  }
}

async function runPliFirLayerTests(producerPage, producerAudit) {
  const target = report.production.pliFir;
  const baselineHealth = await fetchJson(HEALTH_URL);
  ensure(baselineHealth.ok === true, 'Production SFU health was not ready for PLI tests.');

  for (const layer of ['low', 'mid', 'high']) {
    await runFreshConsumerPliLayerTest(layer, producerPage, producerAudit);
  }

  await syncWebSocketAudit(producerPage, producerAudit);
  const finalHealth = await fetchJson(HEALTH_URL);
  target.serverAudit = finalHealth.keyframeFeedback;
  target.fir = {
    staticSupport: true,
    runtimeObserved: (target.serverAudit?.firReceived ?? 0) > 0,
    runtimeStatus: (target.serverAudit?.firReceived ?? 0) > 0
      ? 'observed'
      : 'not-observed'
  };
  target.keyframeRequestEventsReceived = producerAudit.keyframeRequestEvents?.length ?? 0;
  target.requestedKeyframesEncoded = producerAudit.requestedKeyframeChunks?.length ?? 0;
  target.latency = Object.fromEntries(
    ['low', 'mid', 'high'].map(layer => [layer, target.byLayer[layer].latency])
  );
  target.consumerPlaybackContinuous = ['low', 'mid', 'high'].every(layer =>
    target.byLayer[layer].consumerMedia?.continued === true
  );
  target.productionOverhead = {
    enabledProducerTraceEvents: ['pli', 'fir'],
    perPacketTracingEnabled: false,
    diagnosticLoggingDebugOnly: true,
    serverDebugLogObserved: processLogHasDebug(currentServerRecord),
    initialAvailableOutgoingBitrateBps:
      report.execution.bweControl.effectiveInitialAvailableOutgoingBitrateBps ??
      DEFAULT_INITIAL_AVAILABLE_OUTGOING_BITRATE_BPS,
    baselineHealthConsumerCount: baselineHealth.consumers,
    mediaProgressContinued: target.consumerPlaybackContinuous
  };
  ensure(
    target.keyframeRequestEventsReceived >= 3 &&
      target.requestedKeyframesEncoded >= 3,
    'Producer browser did not receive and encode all three requested keyframes.'
  );
  ensure(
    target.consumerPlaybackContinuous,
    'One or more fresh Consumers stopped RTP/decode progress during PLI verification.'
  );
}

async function runSequentialLayerSelectionDiagnostic(consumerPage) {
  const target = report.production.layerSelectionDiagnostic;
  const initialUi = await readUiSnapshot(consumerPage);
  const initialHealth = await fetchJson(HEALTH_URL);
  const consumerAudit = findE2EConsumerAudit(initialHealth, initialUi.peerId);
  target.consumerSession = {
    peerId: consumerAudit.peerId,
    consumerId: consumerAudit.consumerId,
    transportId: consumerAudit.transportId,
    consumerCreatedAt: consumerAudit.createdAt
      ? new Date(consumerAudit.createdAt).toISOString()
      : null,
    preferredLayers: consumerAudit.preferredLayers,
    currentLayers: consumerAudit.currentLayers
  };
  const constrainedLayers = [];
  const layerIndexes = { low: 0, mid: 1, high: 2 };

  for (const layer of ['low', 'mid', 'high']) {
    const layerResult = target.byLayer[layer];
    layerResult.status = 'running';
    layerResult.requestedSpatialLayer = layerIndexes[layer];
    const request = await requestOverAuditedSocket(
      consumerPage,
      'e2eSelectConsumerLayer',
      { layer }
    );
    const selection = request.layerSelection ??
      request.spatialLayerDiagnostics ?? null;
    layerResult.layerSelection = selection;
    layerResult.preferredLayers = selection?.finalPreferredLayers ?? null;
    layerResult.currentLayers = selection?.finalCurrentLayers ?? null;
    layerResult.layerSelectionMs = selection?.transitionElapsedMs ?? null;
    layerResult.targetReachedAtEpochMs = selection?.targetReachedAt ?? null;
    layerResult.targetReachedAt = selection?.targetReachedAt
      ? new Date(selection.targetReachedAt).toISOString()
      : null;
    target.currentLayersSequence.push(
      ...(selection?.observedCurrentSpatialLayerSequence ?? []).map(sample => ({
        layer,
        ...sample
      }))
    );
    target.availableOutgoingBitrateSamples.push(
      ...(selection?.availableOutgoingBitrateSamples ?? []).map(sample => ({
        layer,
        ...sample
      }))
    );
    target.layerschangeSamples.push(
      ...(selection?.layerschangeSamples ?? []).map(sample => ({
        layer,
        ...sample
      }))
    );

    if (!request.e2eOk) {
      layerResult.error = request.e2eError ?? 'Sequential layer selection request failed.';
      if (
        selection?.stage === 'waitForConsumerSpatialLayer' &&
        selection.result === 'timed-out'
      ) {
        layerResult.status = 'constrained';
        layerResult.requestKeyFrameReached = false;
        constrainedLayers.push(layer);
        continue;
      }
      layerResult.status = 'failed';
      throw new Error(
        `Sequential ${layer} layer selection failed before PLI: ${layerResult.error}`
      );
    }

    ensure(request.layer === layer, `Sequential selector returned the wrong ${layer} layer.`);
    ensure(
      selection?.result === 'reached' &&
        selection.finalCurrentLayers?.spatialLayer === layerIndexes[layer],
      `Sequential ${layer} layer selection response lacked target-layer evidence.`
    );
    layerResult.status = 'passed';
    layerResult.targetReached = true;
  }

  target.lowReached = target.byLayer.low.status === 'passed';
  target.midReached = target.byLayer.mid.status === 'passed';
  target.highReached = target.byLayer.high.status === 'passed';
  target.constrainedLayers = constrainedLayers;
  target.classification = constrainedLayers.length > 0
    ? 'sequential layer selection was constrained before any PLI request'
    : 'all sequential layer targets reached before any PLI request';
  target.highSelectionDescription = target.byLayer.high.status === 'constrained'
    ? 'sequential high layer selection did not reach spatial layer 2'
    : target.byLayer.high.status === 'passed'
      ? 'sequential high layer selection reached spatial layer 2'
      : String(target.byLayer.high.error ?? target.byLayer.high.status);

  let finalUi = null;
  let mediaProgressWaitError = null;
  try {
    await waitForCondition(async () => {
      finalUi = await readUiSnapshot(consumerPage);
      return finalUi.rtpReceived > initialUi.rtpReceived &&
        finalUi.framesDecoded > initialUi.framesDecoded &&
        finalUi.receivedFps > 0 && finalUi.decodedFps > 0 &&
        finalUi.consumerState === 'receiving';
    }, 'Consumer media counters to advance after the sequential diagnostic', 5_000, 200);
  } catch (error) {
    mediaProgressWaitError = errorMessage(error);
  }
  finalUi ??= await readUiSnapshot(consumerPage);
  const finalHealth = await fetchJson(HEALTH_URL);
  target.mediaContinuity = {
    initial: {
      consumerState: initialUi.consumerState,
      rtpReceived: initialUi.rtpReceived,
      framesDecoded: initialUi.framesDecoded,
      receivedFps: initialUi.receivedFps,
      decodedFps: initialUi.decodedFps
    },
    final: {
      consumerState: finalUi.consumerState,
      rtpReceived: finalUi.rtpReceived,
      framesDecoded: finalUi.framesDecoded,
      receivedFps: finalUi.receivedFps,
      decodedFps: finalUi.decodedFps,
      injectedChunks: finalHealth.injectedChunks,
      producerPeerId: finalHealth.producerPeerId,
      serverAlive: !currentServerRecord.exit
    },
    rtpAdvanced: finalUi.rtpReceived > initialUi.rtpReceived,
    decodedFramesAdvanced: finalUi.framesDecoded > initialUi.framesDecoded,
    producerMediaAdvanced: finalHealth.injectedChunks > initialHealth.injectedChunks,
    mediaProgressWaitError,
    continued: finalUi.consumerState === 'receiving' &&
      finalUi.receivedFps > 0 && finalUi.decodedFps > 0 &&
      finalUi.rtpReceived > initialUi.rtpReceived &&
      finalUi.framesDecoded > initialUi.framesDecoded &&
      finalHealth.injectedChunks > initialHealth.injectedChunks &&
      !currentServerRecord.exit
  };
  ensure(
    target.mediaContinuity.continued,
    'Production media stopped during the sequential layer-selection diagnostic.'
  );

  target.status = constrainedLayers.length > 0 ? 'constrained' : 'passed';
}

async function runManualConsumerLayerPreference(consumer1, consumer2) {
  const target = report.production.manualConsumerPreference;
  await withStatus(target, async () => {
    const chooseLayer = async (consumer, layer, label) => {
      const before = await readUiSnapshot(consumer.page);
      ensure(
        before.role === 'consumer' && !before.consumerLayerControlsHidden,
        label + ' did not expose the Simulcast layer controls.'
      );
      const response = await requestOverAuditedSocket(
        consumer.page,
        'setConsumerLayer',
        { layer }
      );
      ensure(
        response.e2eOk === true &&
          response.layer === layer &&
          response.spatialLayer === ({ low: 0, mid: 1, high: 2 }[layer]),
        label + ' production setConsumerLayer request failed.'
      );
      return { before, response };
    };

    const consumer1Low = await chooseLayer(consumer1, 'low', 'Consumer 1 LOW');
    const consumer2High = await chooseLayer(consumer2, 'high', 'Consumer 2 HIGH');
    const healthAfterInitialChoices = await fetchJson(HEALTH_URL);
    const consumer1PeerId = consumer1Low.before.peerId;
    const consumer2PeerId = consumer2High.before.peerId;
    const consumer1Audit = findE2EConsumerAudit(healthAfterInitialChoices, consumer1PeerId);
    const consumer2Audit = findE2EConsumerAudit(healthAfterInitialChoices, consumer2PeerId);
    ensure(
      consumer1Audit.preferredLayers?.spatialLayer === 0,
      'Consumer 1 LOW preference did not map to preferred spatial layer 0.'
    );
    ensure(
      consumer2Audit.preferredLayers?.spatialLayer === 2,
      'Consumer 2 HIGH preference did not map to preferred spatial layer 2.'
    );

    const consumer1Mid = await chooseLayer(consumer1, 'mid', 'Consumer 1 MID');
    const healthAfterMidChoice = await fetchJson(HEALTH_URL);
    const consumer1MidAudit = findE2EConsumerAudit(healthAfterMidChoice, consumer1PeerId);
    const consumer2StillHighAudit = findE2EConsumerAudit(healthAfterMidChoice, consumer2PeerId);
    ensure(
      consumer1MidAudit.preferredLayers?.spatialLayer === 1,
      'Consumer 1 MID preference did not map to preferred spatial layer 1.'
    );
    ensure(
      consumer2StillHighAudit.preferredLayers?.spatialLayer === 2,
      'Consumer 2 preference changed when Consumer 1 selected MID.'
    );

    const mediaDiagnostics = await requestOverAuditedSocket(
      consumer1.page,
      'e2eCaptureMediaDiagnostics',
      {}
    );
    ensure(
      mediaDiagnostics.e2eOk === true &&
        Array.isArray(mediaDiagnostics.consumers) &&
        mediaDiagnostics.consumers.length === 2,
      'The E2E-only on-demand media diagnostics did not snapshot both active SFU Consumers.'
    );
    const checkStatsResult = (section, label) => {
      ensure(
        Array.isArray(section?.stats) || typeof section?.error === 'string',
        `${label} diagnostics must return stats or an explicit error.`
      );
    };
    ensure(
      Array.isArray(mediaDiagnostics.producer?.stats) ||
        typeof mediaDiagnostics.producer?.statsError === 'string',
      'Producer diagnostics must return stats or an explicit error.'
    );
    for (const consumerSnapshot of mediaDiagnostics.consumers) {
      ensure(
        consumerSnapshot.peerId === consumer1PeerId ||
          consumerSnapshot.peerId === consumer2PeerId,
        'SFU diagnostics returned a Consumer outside the active two-peer set.'
      );
      checkStatsResult(consumerSnapshot.consumerStats, 'Consumer');
      checkStatsResult(consumerSnapshot.transportStats, 'WebRtcTransport');
    }

    Object.assign(target, {
      producerSimulcastEnabled: healthAfterMidChoice.simulcastEnabled,
      consumer1: {
        peerId: consumer1PeerId,
        low: {
          preferredSpatialLayer: consumer1Audit.preferredLayers?.spatialLayer ?? null,
          response: consumer1Low.response
        },
        mid: {
          preferredSpatialLayer: consumer1MidAudit.preferredLayers?.spatialLayer ?? null,
          response: consumer1Mid.response
        },
        currentLayersAfterMid: consumer1MidAudit.currentLayers
      },
      anomalyOnlySfuDiagnostics: {
        status: 'passed',
        capturedAt: mediaDiagnostics.capturedAt,
        producerStatsRows: mediaDiagnostics.producer?.stats?.length ?? null,
        producerStatsError: mediaDiagnostics.producer?.statsError ?? null,
        consumers: mediaDiagnostics.consumers.map(item => ({
          peerId: item.peerId,
          currentLayers: item.currentLayers,
          consumerStatsRows: item.consumerStats?.stats?.length ?? null,
          consumerStatsError: item.consumerStats?.error ?? null,
          transportStatsRows: item.transportStats?.stats?.length ?? null,
          transportStatsError: item.transportStats?.error ?? null
        }))
      },
      consumer2: {
        peerId: consumer2PeerId,
        high: {
          preferredSpatialLayer: consumer2Audit.preferredLayers?.spatialLayer ?? null,
          response: consumer2High.response
        },
        currentLayersAfterHigh: consumer2StillHighAudit.currentLayers,
        preferredSpatialLayerAfterConsumer1Mid:
          consumer2StillHighAudit.preferredLayers?.spatialLayer ?? null
      },
      currentLayersAreNotAcceptanceCondition: true,
      status: 'passed'
    });
  });
}

async function waitForStableConsumerSpatialLayer(peerId, targetLayer, timeoutMs = 20_000) {
  const targetSpatialLayer = { low: 0, mid: 1, high: 2 }[targetLayer];
  const startedAt = Date.now();
  const observations = [];
  let consecutiveMatches = 0;
  let reachedAt = null;
  while (Date.now() - startedAt < timeoutMs) {
    checkInterrupted();
    const health = await fetchJson(HEALTH_URL);
    const audit = findE2EConsumerAudit(health, peerId);
    const currentSpatialLayer = audit.currentLayers?.spatialLayer ?? null;
    const observation = {
      sampledAt: new Date().toISOString(),
      currentLayers: audit.currentLayers,
      preferredLayers: audit.preferredLayers,
      consumers: health.consumers,
      simulcastEnabled: health.simulcastEnabled
    };
    observations.push(observation);
    if (currentSpatialLayer === targetSpatialLayer) {
      consecutiveMatches += 1;
      reachedAt ??= observation.sampledAt;
      if (consecutiveMatches >= 3) {
        return {
          status: 'reached',
          targetLayer,
          targetSpatialLayer,
          stableSamples: observations.slice(-3),
          observations,
          elapsedMs: Date.now() - startedAt
        };
      }
    } else {
      consecutiveMatches = 0;
      reachedAt = null;
    }
    await wait(400);
  }
  return {
    status: 'not-reached',
    targetLayer,
    targetSpatialLayer,
    lastObservedCurrentLayers: observations.at(-1)?.currentLayers ?? null,
    observations,
    elapsedMs: Date.now() - startedAt
  };
}

async function runConsumerInboundLayerSequence(consumer) {
  const target = report.production.consumerInboundRtpAudit.simulcastOnLayerSequence;
  await withStatus(target, async () => {
    const initialHealth = await assertConsumerCount(1);
    ensure(initialHealth.simulcastEnabled === true,
      'Consumer layer-sequence audit requires the Simulcast ON Producer.');
    const ui = await readUiSnapshot(consumer.page);
    ensure(typeof ui.peerId === 'string' && ui.peerId.length > 0,
      'Consumer layer-sequence audit could not read the Consumer peer ID.');
    const layers = {};
    for (const layer of ['low', 'mid', 'high']) {
      const startTimeMs = Date.now();
      let response;
      let stableLayer;
      let requestError = null;
      try {
        response = await requestOverAuditedSocket(
          consumer.page,
          'setConsumerLayer',
          { layer }
        );
        if (response.e2eOk) {
          stableLayer = await waitForStableConsumerSpatialLayer(ui.peerId, layer);
        } else {
          stableLayer = {
            status: 'not-reached',
            targetLayer: layer,
            reason: response.e2eError ?? 'server-rejected-layer-selection'
          };
        }
      } catch (error) {
        if (interruptedBy) throw error;
        requestError = errorMessage(error);
        stableLayer = { status: 'not-reached', targetLayer: layer, reason: requestError };
      }

      const media = await measureConsumer(
        consumer.page,
        'Inbound RTP audit ' + layer.toUpperCase(),
        3000
      );
      const endTimeMs = Date.now();
      const [health, context] = await Promise.all([
        assertConsumerCount(1),
        readConsumerDiagnosticContext(consumer.page)
      ]);
      const scenario = buildConsumerInboundScenario(
        context.rtcStatsProbe,
        context,
        startTimeMs,
        endTimeMs
      );
      scenario.simulcast = {
        enabled: health.simulcastEnabled,
        requestedLayer: layer,
        requestedSpatialLayer: { low: 0, mid: 1, high: 2 }[layer],
        stableCurrentLayer: stableLayer.status === 'reached'
          ? stableLayer.targetSpatialLayer
          : stableLayer.lastObservedCurrentLayers?.spatialLayer ?? null,
        selection: stableLayer
      };
      layers[layer] = {
        status: scenario.status === 'captured' && stableLayer.status === 'reached'
          ? 'captured'
          : 'constrained',
        signalingResponse: response ?? null,
        requestError,
        stableLayer,
        media,
        scenario
      };
    }
    const layerStatsDump = await downloadStatsDumpFromPage(
      consumer.page,
      'consumer-inbound-rtp-layer-sequence'
    );
    const [layerProbe, layerUi] = await Promise.all([
      readConsumerRtcStatsProbe(consumer.page),
      readUiSnapshot(consumer.page)
    ]);
    const layerRuntimeAudit = verifyConsumerRuntimeStats(
      layerStatsDump,
      layerProbe,
      layerUi
    ).runtimeAudit;
    const layerSamples = layerStatsDump.dump.consumerStats;
    for (const layer of ['low', 'mid', 'high']) {
      const window = layers[layer].scenario.window;
      const start = Date.parse(window.startedAt);
      const end = Date.parse(window.endedAt);
      layers[layer].consumerStats = layerSamples
        .filter(sample => Date.parse(sample.timestamp) >= start && Date.parse(sample.timestamp) <= end)
        .map(sample => ({
          timestamp: sample.timestamp,
          elapsedMs: sample.elapsedMs,
          receivedFps: sample.receivedFps,
          decodedFps: sample.decodedFps,
          receivedMediaBitrateKbps: sample.receivedMediaBitrateKbps,
          packetsReceived: sample.packetsReceived,
          packetsLost: sample.packetsLost,
          framesReceived: sample.framesReceived,
          framesDecoded: sample.framesDecoded,
          bytesReceived: sample.bytesReceived
        }));
    }
    target.consumerStats = {
      filename: layerStatsDump.filename,
      sampleCount: layerSamples.length,
      runtimeAudit: layerRuntimeAudit,
      independentRawReportComparison: layerRuntimeAudit.status === 'passed'
    };
    target.consumerPeerId = ui.peerId;
    target.consumerCount = initialHealth.consumers;
    target.producerSimulcastEnabled = initialHealth.simulcastEnabled;
    target.sequence = ['low', 'mid', 'high'];
    target.byLayer = layers;
    target.status = Object.values(layers).every(layer => layer.status === 'captured')
      ? 'passed'
      : 'constrained';
  });
}

async function runSimulcastOffE2E() {
  const target = report.production.offMode;
  target.cycle = createProducerCycleTrace();
  await withStatus(target, async () => {
    await assertPortFree(SIGNALING_PORT);
    await assertPortFree(DEV_PORT);
    const server = startNpmScript(
      'server-prod-off',
      'server:prod',
      [],
      { SFU_DEBUG: '0', SFU_E2E_MEDIA: '1', NODE_ENV: 'production' }
    );
    const preview = startNpmScript(
      'preview-prod-off',
      'preview',
      [],
      { SFU_DEBUG: '0', NODE_ENV: 'production' }
    );
    currentServerRecord = server;
    let producer = null;
    let consumer = null;
    try {
      await waitForHealth(server, health => health.ok === true, 'production OFF /health');
      producer = await withStatus(target.producer, () =>
        startProducerCycle(
          'production',
          PRODUCTION_APP_URL,
          'prod-off-cycle',
          target.cycle,
          { simulcastEnabled: false }
        )
      );
      await syncWebSocketAudit(producer.producerPage, producer.producerAudit);
      const producerHealth = await fetchJson(HEALTH_URL);
      const producerLayers = new Set(producer.producerAudit.encodedChunkLayers ?? []);
      ensure(
        producerHealth.simulcastEnabled === false &&
          JSON.stringify(producerHealth.activeLayers) === JSON.stringify(['high']) &&
          producerHealth.producerType === 'simple',
        'Simulcast OFF health did not report simple HIGH-only Producer state.'
      );
      ensure(
        producer.producerAudit.videoEncoderConstructors === 1,
        'Simulcast OFF created more than one WebCodecs VideoEncoder.'
      );
      ensure(
        producerLayers.size === 1 && producerLayers.has('high'),
        'Simulcast OFF Producer emitted a non-HIGH encoded chunk.'
      );
      const offInputFps = producerHealth.inputFps ?? {};
      ensure(
        Number(offInputFps.source) > 0 && Number(offInputFps.high) > 0 &&
          Number(offInputFps.low) === 0 && Number(offInputFps.mid) === 0,
        'Simulcast OFF input FPS did not remain HIGH-only.'
      );

      await withStatus(target.statsDump, async () => {
        const download = await downloadStatsDumpFromPage(
          producer.producerPage,
          'production-producer-off',
          target.statsDump
        );
        Object.assign(target.statsDump, verifyProducerStatsDump(download, false));
      });

      const inboundRtpWindowStartMs = Date.now();
      consumer = await withStatus(target.consumer, async () => {
        const page = await startConsumerPage(
          'production',
          PRODUCTION_APP_URL,
          'prod-off-consumer-1'
        );
        const sample = await measureConsumer(page.page, 'Simulcast OFF Consumer');
        const inboundRtpWindowEndMs = Date.now();
        const consumerHealth = await assertConsumerCount(1);
        const ui = await readUiSnapshot(page.page);
        ensure(
          ui.consumerLayerControlsHidden === true,
          'Simulcast OFF Consumer still exposed manual layer controls.'
        );
        Object.assign(target.consumer, {
          ...sample,
          ui,
          controlsHidden: ui.consumerLayerControlsHidden
        });
        page.inboundRtpWindow = {
          startTimeMs: inboundRtpWindowStartMs,
          endTimeMs: inboundRtpWindowEndMs,
          consumerCount: consumerHealth.consumers,
          producerSimulcastEnabled: consumerHealth.simulcastEnabled
        };
        return page;
      });

      const offContext = await readConsumerDiagnosticContext(consumer.page);
      const offScenario = buildConsumerInboundScenario(
        offContext.rtcStatsProbe,
        offContext,
        consumer.inboundRtpWindow.startTimeMs,
        consumer.inboundRtpWindow.endTimeMs
      );
      offScenario.simulcast = {
        enabled: consumer.inboundRtpWindow.producerSimulcastEnabled,
        expectedLayer: 'high',
        consumerCount: consumer.inboundRtpWindow.consumerCount,
        producerType: producerHealth.producerType
      };
      report.production.consumerInboundRtpAudit.simulcastOff = offScenario;

      await withStatus(target.layerControlRejection, async () => {
        const response = await requestOverAuditedSocket(
          consumer.page,
          'setConsumerLayer',
          { layer: 'high' }
        );
        ensure(
          response.e2eOk === false &&
            /active simulcast Producer/i.test(String(response.e2eError)),
          'Simulcast OFF setConsumerLayer request was not rejected by the server.'
        );
        Object.assign(target.layerControlRejection, {
          response,
          rejected: true
        });
      });

      Object.assign(target, {
        health: producerHealth,
        activeEncoderCount: producer.producerAudit.videoEncoderConstructors,
        emittedLayers: [...producerLayers],
        inputFps: offInputFps,
        producerType: producerHealth.producerType,
        activeLayers: producerHealth.activeLayers
      });
    } finally {
      await Promise.all([...runtime.pages].map(page => page.close().catch(() => {})));
      await stopProcess(preview);
      await stopProcess(server);
      currentServerRecord = null;
      target.cleanup = {
        status: 'passed',
        serverStopped: Boolean(server.stopped || server.exit),
        previewStopped: Boolean(preview.stopped || preview.exit),
        pagesClosed: runtime.pages.size === 0
      };
    }
  });
}

async function runProductionE2E() {
  await withStatus(report.production, async () => {
    await withStatus(report.production.e2eControlGuard, async () => {
      Object.assign(
        report.production.e2eControlGuard,
        await verifyE2eControlDisabledByDefault()
      );
    });

    await assertPortFree(SIGNALING_PORT);
    await assertPortFree(DEV_PORT);
    const server = startNpmScript(
      'server-prod',
      'server:prod',
      [],
      { SFU_DEBUG: '0', SFU_E2E_MEDIA: '1', NODE_ENV: 'production' }
    );
    const preview = startNpmScript(
      'preview-prod',
      'preview',
      [],
      { SFU_DEBUG: '0', NODE_ENV: 'production' }
    );
    currentServerRecord = server;
    await waitForHealth(server, health => health.ok === true, 'production /health');
    const endpoint = await verifyBrowserEndpoint(
      PRODUCTION_APP_URL,
      'production',
      'HTTPS production Vite preview',
      '/assets/index-',
      preview
    );
    Object.assign(report.production.endpoint, endpoint);

    await withStatus(report.production.statsVisualizer, async () => {
      Object.assign(
        report.production.statsVisualizer,
        await verifyStatsVisualizer(PRODUCTION_APP_URL)
      );
    });

    const producer = await withStatus(report.production.producer, () =>
      startProducerCycle(
        'production',
        PRODUCTION_APP_URL,
        'prod-cycle-1',
        report.production.producerCycles.initial
      )
    );
    Object.assign(report.production.producer, {
      ...producer.producerMetrics,
      localLogHidden: producer.initialLog.hidden,
      patternTitle: PATTERN_TITLE,
      patternFrameProgress: producer.patternFrames
    });

    await withStatus(report.production.producerStatsDump, async () => {
      const download = await downloadStatsDumpFromPage(
        producer.producerPage,
        'production-producer',
        report.production.producerStatsDump
      );
      Object.assign(
        report.production.producerStatsDump,
        verifyProducerStatsDump(download, true)
      );
    });

    if (STOP_AFTER_PLI) {
      await withStatus(report.production.pliFir, () =>
        runPliFirLayerTests(producer.producerPage, producer.producerAudit)
      );
      report.execution.completedScope =
        'production Producer setup and fresh-Consumer low/mid/high PLI correctness';
      report.execution.skippedChecks = [...TARGETED_SKIPPED_CHECKS];
      await Promise.all([...runtime.pages].map(page => page.close().catch(() => {})));
      await stopProcess(preview);
      await stopProcess(server);
      currentServerRecord = null;
      report.production.serverExitedAfterTests = true;
      report.production.previewExitedAfterTests = true;
      return;
    }

    const consumer1InboundRtpWindowStartMs = Date.now();
    const consumer1 = await withStatus(report.production.consumer1, async () => {
      const page = await startConsumerPage(
        'production',
        PRODUCTION_APP_URL,
        'prod-consumer-1'
      );
      const sample = await measureConsumer(page.page, 'Consumer 1');
      const windowEndMs = Date.now();
      const health = await assertConsumerCount(1);
      page.inboundRtpWindow = {
        startTimeMs: consumer1InboundRtpWindowStartMs,
        endTimeMs: windowEndMs,
        consumerCountAtEnd: health.consumers,
        producerSimulcastEnabled: health.simulcastEnabled
      };
      await syncWebSocketAudit(page.page, page.audit);
      Object.assign(report.production.consumer1, {
        ...sample,
        localLogHidden: await page.page.$eval('#log', element => element.hidden)
      });
      return page;
    });

    await withStatus(report.production.consumerStatsDump, async () => {
      const download = await downloadStatsDumpFromPage(
        consumer1.page,
        'production-consumer',
        report.production.consumerStatsDump
      );
      const context = await readConsumerDiagnosticContext(consumer1.page);
      const ui = await readUiSnapshot(consumer1.page);
      Object.assign(
        report.production.consumerStatsDump,
        verifyConsumerStatsDump(download),
        verifyConsumerRuntimeStats(download, context.rtcStatsProbe, ui)
      );
      const initialHealth = await fetchJson(HEALTH_URL);
      const scenario = buildConsumerInboundScenario(
        context.rtcStatsProbe,
        context,
        consumer1.inboundRtpWindow.startTimeMs,
        consumer1.inboundRtpWindow.endTimeMs
      );
      scenario.simulcast = {
        enabled: consumer1.inboundRtpWindow.producerSimulcastEnabled,
        layerChangeRequested: false,
        consumerCount: consumer1.inboundRtpWindow.consumerCountAtEnd,
        noLayerSelectionRequestsObserved:
          scenario.getStats.layerSelectionRequestsInWindow.length === 0
      };
      ensure(
        scenario.simulcast.enabled === true &&
          scenario.simulcast.consumerCount === 1 &&
          scenario.simulcast.noLayerSelectionRequestsObserved,
        'Consumer 1 no-layer-change audit window did not remain a single Simulcast Consumer.'
      );
      scenario.simulcast.healthAtDiagnosticCapture = {
        consumers: initialHealth.consumers,
        simulcastEnabled: initialHealth.simulcastEnabled
      };
      report.production.consumerInboundRtpAudit.simulcastOnNoLayerChange = scenario;
    });

    await runConsumerInboundLayerSequence(consumer1);

    const consumer2 = await withStatus(report.production.consumer2, async () => {
      const page = await startConsumerPage(
        'production',
        PRODUCTION_APP_URL,
        'prod-consumer-2'
      );
      const sample = await measureConsumer(page.page, 'Consumer 2');
      await syncWebSocketAudit(page.page, page.audit);
      Object.assign(report.production.consumer2, {
        ...sample,
        localLogHidden: await page.page.$eval('#log', element => element.hidden)
      });
      return page;
    });

    await withStatus(report.production.consumerStatsFailureRecovery, async () => {
      const recovery = await verifyInjectedConsumerStatsFailure(consumer2.page);
      Object.assign(report.production.consumerStatsFailureRecovery, recovery);
    });

    await runManualConsumerLayerPreference(consumer1, consumer2);

    await withStatus(report.production.multiConsumer, async () => {
      const health = await assertConsumerCount(2);
      const startTimeMs = Date.now();
      const [consumer1Media, consumer2Media] = await Promise.all([
        measureConsumer(consumer1.page, 'Multi-consumer audit Consumer 1', 3000),
        measureConsumer(consumer2.page, 'Multi-consumer audit Consumer 2', 3000)
      ]);
      const endTimeMs = Date.now();
      const healthAfter = await assertConsumerCount(2);
      const [consumer1Context, consumer2Context] = await Promise.all([
        readConsumerDiagnosticContext(consumer1.page),
        readConsumerDiagnosticContext(consumer2.page)
      ]);
      const consumer1Scenario = buildConsumerInboundScenario(
        consumer1Context.rtcStatsProbe,
        consumer1Context,
        startTimeMs,
        endTimeMs
      );
      const consumer2Scenario = buildConsumerInboundScenario(
        consumer2Context.rtcStatsProbe,
        consumer2Context,
        startTimeMs,
        endTimeMs
      );
      const consumer1Id = consumer1Scenario.consumer.consumerId;
      const consumer2Id = consumer2Scenario.consumer.consumerId;
      const consumer1TrackId = consumer1Scenario.consumer.trackId;
      const consumer2TrackId = consumer2Scenario.consumer.trackId;
      const consumer1PeerId = consumer1Scenario.consumer.peerId;
      const consumer2PeerId = consumer2Scenario.consumer.peerId;
      const consumer1TracksMatch = consumer1Scenario.statTimelines.some(item =>
        item.trackIdentifierMatchesConsumerTrack === true
      );
      const consumer2TracksMatch = consumer2Scenario.statTimelines.some(item =>
        item.trackIdentifierMatchesConsumerTrack === true
      );
      const bothScenariosCaptured = consumer1Scenario.status === 'captured' &&
        consumer2Scenario.status === 'captured';
      const identitiesDistinct = Boolean(consumer1Id && consumer2Id &&
        consumer1Id !== consumer2Id && consumer1PeerId && consumer2PeerId &&
        consumer1PeerId !== consumer2PeerId);
      const trackIsolationVerified = Boolean(consumer1TrackId && consumer2TrackId &&
        consumer1TrackId !== consumer2TrackId && consumer1TracksMatch && consumer2TracksMatch);
      report.production.consumerInboundRtpAudit.multiConsumerIsolation = {
        status: bothScenariosCaptured && identitiesDistinct && trackIsolationVerified
          ? 'passed'
          : 'constrained',
        window: {
          startedAt: new Date(startTimeMs).toISOString(),
          endedAt: new Date(endTimeMs).toISOString(),
          durationMs: endTimeMs - startTimeMs,
          healthConsumerCountAtStart: health.consumers,
          healthConsumerCountAtEnd: healthAfter.consumers
        },
        separatePageAndPeerContexts: true,
        consumerIdsDistinct: Boolean(consumer1Id && consumer2Id && consumer1Id !== consumer2Id),
        peerIdsDistinct: Boolean(consumer1PeerId && consumer2PeerId &&
          consumer1PeerId !== consumer2PeerId),
        trackIdsDistinct: Boolean(consumer1TrackId && consumer2TrackId &&
          consumer1TrackId !== consumer2TrackId),
        eachStatsTimelineMatchesOwnRemoteTrack: {
          consumer1: consumer1TracksMatch,
          consumer2: consumer2TracksMatch
        },
        consumer1: { movingMedia: consumer1Media, scenario: consumer1Scenario },
        consumer2: { movingMedia: consumer2Media, scenario: consumer2Scenario },
        note: 'RTCStats IDs are scoped to each PeerConnection; identical text IDs across different pages would not establish cross-page mixing.'
      };
      Object.assign(report.production.multiConsumer, {
        expectedConsumers: 2,
        observedConsumers: healthAfter.consumers,
        producerPeerId: health.producerPeerId,
        producerId: health.producerId,
        inboundRtpAuditStatus:
          report.production.consumerInboundRtpAudit.multiConsumerIsolation.status
      });
    });

    await withStatus(report.production.consumerIndependence, async () => {
      const before = await readUiSnapshot(consumer2.page);
      await consumer1.page.close();
      const oneConsumerHealth = await assertConsumerCount(1);
      await wait(5000);
      const after = await readUiSnapshot(consumer2.page);
      const frameHash = await captureVideoHash(consumer2.page);
      ensure(after.rtpReceived > before.rtpReceived, 'Consumer 2 RTP stopped after Consumer 1 closed.');
      ensure(after.framesDecoded > before.framesDecoded, 'Consumer 2 decode stopped after Consumer 1 closed.');
      ensure(after.receivedFps > 0 && after.decodedFps > 0, 'Consumer 2 FPS stopped after Consumer 1 closed.');
      ensure(after.consumerState === 'receiving', 'Consumer 2 left receiving state after Consumer 1 closed.');
      Object.assign(report.production.consumerIndependence, {
        closedConsumer: 'Consumer 1',
        consumerCountAfterClose: oneConsumerHealth.consumers,
        consumer2RtpDelta: after.rtpReceived - before.rtpReceived,
        consumer2DecodedDelta: after.framesDecoded - before.framesDecoded,
        consumer2ReceivedFps: after.receivedFps,
        consumer2DecodedFps: after.decodedFps,
        consumer2FrameHash: frameHash.sha256
      });
    });

    await withStatus(report.production.pliFir, () =>
      runPliFirLayerTests(producer.producerPage, producer.producerAudit)
    );

    await withStatus(report.production.layerSelectionDiagnostic, () =>
      runSequentialLayerSelectionDiagnostic(consumer2.page)
    );

    await withStatus(report.production.diagnostics, async () => {
      // Consumer 1 is already closed after the independence check. Its audit and
      // saved DOM assertion remain useful, but never query its closed page.
      await Promise.all([
        syncWebSocketAudit(producer.producerPage, producer.producerAudit),
        syncWebSocketAudit(consumer2.page, consumer2.audit)
      ]);
      const audits = [producer.producerAudit, consumer1.audit, consumer2.audit];
      const [producerLogHidden, consumer2LogHidden] = await Promise.all([
        producer.producerPage.$eval('#log', element => element.hidden),
        consumer2.page.$eval('#log', element => element.hidden)
      ]);
      const consumer1LogHidden = report.production.consumer1.localLogHidden;
      const logHidden = [producerLogHidden, consumer1LogHidden, consumer2LogHidden];
      const clientDebugCount = audits.reduce((total, audit) => total + audit.consoleDebug.length, 0);
      const forbiddenMessages = audits.flatMap(diagnosticEvents);
      const serverDebug = processLogHasDebug(server);
      const roomStateEventCounts = {
        producer: producer.producerAudit.websocketEventCounts.roomState ?? 0,
        consumer1: consumer1.audit.websocketEventCounts.roomState ?? 0,
        consumer2: consumer2.audit.websocketEventCounts.roomState ?? 0
      };
      const healthFirst = await fetchJson(HEALTH_URL);
      const consumerFirst = await readUiSnapshot(consumer2.page);
      await wait(2000);
      const healthSecond = await fetchJson(HEALTH_URL);
      const consumerSecond = await readUiSnapshot(consumer2.page);
      const inputFps = healthSecond.inputFps ?? {};
      ensure(logHidden.every(Boolean), 'Production #log must remain hidden for all Consumer/Producer pages.');
      ensure(clientDebugCount === 0, 'Production browser emitted console.debug messages.');
      ensure(forbiddenMessages.length === 0, 'Production sent debug-only diagnostic WebSocket events.');
      ensure(!serverDebug, 'Production server emitted debug logs.');
      ensure(!server.exit, 'Production server exited during connected media diagnostics.');
      ensure(
        roomStateEventCounts.producer > 0 && roomStateEventCounts.consumer2 > 0,
        'Production roomState events were not observed for the active Producer and Consumer.'
      );
      ensure(
        healthSecond.ok === true &&
          Boolean(healthSecond.producerPeerId) &&
          Boolean(healthSecond.producerId) &&
          healthSecond.consumers === 1 &&
          healthSecond.injectedChunks > healthFirst.injectedChunks &&
          healthSecond.injectedPackets > healthFirst.injectedPackets,
        'Production health/media counters did not remain active after Consumer 1 closed.'
      );
      ensure(
        Number(inputFps.source) > 0 &&
          Number(inputFps.low) > 0 &&
          Number(inputFps.mid) > 0 &&
          Number(inputFps.high) > 0,
        'Production SFU input FPS stopped during diagnostics.'
      );
      ensure(
        consumerSecond.rtpReceived > consumerFirst.rtpReceived &&
          consumerSecond.framesDecoded > consumerFirst.framesDecoded &&
          consumerSecond.receivedFps > 0 &&
          consumerSecond.decodedFps > 0,
        'Production Consumer 2 RTP/decode/FPS stopped during diagnostics.'
      );
      Object.assign(report.production.diagnostics, {
        producerLogHidden,
        consumer1LogHidden,
        consumer2LogHidden,
        consumerLogHidden: consumer2LogHidden,
        clientConsoleDebugCount: clientDebugCount,
        diagnosticWebSocketEvents: forbiddenMessages,
        serverDebugLogObserved: serverDebug,
        roomStateEventCounts,
        healthFirst,
        healthSecond,
        consumer2First: consumerFirst,
        consumer2Second: consumerSecond,
        serverLogTail: server.tail.slice(-6000),
        sourceGuards: report.staticValidation.sourceGuards.checks
      });
    });

    await withStatus(report.production.capturedSourceCleanup, async () => {
      report.production.capturedSourceCleanup.cleanupSamples = [];
      const cleanup = await runCleanupSeries({
        closeActionName: 'close captured WEBRTC E2E TEST source tab',
        closeAction: () => producer.patternPage.close(),
        onSample: sample => {
          report.production.capturedSourceCleanup.cleanupSamples.push(sample);
        },
        producerPage: producer.producerPage,
        consumerPage: consumer2.page,
        requireTrackEnded: true,
        expectedConsumerCountAfterClose: 1
      });
      try {
        await syncWebSocketAudit(producer.producerPage, producer.producerAudit);
        report.production.producerCycles.initial.getDisplayMediaAudit =
          producer.producerAudit.displayCapture;
      } catch (error) {
        report.production.producerCycles.initial.getDisplayMediaAuditSyncError =
          errorMessage(error);
      }
      Object.assign(report.production.capturedSourceCleanup, {
        ...cleanup
      });
      ensure(cleanup.status === 'passed', 'Captured-source cleanup failed.');
      ensure(!server.exit, 'Production server exited during captured-source cleanup.');
    });

    await producer.producerPage.close().catch(() => {});
    await consumer2.page.close().catch(() => {});
    const roomAfterCapturedSourcePagesClosed = await waitForEmptyRoom(
      server,
      'room empty after captured-source cleanup pages close'
    );
    Object.assign(report.production.capturedSourceCleanup, {
      roomEmptyAfterPagesClosed: true,
      roomHealthAfterPagesClosed: roomAfterCapturedSourcePagesClosed
    });

    await withStatus(report.production.producerPageCloseCleanup, async () => {
      const pageCloseReport = report.production.producerPageCloseCleanup;
      pageCloseReport.phase = 'producer-cycle-setup';
      const secondProducer = await startProducerCycle(
        'production',
        PRODUCTION_APP_URL,
        'prod-cycle-page-close',
        report.production.producerCycles.pageClose
      );
      ensure(
        report.production.producerCycles.pageClose.checkpoints.cycleReady.status === 'passed',
        'Page-close cleanup requires the second Producer cycle to reach cycleReady.'
      );
      const metrics = secondProducer.producerMetrics;
      pageCloseReport.producerMetrics = metrics;
      pageCloseReport.phase = 'producer-page-close';
      pageCloseReport.cleanupSamples = [];
      const cleanup = await runCleanupSeries({
        closeActionName: 'close producer application page',
        beforeCloseAction: async health => {
          pageCloseReport.phase = 'pre-close-snapshot';
          pageCloseReport.preCloseSnapshot = {
            capturedAt: new Date().toISOString(),
            ui: await readUiSnapshot(secondProducer.producerPage),
            health
          };
        },
        closeAction: async () => {
          pageCloseReport.phase = 'close-action';
          pageCloseReport.closeActionExecuted = true;
          await secondProducer.producerPage.close();
        },
        onSample: sample => {
          pageCloseReport.cleanupSamples.push(sample);
          if (sample.offsetMs > 0) {
            pageCloseReport.phase = 'cleanup-sampling';
          }
        },
        producerPage: null,
        consumerPage: null,
        requireTrackEnded: false,
        expectedConsumerCountAfterClose: 0
      });
      Object.assign(pageCloseReport, { ...cleanup, producerMetrics: metrics });
      await secondProducer.patternPage.close().catch(() => {});
      pageCloseReport.phase = 'empty-room-check';
      let emptyRoomHealth;
      try {
        emptyRoomHealth = await waitForEmptyRoom(
          server,
          'room empty after Producer page-close cleanup'
        );
      } catch (error) {
        pageCloseReport.emptyRoomError = errorMessage(error);
        throw error;
      }
      pageCloseReport.emptyRoomHealth = emptyRoomHealth;
      ensure(cleanup.status === 'passed', 'Producer-page-close cleanup failed.');
      ensure(!server.exit, 'Production server exited during producer-page-close cleanup.');
      pageCloseReport.phase = 'passed';
    });

    await withStatus(report.production.reloadCleanup, async () => {
      const reloadReport = report.production.reloadCleanup;
      reloadReport.phase = 'producer-cycle-setup';
      const reloadProducer = await startProducerCycle(
        'production',
        PRODUCTION_APP_URL,
        'prod-cycle-reload',
        report.production.producerCycles.reload
      );
      ensure(
        report.production.producerCycles.reload.checkpoints.cycleReady.status === 'passed',
        'Reload cleanup requires the Producer cycle to reach cycleReady.'
      );
      reloadReport.producerMetrics = reloadProducer.producerMetrics;
      reloadReport.phase = 'reload-action';
      reloadReport.cleanupSamples = [];
      const cleanup = await runCleanupSeries({
        closeActionName: 'reload producer application page without pressing Start',
        beforeCloseAction: async health => {
          reloadReport.phase = 'pre-reload-snapshot';
          reloadReport.preReloadSnapshot = {
            capturedAt: new Date().toISOString(),
            ui: await readUiSnapshot(reloadProducer.producerPage),
            health
          };
        },
        closeAction: async () => {
          reloadReport.phase = 'reload-action';
          reloadReport.reloadActionExecuted = true;
          await reloadProducer.producerPage.reload({ waitUntil: 'domcontentloaded' });
          await reloadProducer.producerPage.waitForSelector('#startButton', {
            visible: true,
            timeout: PAGE_TIMEOUT_MS
          });
        },
        onSample: sample => {
          reloadReport.cleanupSamples.push(sample);
          if (sample.offsetMs > 0) {
            reloadReport.phase = 'cleanup-sampling';
          }
        },
        producerPage: null,
        consumerPage: null,
        requireTrackEnded: false,
        expectedConsumerCountAfterClose: 0
      });
      Object.assign(reloadReport, { ...cleanup, producerMetrics: reloadProducer.producerMetrics });
      await reloadProducer.patternPage.close().catch(() => {});
      const pageAfterReload = await readUiSnapshot(reloadProducer.producerPage);
      reloadReport.phase = 'empty-room-check';
      let emptyRoomHealth;
      try {
        emptyRoomHealth = await waitForEmptyRoom(
          server,
          'room empty after Producer reload without Start'
        );
      } catch (error) {
        reloadReport.emptyRoomError = errorMessage(error);
        throw error;
      }
      Object.assign(reloadReport, { pageAfterReload, emptyRoomHealth });
      ensure(cleanup.status === 'passed', 'Producer reload cleanup failed.');
      ensure(
        pageAfterReload.role === 'idle' && pageAfterReload.socketState === 'idle',
        'Reloaded Producer page should remain idle; Start must not be pressed automatically.'
      );
      ensure(!server.exit, 'Production server exited during Producer reload cleanup.');
      reloadReport.phase = 'passed';
    });

    await Promise.all([...runtime.pages].map(page => page.close().catch(() => {})));
    await stopProcess(preview);
    await stopProcess(server);
    currentServerRecord = null;
    report.production.serverExitedAfterTests = true;
    report.production.previewExitedAfterTests = true;
  });
}

async function verifyBrowserEndpoint(url, mode, label, expectedClientMarker, record = null) {
  const { page } = await newPage(label, mode);
  try {
    const response = await waitForCondition(async () => {
      if (record?.spawnError) {
        throw new Error(record.label + ' failed to start: ' + errorMessage(record.spawnError));
      }
      if (record?.exit) {
        throw new Error(record.label + ' exited before endpoint readiness. Log tail:\n' + record.tail);
      }
      try {
        const candidate = await page.goto(url + '/', { waitUntil: 'networkidle2' });
        return candidate?.status() === 200 ? candidate : null;
      } catch {
        return null;
      }
    }, label + ' HTTPS endpoint', SERVER_TIMEOUT_MS, RUNTIME_POLL_INTERVAL_MS);
    ensure(response?.status() === 200, label + ' did not return HTTP 200 over HTTPS.');
    const html = await page.content();
    ensure(
      html.includes(expectedClientMarker) && html.includes('id="startButton"'),
      label + ' served an unexpected client document.'
    );
    const browserEvidence = await page.evaluate(async () => {
      const healthResponse = await fetch('/health', { cache: 'no-store' });
      let health = null;
      try {
        health = await healthResponse.json();
      } catch {
        health = null;
      }
      const resources = performance.getEntriesByType('resource')
        .map(entry => new URL(entry.name).pathname);
      return {
        protocol: location.protocol,
        port: location.port,
        javascriptAssets: [...new Set(resources.filter(resource => /\.m?js$/i.test(resource)))],
        stylesheetAssets: [...new Set(resources.filter(resource => /\.css$/i.test(resource)))],
        startButtonVisible: Boolean(document.querySelector('#startButton')),
        healthStatus: healthResponse.status,
        health
      };
    });
    ensure(
      browserEvidence.protocol === 'https:' && browserEvidence.port === String(DEV_PORT),
      label + ' did not load from the fixed HTTPS browser port.'
    );
    ensure(
      browserEvidence.startButtonVisible && browserEvidence.javascriptAssets.length > 0 &&
        browserEvidence.stylesheetAssets.length > 0,
      label + ' did not load the app UI, JavaScript, and CSS assets.'
    );
    ensure(
      browserEvidence.healthStatus === 200 && browserEvidence.health?.ok === true,
      label + ' could not reach backend /health through the HTTPS gateway.'
    );
    return {
      status: 'passed',
      url: url + '/',
      documentStatus: response.status(),
      javascriptAssets: browserEvidence.javascriptAssets,
      stylesheetAssets: browserEvidence.stylesheetAssets,
      proxiedHealth: {
        status: browserEvidence.healthStatus,
        ok: browserEvidence.health.ok,
        signalingPort: SIGNALING_PORT
      }
    };
  } finally {
    if (!page.isClosed()) {
      await page.close().catch(() => {});
    }
  }
}

async function runDebugE2E() {
  await withStatus(report.debug, async () => {
    await assertPortFree(SIGNALING_PORT);
    await assertPortFree(DEV_PORT);
    const server = startNpmScript(
      'server-debug',
      'server:debug',
      [],
      { SFU_DEBUG: '0', NODE_ENV: 'development' }
    );
    currentServerRecord = server;
    await waitForHealth(server, health => health.ok === true, 'debug server /health');

    const client = startNpmScript(
      'client-debug-vite',
      'client',
      [],
      { SFU_DEBUG: '0', NODE_ENV: 'development' }
    );
    ensure(!client.exit, 'HTTPS debug Vite client exited before its endpoint check.');
    const debugEndpoint = await verifyBrowserEndpoint(
      DEV_APP_URL,
      'debug',
      'HTTPS debug Vite client',
      '/src/client.ts',
      client
    );
    report.debug.endpoint = debugEndpoint;

    const producer = await withStatus(report.debug.producer, () =>
      startProducerCycle(
        'debug',
        DEV_APP_URL,
        'debug-cycle',
        report.debug.producerCycle
      )
    );
    Object.assign(report.debug.producer, producer.producerMetrics, {
      localLogHidden: producer.initialLog.hidden,
      patternTitle: PATTERN_TITLE,
      patternFrameProgress: producer.patternFrames
    });
    const consumer = await withStatus(report.debug.consumer, async () => {
      const page = await startConsumerPage('debug', DEV_APP_URL, 'debug-consumer-1');
      const sample = await measureConsumer(page.page, 'Debug Consumer 1');
      Object.assign(report.debug.consumer, sample);
      return page;
    });

    await withStatus(report.debug.diagnostics, async () => {
      await Promise.all([
        waitForWebSocketEvent(
          producer.producerPage,
          producer.producerAudit,
          'pipelineStats',
          'debug pipelineStats event',
          PAGE_TIMEOUT_MS
        ),
        waitForWebSocketEvent(
          consumer.page,
          consumer.audit,
          'transportState',
          'debug transportState event',
          PAGE_TIMEOUT_MS
        ),
        waitForWebSocketEvent(
          consumer.page,
          consumer.audit,
          'serverStats',
          'debug serverStats event (100-second production interval)',
          DEBUG_STATS_TIMEOUT_MS,
        )
      ]);

      await Promise.all([
        syncWebSocketAudit(producer.producerPage, producer.producerAudit),
        syncWebSocketAudit(consumer.page, consumer.audit)
      ]);

      const producerLog = await producer.producerPage.$eval(
        '#log',
        element => ({ hidden: element.hidden, text: element.textContent ?? '' })
      );
      const consumerLog = await consumer.page.$eval(
        '#log',
        element => ({ hidden: element.hidden, text: element.textContent ?? '' })
      );
      ensure(!producerLog.hidden && !consumerLog.hidden, 'Debug #log box is not visible.');
      ensure(
        /assigned as producer/.test(producerLog.text) &&
          /assigned as consumer/.test(consumerLog.text),
        'Debug role-assignment diagnostics are missing.'
      );
      ensure(
        /server injected/.test(producerLog.text),
        'Debug pipeline diagnostics are missing from the Producer log box.'
      );
      ensure(
        /transport/.test(consumerLog.text),
        'Debug transportState diagnostics are missing from the Consumer log box.'
      );
      ensure(
        /server stats:/.test(consumerLog.text),
        'Debug serverStats diagnostics are missing from the Consumer log box.'
      );
      ensure(
        producer.producerAudit.consoleDebug.length > 0 &&
          consumer.audit.consoleDebug.length > 0,
        'Debug browser console.debug output was not observed.'
      );
      const traceMonitoringGuard =
        report.staticValidation.sourceGuards.checks.traceMonitoringDebugGuard;
      ensure(traceMonitoringGuard, 'Consumer trace monitoring is not guarded by debug mode.');
      const layerschangeListenerEnabled =
        report.staticValidation.sourceGuards.checks.layersChangeDebugGuard;
      ensure(
        layerschangeListenerEnabled,
        'Consumer layerschange listener is not guarded by debug mode.'
      );
      ensure(processLogHasDebug(server), 'Debug server/worker output did not include debug logs.');
      ensure(!server.exit && !client.exit, 'A debug process exited during the active media test.');

      const finalConsumer = await readUiSnapshot(consumer.page);
      ensure(
        finalConsumer.rtpReceived > report.debug.consumer.final.rtpReceived &&
          finalConsumer.framesDecoded > report.debug.consumer.final.framesDecoded,
        'Debug Consumer media stopped while waiting for diagnostics.'
      );
      const layerschangeLogObserved = /current layers/.test(server.tail);
      Object.assign(report.debug.diagnostics, {
        debugLogVisibleProducer: !producerLog.hidden,
        debugLogVisibleConsumer: !consumerLog.hidden,
        producerWebSocketEvents: producer.producerAudit.websocketEventCounts,
        consumerWebSocketEvents: consumer.audit.websocketEventCounts,
        serverDebugLogObserved: true,
        layerschangeListenerEnabled,
        layerschangeLogObserved,
        consumerTraceMonitoringGuardVerified: traceMonitoringGuard,
        workerDebugLevelSwitchVerified:
          report.staticValidation.sourceGuards.checks.workerDebugLevelSwitch,
        sourceGuards: report.staticValidation.sourceGuards.checks,
        producerLogTail: producerLog.text.slice(0, 6000),
        consumerLogTail: consumerLog.text.slice(0, 6000),
        finalConsumerSnapshot: finalConsumer,
        serverLogTail: server.tail.slice(-6000)
      });
    });

    await Promise.all([...runtime.pages].map(page => page.close().catch(() => {})));
    await stopProcess(client);
    await stopProcess(server);
    currentServerRecord = null;
    report.debug.processesStoppedAfterTests = true;
  });
}

let currentServerRecord = null;

async function stopProcess(record) {
  if (!record || record.stopped) {
    return;
  }
  record.stopRequested = true;
  const child = record.child;
  if (record.exit || child.exitCode !== null || child.signalCode !== null) {
    record.stopRequested = false;
    record.stopped = true;
    await record.writeChain;
    return;
  }
  record.stoppedByHarness = true;

  if (process.platform === 'win32') {
    await new Promise(resolve => {
      let settled = false;
      const killer = spawn(
        'taskkill.exe',
        ['/PID', String(child.pid), '/T', '/F'],
        { windowsHide: true, stdio: 'ignore' }
      );
      const finish = () => {
        if (!settled) {
          settled = true;
          resolve();
        }
      };
      killer.once('error', finish);
      killer.once('close', finish);
      setTimeout(finish, 8000).unref?.();
    });
  } else {
    try {
      process.kill(-child.pid, 'SIGTERM');
    } catch {
      child.kill('SIGTERM');
    }
  }

  const exitResult = await waitForProcessExit(record, 8000);
  if (!exitResult) {
    if (process.platform === 'win32') {
      await new Promise(resolve => {
        const killer = spawn(
          'taskkill.exe',
          ['/PID', String(child.pid), '/T', '/F'],
          { windowsHide: true, stdio: 'ignore' }
        );
        killer.once('error', resolve);
        killer.once('close', resolve);
        setTimeout(resolve, 8000).unref?.();
      });
    } else {
      try {
        process.kill(-child.pid, 'SIGKILL');
      } catch {
        child.kill('SIGKILL');
      }
    }
  }
  await waitForProcessExit(record, 8000);
  record.stopped = Boolean(record.exit || child.exitCode !== null);
  await record.writeChain;
}

async function closeHttpServer(server) {
  if (!server?.listening) {
    return true;
  }
  return new Promise(resolve => {
    server.close(() => resolve(true));
    server.closeAllConnections?.();
    setTimeout(() => resolve(false), 5000).unref?.();
  });
}

async function cleanupRuntime() {
  if (runtime.cleanupPromise) {
    return runtime.cleanupPromise;
  }
  runtime.cleanupStarted = true;
  runtime.cleanupPromise = (async () => {
    if (runtime.browser) {
      try {
        await withTimeout(runtime.browser.close(), 12_000, 'Chrome shutdown');
        report.cleanup.browserClosed = true;
      } catch {
        // If Chrome does not close normally, terminate only Puppeteer's owned process tree.
      }
    } else {
      report.cleanup.browserClosed = true;
    }

    if (
      runtime.browserProcess &&
      runtime.browserProcess.exitCode === null &&
      runtime.browserProcess.pid
    ) {
      if (process.platform === 'win32') {
        await new Promise(resolve => {
          const killer = spawn(
            'taskkill.exe',
            ['/PID', String(runtime.browserProcess.pid), '/T', '/F'],
            { windowsHide: true, stdio: 'ignore' }
          );
          killer.once('error', resolve);
          killer.once('close', resolve);
          setTimeout(resolve, 8000).unref?.();
        });
      } else {
        try {
          process.kill(-runtime.browserProcess.pid, 'SIGKILL');
        } catch {
          runtime.browserProcess.kill('SIGKILL');
        }
      }
      report.cleanup.browserClosed =
        runtime.browserProcess.exitCode !== null ||
        runtime.browserProcess.signalCode !== null;
    } else if (runtime.browserProcess) {
      report.cleanup.browserClosed = true;
    }

    for (const record of [...runtime.processes].reverse()) {
      await stopProcess(record).catch(error => {
        report.errors.push('Could not stop ' + record.label + ': ' + errorMessage(error));
      });
    }
    report.cleanup.childProcessesStopped = runtime.processes.every(
      record => record.stopped || Boolean(record.exit)
    );

    report.cleanup.patternServerStopped = await closeHttpServer(runtime.patternServer);

    for (const record of runtime.processes) {
      await record.writeChain;
      report.runtimeLogs.push({
        name: record.label,
        exit: record.exit,
        spawnError: record.spawnError ? errorMessage(record.spawnError) : null,
        stopRequested: record.stopRequested,
        stoppedByHarness: record.stoppedByHarness,
        logWriteError: record.logWriteError,
        logTail: record.tail
      });
    }

    if (runtime.tempDir) {
      const tempRoot = path.resolve(os.tmpdir());
      const candidate = path.resolve(runtime.tempDir);
      ensure(
        candidate.startsWith(tempRoot + path.sep) &&
          path.basename(candidate).startsWith('webrtc-sfu-media-e2e-'),
        'Refusing to remove a temp directory outside this harness temp root.'
      );
      for (let attempt = 0; attempt < 4; attempt += 1) {
        try {
          await rm(candidate, { recursive: true, force: true, maxRetries: 2 });
          report.cleanup.tempDirectoryRemoved = true;
          break;
        } catch {
          await rawDelay(250 * (attempt + 1));
        }
      }
    }
  })();
  return runtime.cleanupPromise;
}

function renderProducerCycleLines(label, trace) {
  const checkpoints = Object.entries(trace?.checkpoints ?? {})
    .filter(([, checkpoint]) => checkpoint.status !== 'not-run')
    .map(([name, checkpoint]) =>
      '    ' + name + ': ' + checkpoint.status + ' (' +
        String(checkpoint.durationMs ?? 'n/a') + 'ms)'
    );
  const warnings = (trace?.timingSanityWarnings ?? []).map(warning =>
    '    timing warning: ' + warning.checkpoint + '=' + warning.durationMs +
      'ms (limit ' + warning.thresholdMs + 'ms)'
  );
  return [
    '  ' + label + ': ' + String(trace?.status ?? 'not-run') +
      ' (' + String(trace?.durationMs ?? 'n/a') + 'ms)',
    ...checkpoints,
    ...warnings
  ];
}

function renderLayerSelectionLines(layerName, selection, requestKeyFrameReached) {
  if (!selection) {
    return [`    ${layerName} layer selection: not reported`];
  }
  if (selection.stage === 'setPreferredLayers') {
    return [
      `    ${layerName} layer selection: setPreferredLayers failed; requested=${selection.requestedSpatialLayer}; preferred before=${JSON.stringify(selection.preferredLayersBefore)}; preferred after failure=${JSON.stringify(selection.preferredLayersAfterFailure)}; initial current=${JSON.stringify(selection.initialCurrentLayers)}`,
      `      error: ${selection.error}`,
      `      requestKeyFrame reached: ${requestKeyFrameReached ?? selection.requestKeyFrameReached}`
    ];
  }
  const sequence = (selection.observedCurrentSpatialLayerSequence ?? [])
    .map(sample => sample.spatialLayer ?? 'null')
    .join(' -> ');
  const outgoingBitrate = (selection.availableOutgoingBitrateSamples ?? [])
    .map(sample => sample.availableOutgoingBitrate ?? 'null')
    .join(', ');
  return [
    `    ${layerName} layer selection: ${selection.result}; requested=${selection.requestedSpatialLayer}; preferred before=${JSON.stringify(selection.preferredLayersBefore)}; preferred after set=${JSON.stringify(selection.preferredLayersAfterSet)}; initial current=${JSON.stringify(selection.initialCurrentLayers)}; final current=${JSON.stringify(selection.finalCurrentLayers)}; setPreferredLayers=${selection.setPreferredLayersDurationMs}ms; wait=${selection.waitElapsedMs}ms; transition=${selection.transitionElapsedMs ?? 'n/a'}ms; targetReachedAt=${selection.targetReachedAt ?? 'n/a'}`,
    `      current spatial sequence: [${sequence}]`,
    `      final priority=${selection.finalPriority}; score=${JSON.stringify(selection.finalScore)}; paused=${selection.finalPaused}; producerPaused=${selection.finalProducerPaused}`,
    `      availableOutgoingBitrate samples (bps): [${outgoingBitrate}]`,
    `      layerschange: ${JSON.stringify(selection.layerschangeSamples ?? [])}`,
    `      requestKeyFrame reached: ${requestKeyFrameReached ?? selection.requestKeyFrameReached}`
  ];
}

function renderFreshPliLayerLines(layerName, result) {
  const session = result.consumerSession ?? {};
  const initial = result.initialLayerState ?? {};
  const media = result.consumerMedia ?? {};
  const cleanup = result.cleanup ?? {};
  return [
    `    fresh ${layerName} Consumer: ${result.status}; peer=${session.peerId ?? 'n/a'}; consumer=${session.consumerId ?? 'n/a'}; transport=${session.transportId ?? 'n/a'}; createdAt=${session.consumerCreatedAt ?? 'n/a'}`,
    `      initial preferred/current=${JSON.stringify({ preferredLayers: initial.preferredLayers ?? null, currentLayers: initial.currentLayers ?? null })}; target reached=${result.targetReachedAt ?? 'n/a'}; layerSelectionMs=${result.layerSelectionMs ?? 'n/a'}`,
    `      requestKeyFrame reached=${result.requestKeyFrameReached ?? 'n/a'}; PLI feedback=${result.pliStatus}; evidence=${JSON.stringify(result.pliEvidence ?? null)}`,
    `      PLI latency ms (selection excluded)=${JSON.stringify(result.latency ?? null)}`,
    `      consumer RTP delta=${media.rtpDelta ?? 'n/a'}; decoded frame delta=${media.decodedFrameDelta ?? 'n/a'}; cleanup=${JSON.stringify(cleanup)}`,
    ...renderLayerSelectionLines(`fresh Consumer ${layerName}`, result.layerSelection, result.requestKeyFrameReached)
  ];
}

function renderConsumerInboundScenarioLines(label, scenario) {
  if (!scenario || scenario.status === 'not-run') {
    return [`  ${label}: not run`];
  }
  const spread = scenario.timestampSpreadMs ?? {};
  const lines = [
    `  ${label}: ${scenario.status}; Consumer=${scenario.consumer?.consumerId ?? 'UNKNOWN'}; peer=${scenario.consumer?.peerId ?? 'UNKNOWN'}; getStats reports=${scenario.getStats?.callCount ?? 0}; timestamp spread max/median=${spread.maximumWithinReport ?? 'n/a'}/${spread.medianWithinReport ?? 'n/a'}ms`
  ];
  for (const timeline of scenario.statTimelines ?? []) {
    const codec = timeline.codecs?.map(item =>
      `${item.mimeType ?? 'UNKNOWN'}/${item.payloadType ?? 'UNKNOWN'}`
    ).join(',') || 'UNKNOWN';
    const delta = field => timeline.deltas?.[field]?.value ?? 'UNKNOWN';
    lines.push(
      `    ${timeline.statId}: SSRC=${(timeline.ssrcs ?? []).join(',') || 'UNKNOWN'}; codec=${codec}; track=${(timeline.trackIdentifiers ?? []).join(',') || 'UNKNOWN'}; ΔframesReceived/Decoded=${delta('framesReceived')}/${delta('framesDecoded')}; Δbytes/packets=${delta('bytesReceived')}/${delta('packetsReceived')}; primary SSRC match=${JSON.stringify(timeline.primarySsrcMatchesConsumerRtpParameters ?? [])}; rtxSsrc field match=${JSON.stringify(timeline.rtxFieldMatchesConsumerRtpParameters ?? [])}; current SSRC-as-RTX match=${JSON.stringify(timeline.rtxSsrcMatchesConsumerRtpParameters ?? [])}; ΔretransmittedPacketsReceived=${delta('retransmittedPacketsReceived')}; ${timeline.classification}`
    );
  }
  return lines;
}

function finalizeConsumerInboundRtpAudit() {
  const audit = report.production.consumerInboundRtpAudit;
  const layerSequence = audit.simulcastOnLayerSequence;
  const layerCapturesComplete = layerSequence.status === 'passed' &&
    ['low', 'mid', 'high'].every(layer => layerSequence.byLayer?.[layer]?.status === 'captured');
  const checks = {
    simulcastOnNoLayerChange: audit.simulcastOnNoLayerChange.status === 'captured',
    simulcastOnLowMidHigh: layerCapturesComplete,
    simulcastOff: audit.simulcastOff.status === 'captured',
    multiConsumerIsolation: audit.multiConsumerIsolation.status === 'passed'
  };
  audit.coverage = checks;
  audit.status = Object.values(checks).every(Boolean) ? 'captured' : 'incomplete';
}

function renderTextReport() {
  const sequentialDiagnostic = report.production.layerSelectionDiagnostic;
  const lines = [
    'WebRTC actual-media E2E smoke report',
    'Result: ' + (report.passed ? 'PASS' : 'FAIL'),
    'Run mode: ' + report.execution.mode,
    'Run scope: ' + report.execution.scope,
    'E2E BWE control: ' + (report.execution.bweControl.performed ? 'yes' : 'no') +
      '; baseline initial estimate=' +
      report.execution.bweControl.baselineInitialAvailableOutgoingBitrateBps +
      ' bps; effective initial estimate=' +
      report.execution.bweControl.effectiveInitialAvailableOutgoingBitrateBps +
      ' bps; scope=' + report.execution.bweControl.overrideScope,
    'Full E2E completed: ' + report.execution.fullE2ECompleted,
    'Completed scope: ' + String(report.execution.completedScope ?? 'incomplete'),
    'Skipped checks: ' + JSON.stringify(report.execution.skippedChecks),
    'Timestamp: ' + report.timestamp,
    'Chrome: ' + String(report.automation.chromeExecutable ?? 'not launched'),
    'Capture source title: ' + PATTERN_TITLE,
    'Capture switch: ' + report.automation.captureAutoSelectSwitch,
    'User interaction required: NO',
    'Production: ' + report.production.status,
    '  Producer: ' + report.production.producer.status,
    '  Consumer 1: ' + report.production.consumer1.status,
    '  Consumer JSON stats dump: ' + report.production.consumerStatsDump.status +
      ' (' + String(report.production.consumerStatsDump.sampleCount ?? 'n/a') + ' samples)',
    '    RTCStats accuracy audit: ' +
      String(report.production.consumerStatsDump.runtimeAudit?.status ?? 'not run') +
      ' (' + String(report.production.consumerStatsDump.runtimeAudit?.getStatsCallCount ?? 'n/a') +
      ' getStats calls, ' +
      String(report.production.consumerStatsDump.runtimeAudit?.distinctInboundStatIdCount ?? 'n/a') +
      ' inbound IDs, max concurrent=' +
      String(report.production.consumerStatsDump.runtimeAudit?.maximumConcurrentCalls ?? 'n/a') + ')',
    '    RTCStats raw fields compared: ' +
      String(report.production.consumerStatsDump.runtimeAudit?.rawCounterAndTimestampFieldsCompared ?? 'n/a') +
      '; derived rate fields compared: ' +
      String(report.production.consumerStatsDump.runtimeAudit?.derivedRateFieldsCompared ?? 'n/a') +
      '; raw/media inbound objects per poll=' +
      JSON.stringify({
        raw: report.production.consumerStatsDump.runtimeAudit?.inboundVideoObjectCountByPoll ?? null,
        media: report.production.consumerStatsDump.runtimeAudit?.consumerMediaObjectCountByPoll ?? null
      }) +
      '; probator objects observed=' +
      String(report.production.consumerStatsDump.runtimeAudit?.mediasoupRtpProbatorObjectsObserved ?? 'n/a') +
      '; RTX-related fields present=' +
      String(report.production.consumerStatsDump.runtimeAudit?.rtxAssociatedCountersObserved ?? 'n/a') +
      ' (presence does not prove retransmission)',
    '  Consumer inbound RTP identity/aggregation audit: ' +
      report.production.consumerInboundRtpAudit.status +
      '; coverage=' + JSON.stringify(report.production.consumerInboundRtpAudit.coverage ?? null),
    ...renderConsumerInboundScenarioLines(
      'Simulcast ON, one Consumer, no layer change',
      report.production.consumerInboundRtpAudit.simulcastOnNoLayerChange
    ),
    '  Simulcast ON layer sequence: ' +
      report.production.consumerInboundRtpAudit.simulcastOnLayerSequence.status +
      ' (' + JSON.stringify(report.production.consumerInboundRtpAudit.simulcastOnLayerSequence.sequence ?? []) + ')',
    ...['low', 'mid', 'high'].flatMap(layer => {
      const result = report.production.consumerInboundRtpAudit.simulcastOnLayerSequence.byLayer?.[layer];
      if (!result) return [`    ${layer}: not run`];
      const rateSamples = (result.consumerStats ?? []).map(sample => ({
        elapsedMs: sample.elapsedMs,
        receivedFps: sample.receivedFps,
        decodedFps: sample.decodedFps,
        receivedMediaBitrateKbps: sample.receivedMediaBitrateKbps
      }));
      return [
        `    ${layer}: ${result.status}; stable current layer=${result.scenario?.simulcast?.stableCurrentLayer ?? 'UNKNOWN'}; layer wait=${result.stableLayer?.elapsedMs ?? 'n/a'}ms; Consumer JSON rate samples=${JSON.stringify(rateSamples)}`,
        ...renderConsumerInboundScenarioLines(`Simulcast ON ${layer}`, result.scenario)
      ];
    }),
    ...renderConsumerInboundScenarioLines(
      'Simulcast OFF, one HIGH Consumer',
      report.production.consumerInboundRtpAudit.simulcastOff
    ),
    '  Multi-consumer stats isolation: ' +
      report.production.consumerInboundRtpAudit.multiConsumerIsolation.status +
      '; checks=' + JSON.stringify({
        consumerIdsDistinct: report.production.consumerInboundRtpAudit.multiConsumerIsolation.consumerIdsDistinct ?? null,
        peerIdsDistinct: report.production.consumerInboundRtpAudit.multiConsumerIsolation.peerIdsDistinct ?? null,
        trackIdsDistinct: report.production.consumerInboundRtpAudit.multiConsumerIsolation.trackIdsDistinct ?? null,
        ownTrackMatches: report.production.consumerInboundRtpAudit.multiConsumerIsolation.eachStatsTimelineMatchesOwnRemoteTrack ?? null
      }),
    ...renderConsumerInboundScenarioLines(
      'Multi-consumer Consumer 1',
      report.production.consumerInboundRtpAudit.multiConsumerIsolation.consumer1?.scenario
    ),
    ...renderConsumerInboundScenarioLines(
      'Multi-consumer Consumer 2',
      report.production.consumerInboundRtpAudit.multiConsumerIsolation.consumer2?.scenario
    ),
    '  Consumer getStats failure recovery: ' +
      String(report.production.consumerStatsFailureRecovery.status) +
      ' (injected call=' +
      String(report.production.consumerStatsFailureRecovery.injectedCallNumber ?? 'n/a') +
      ', null sample=' +
      String(report.production.consumerStatsFailureRecovery.nullFailureSampleRecorded ?? 'n/a') +
      ', baseline recovery=' +
      String(report.production.consumerStatsFailureRecovery.firstRecoveryUsedAsBaseline ?? 'n/a') + ')',
    '  Consumer 2: ' + report.production.consumer2.status,
    '  Producer JSON stats dump: ' + report.production.producerStatsDump.status +
      ' (' + String(report.production.producerStatsDump.sampleCount ?? 'n/a') + ' samples)',
    '  Stats Visualizer smoke: ' + report.production.statsVisualizer.status +
      ' (HTTP ' + String(report.production.statsVisualizer.httpStatus ?? 'n/a') + ')',
    '    Chart.js renderer: ' + String(report.production.statsVisualizer.renderer ?? 'not observed'),
    '    SVG fallback: ' + String(report.production.statsVisualizer.fallback?.status ?? 'not run') +
      ' (' + String(report.production.statsVisualizer.fallback?.failureMode ?? 'n/a') + ')',
    ...((report.production.statsVisualizer.largeRuns ?? []).map(run =>
      '    large dashboard ' + run.role +
        (run.simulcast ? ' simulcast ' + run.simulcast : '') +
        ': ' + run.sampleCount + ' samples, ' + run.jsonBytes + ' bytes, ' +
        run.renderer + ', ' + run.renderMs + 'ms'
    )),
    '  Multi-consumer: ' + report.production.multiConsumer.status,
    '  Consumer independence: ' + report.production.consumerIndependence.status,
    '  E2E control rejected by default: ' + report.production.e2eControlGuard.status,
    '  Fresh-Consumer PLI correctness: ' + report.production.pliFir.status,
    '    request coalescing source assertions: ' + report.production.pliFir.coalescing.status +
      ' (max ' + String(report.production.pliFir.coalescing.maximumAuditRequests ?? 16) +
      ' audit IDs, one obligation per layer)',
    '    fresh low/mid/high: ' +
      ['low', 'mid', 'high'].map(layer =>
        layer + '=' + report.production.pliFir.byLayer[layer].status
      ).join(', '),
    '    PLI failure classification: ' +
      String(report.production.pliFir.failureClassification ?? 'none'),
    ...['low', 'mid', 'high'].flatMap(layer =>
      renderFreshPliLayerLines(
        layer,
        report.production.pliFir.byLayer[layer]
      )
    ),
    '    runtime FIR: ' + report.production.pliFir.fir.runtimeStatus,
    '    PLI request-to-keyframe latency ms (layer selection excluded): ' +
      JSON.stringify(report.production.pliFir.latency ?? 'not observed'),
    '  Sequential layer-selection diagnostic: ' + sequentialDiagnostic.status,
    '    reached: low=' + String(sequentialDiagnostic.lowReached ?? 'not run') +
      ', mid=' + String(sequentialDiagnostic.midReached ?? 'not run') +
      ', high=' + String(sequentialDiagnostic.highReached ?? 'not run'),
    '    high result: ' + String(sequentialDiagnostic.highSelectionDescription ?? 'not run'),
    '    selection times ms: ' + JSON.stringify(
      Object.fromEntries(['low', 'mid', 'high'].map(layer => [
        layer,
        sequentialDiagnostic.byLayer[layer].layerSelectionMs
      ]))
    ),
    '    current spatial-layer sequence: ' +
      JSON.stringify(sequentialDiagnostic.currentLayersSequence),
    '    available outgoing bitrate samples (bps): ' +
      JSON.stringify(sequentialDiagnostic.availableOutgoingBitrateSamples),
    '    media continuity: ' + JSON.stringify(sequentialDiagnostic.mediaContinuity),
    '  Production diagnostics: ' + report.production.diagnostics.status,
    '  Captured-source cleanup: ' + report.production.capturedSourceCleanup.status,
    '  Producer page-close cleanup: ' + report.production.producerPageCloseCleanup.status,
    '    phase: ' + report.production.producerPageCloseCleanup.phase,
    '    closeActionExecuted: ' + report.production.producerPageCloseCleanup.closeActionExecuted,
    '  Producer reload cleanup: ' + report.production.reloadCleanup.status,
    '    phase: ' + report.production.reloadCleanup.phase,
    '    reloadActionExecuted: ' + report.production.reloadCleanup.reloadActionExecuted,
    'Producer cycle checkpoints:',
    ...renderProducerCycleLines('initial', report.production.producerCycles.initial),
    ...renderProducerCycleLines('pageClose', report.production.producerCycles.pageClose),
    ...renderProducerCycleLines('reload', report.production.producerCycles.reload),
    ...renderProducerCycleLines('debug', report.debug.producerCycle),
    'Runtime wait diagnostics: ' + report.runtimeWaitDiagnostics.length,
    ...report.runtimeWaitDiagnostics.map(diagnostic =>
      '  ' + String(diagnostic.cycleLabel ?? 'runtime') + '/' +
        String(diagnostic.checkpoint ?? 'wait') + ': ' + diagnostic.classification
    ),
    '  Actual Producer codec: ' +
      String(report.production.producer.actualEncoderCodec ?? 'not observed'),
    '  Producer deltas: encoded chunks +' +
      String(report.production.producer.deltas?.encodedChunks ?? 'n/a') +
      ', injected RTP packets +' +
      String(report.production.producer.deltas?.injectedPackets ?? 'n/a'),
    '  Consumer 1 deltas: RTP +' +
      String(report.production.consumer1.deltas?.rtpReceived ?? 'n/a') +
      ', decoded frames +' +
      String(report.production.consumer1.deltas?.framesDecoded ?? 'n/a'),
    '  Consumer 2 deltas: RTP +' +
      String(report.production.consumer2.deltas?.rtpReceived ?? 'n/a') +
      ', decoded frames +' +
      String(report.production.consumer2.deltas?.framesDecoded ?? 'n/a'),
    '  Multi-consumer count: ' +
      String(report.production.multiConsumer.observedConsumers ?? 'n/a'),
    '  Simulcast OFF stats dump: ' + report.production.offMode.statsDump.status +
      ' (layers=' + JSON.stringify(report.production.offMode.statsDump.availableLayers ?? []) + ')',
    'Debug: ' + report.debug.status,
    '  Trace monitoring debug guard: ' +
      String(report.debug.diagnostics.consumerTraceMonitoringGuardVerified ?? 'not observed'),
    '  Graceful shutdown handler source guard: ' +
      String(report.staticValidation.sourceGuards.checks?.gracefulShutdownSignalHandlers ?? 'not observed'),
    '  Layerschange listener enabled: ' +
      String(
        report.debug.diagnostics.layerschangeListenerEnabled ??
          report.staticValidation.sourceGuards.checks?.layersChangeDebugGuard ??
          'not observed'
      ),
    '  Layerschange event observed (optional): ' +
      String(report.debug.diagnostics.layerschangeLogObserved ?? 'not observed'),
    'Static pipeline verification: ' + report.staticValidation.pipeline.status,
    'TypeScript typecheck: ' + report.staticValidation.typecheck.status,
    'Production build: ' + report.staticValidation.productionBuild.status,
    'Temporary resources removed: ' +
      String(
        report.cleanup.browserClosed &&
          report.cleanup.childProcessesStopped &&
          report.cleanup.patternServerStopped &&
          report.cleanup.tempDirectoryRemoved
      ),
    'Errors:'
  ];
  if (report.errors.length === 0) {
    lines.push('  none');
  } else {
    for (const error of report.errors) {
      lines.push('  - ' + error);
    }
  }
  return lines.join('\n') + '\n';
}

async function writeReports() {
  report.finishedAt = new Date().toISOString();
  await mkdir(ARTIFACT_DIR, { recursive: true });
  await writeFile(JSON_REPORT_PATH, JSON.stringify(report, null, 2) + '\n', 'utf8');
  await writeFile(TEXT_REPORT_PATH, renderTextReport(), 'utf8');
  console.log('JSON report: ' + JSON_REPORT_PATH);
  console.log('Text report: ' + TEXT_REPORT_PATH);
}

async function run() {
  try {
    runtime.tempDir = await mkdtemp(path.join(os.tmpdir(), 'webrtc-sfu-media-e2e-'));
    await mkdir(path.join(runtime.tempDir, 'logs'), { recursive: true });
    const pattern = await startPatternServer();
    report.automation.patternUrl = pattern.url;
    report.automation.patternFile = path.basename(pattern.file);

    await runStaticPreparation();
    await launchTestBrowser();
    await runProductionE2E();
    if (!STOP_AFTER_PLI) {
      await runSimulcastOffE2E();
    }
    if (!STOP_AFTER_PLI) {
      await runDebugE2E();
      report.execution.fullE2ECompleted = true;
      report.execution.completedScope = 'full production and debug E2E';
    }
    report.passed = true;
  } catch (error) {
    report.passed = false;
    report.errors.push(errorMessage(error));
    console.error('E2E media smoke failed: ' + errorMessage(error));
  } finally {
    await cleanupRuntime().catch(error => {
      report.passed = false;
      report.errors.push('Cleanup failed: ' + errorMessage(error));
    });
    if (
      !report.cleanup.browserClosed ||
      !report.cleanup.childProcessesStopped ||
      !report.cleanup.patternServerStopped ||
      !report.cleanup.tempDirectoryRemoved
    ) {
      report.passed = false;
      report.errors.push('One or more temporary resources were not confirmed cleaned up.');
    }
    finalizeConsumerInboundRtpAudit();
    await writeReports().catch(error => {
      report.passed = false;
      report.errors.push('Could not write result artifacts: ' + errorMessage(error));
      console.error('Could not write E2E report: ' + errorMessage(error));
    });
  }
  process.exitCode = report.passed ? 0 : 1;
  if (report.passed) {
    console.log('E2E media smoke (' + report.execution.mode + '): PASS');
  } else {
    console.error('E2E media smoke (' + report.execution.mode + '): FAIL');
  }
}

function handleSignal(signalName) {
  if (interruptedBy) {
    return;
  }
  interruptedBy = signalName;
  report.interruptedBy = signalName;
  abortController.abort();
  if (runtime.tempDir) {
    void cleanupRuntime();
  }
}

process.once('SIGINT', () => handleSignal('SIGINT'));
process.once('SIGTERM', () => handleSignal('SIGTERM'));
if (process.platform !== 'win32') {
  process.once('SIGHUP', () => handleSignal('SIGHUP'));
}

export function setCurrentServerRecord(record) {
  currentServerRecord = record;
}

export {
  runtime as sharedE2eRuntime,
  report as sharedE2eReport,
  assertPortFree,
  captureVideoHash,
  checkInterrupted,
  cleanupRuntime,
  createProducerCycleTrace,
  downloadStatsDumpFromPage,
  fetchJson,
  findE2EConsumerAudit,
  launchTestBrowser,
  readUiSnapshot,
  requestOverAuditedSocket,
  startConsumerPage,
  startNpmScript,
  startPatternServer,
  startProducerCycle,
  verifyConsumerStatsDump,
  verifyProducerStatsDump,
  wait,
  waitForConsumerReady,
  waitForHealth,
  waitForStableConsumerSpatialLayer,
  HEALTH_URL,
  PATTERN_TITLE,
  PRODUCTION_APP_URL,
  RUNTIME_POLL_INTERVAL_MS,
  SIGNALING_PORT,
  DEV_PORT
};

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url))
) {
  await run();
}
