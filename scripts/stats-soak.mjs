#!/usr/bin/env node

import { execFileSync } from 'node:child_process';
import { appendFile, copyFile, mkdir, mkdtemp, readFile, rename, stat, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import pidusage from 'pidusage';
import si from 'systeminformation';
import { analyzeRun } from './analyze-stats-soak.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SOAK_ROOT = path.join(ROOT, 'artifacts', 'soak-runs');
const DIAGNOSTIC_PROBE_INTERVAL_MS = 5_000;
const STALL_CONFIRMATION_MS = 15_000;
const STALL_EVIDENCE_SPACING_MS = 5_000;
const args = new Map();
for (const token of process.argv.slice(2)) {
  const match = /^--([^=]+)(?:=(.*))?$/.exec(token);
  if (!match) throw new Error(`Unexpected argument: ${token}`);
  args.set(match[1], match[2] ?? 'true');
}

const durationMinutes = Number(args.get('duration-minutes') ?? 3);
if (![3, 20, 25, 30].includes(durationMinutes)) {
  throw new Error('Supported soak durations are exactly 3, 20, 25, or 30 minutes.');
}
const label = String(args.get('label') ?? (durationMinutes === 3 ? 'rehearsal' : 'full-soak'))
  .replaceAll(/[^a-z0-9_-]/gi, '-')
  .replaceAll(/-+/g, '-')
  .replaceAll(/^-|-$/g, '') || 'soak';
const backendPort = Number(process.env.PORT ?? 3018);
if (!Number.isInteger(backendPort) || backendPort < 1 || backendPort > 65535) {
  throw new Error('PORT must be an integer from 1 through 65535.');
}
process.env.PORT = String(backendPort);

const e2e = await import('./e2e-media-smoke.mjs');
const {
  sharedE2eReport,
  sharedE2eRuntime,
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
  setCurrentServerRecord,
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
  SIGNALING_PORT,
  DEV_PORT
} = e2e;

const runTimestamp = new Date().toISOString().replaceAll(/[:.]/g, '-');
const runId = `${runTimestamp}-${label}`;
const runDir = path.join(SOAK_ROOT, runId);
const participantNames = ['producer', 'consumer1', 'consumer2'];
const pages = {};
const events = [];
const anomalies = [];
const downloads = [];
const monitorSnapshots = [];
const diagnosticProbeHistory = [];
const layerChanges = [];
let serverRecord = null;
let previewRecord = null;
let producerCycle = null;
let t0Monotonic = null;
let t0WallTime = null;
let abortCause = null;

const manifest = {
  memoryMetricVersion: 3,
  runId,
  label,
  durationMinutes,
  targetDurationMs: durationMinutes * 60_000,
  status: 'starting',
  createdAt: new Date().toISOString(),
  backendPort: SIGNALING_PORT,
  browserPort: DEV_PORT,
  sameHost: true,
  t0: null,
  finishedAt: null,
  participants: {},
  schedule: [],
  events,
  anomalies,
  downloads,
  layerChanges,
  monitorSnapshots,
  diagnosticProbeCount: 0,
  stallDiagnostics: [],
  visualizer: null,
  cleanup: null,
  errors: [],
  files: {
    environment: 'environment.json',
    manifest: 'manifest.json',
    eventTimeline: 'event-timeline.json',
    analysis: 'analysis.json',
    report: 'report.md',
    diagnosticProbes: 'diagnostic-probes.jsonl'
  }
};

function ensure(condition, message) {
  if (!condition) throw new Error(message);
}

function elapsedMs() {
  return t0Monotonic === null ? null : Math.max(0, Math.round(performance.now() - t0Monotonic));
}

async function installRenderedFrameObserver(page) {
  return page.evaluate(() => {
    const video = document.querySelector('#remoteVideo');
    if (!video) return { installed: false, reason: 'remoteVideo element is missing' };
    if (window.__sfuSoakRenderObserver) {
      return { installed: true, ...window.__sfuSoakRenderObserver };
    }
    const state = {
      installed: true,
      supported: typeof video.requestVideoFrameCallback === 'function',
      source: typeof video.requestVideoFrameCallback === 'function'
        ? 'requestVideoFrameCallback'
        : 'getVideoPlaybackQuality.totalVideoFrames',
      callbackCount: 0,
      lastFrame: null,
      callbackError: null
    };
    window.__sfuSoakRenderObserver = state;
    if (typeof video.requestVideoFrameCallback === 'function') {
      const onFrame = (now, metadata) => {
        state.callbackCount += 1;
        state.lastFrame = {
          now,
          mediaTime: metadata.mediaTime ?? null,
          presentedFrames: metadata.presentedFrames ?? null,
          expectedDisplayTime: metadata.expectedDisplayTime ?? null,
          width: metadata.width ?? null,
          height: metadata.height ?? null,
          processingDuration: metadata.processingDuration ?? null
        };
        try {
          video.requestVideoFrameCallback(onFrame);
        } catch (error) {
          state.callbackError = String(error);
        }
      };
      try {
        video.requestVideoFrameCallback(onFrame);
      } catch (error) {
        state.supported = false;
        state.callbackError = String(error);
        state.source = 'getVideoPlaybackQuality.totalVideoFrames';
      }
    }
    return { ...state };
  });
}

function renderedFrameCounter(snapshot) {
  const observer = snapshot?.renderObserver;
  if (observer?.supported === true && snapshot?.documentVisibilityState === 'visible') {
    const presentedFrames = observer.lastFrame?.presentedFrames;
    return typeof presentedFrames === 'number'
      ? presentedFrames
      : observer.callbackCount ?? null;
  }
  return null;
}

async function atomicWrite(filePath, content) {
  const temporaryPath = `${filePath}.${process.pid}.tmp`;
  await writeFile(temporaryPath, content);
  await rename(temporaryPath, filePath);
}

async function persistManifest() {
  await atomicWrite(path.join(runDir, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n');
  await atomicWrite(path.join(runDir, 'event-timeline.json'), JSON.stringify(events, null, 2) + '\n');
}

async function recordEvent(type, data = {}) {
  events.push({
    sequence: events.length + 1,
    type,
    wallTime: new Date().toISOString(),
    elapsedMs: elapsedMs(),
    ...data
  });
  await persistManifest();
}

async function recordAnomaly(kind, participant, details = {}) {
  const anomaly = {
    kind,
    participant: participant ?? null,
    wallTime: new Date().toISOString(),
    elapsedMs: elapsedMs(),
    recoveredAt: null,
    details
  };
  anomalies.push(anomaly);
  await recordEvent('anomaly', anomaly);
  return anomaly;
}

function scrubOsInfo(value) {
  return {
    platform: value.platform ?? null,
    distro: value.distro ?? null,
    release: value.release ?? null,
    arch: value.arch ?? null,
    build: value.build ?? null
  };
}

async function environmentSnapshot(browser) {
  const [osInfo, cpu, memory, graphics] = await Promise.all([
    si.osInfo().catch(() => null),
    si.cpu().catch(() => null),
    si.mem().catch(() => null),
    si.graphics().catch(() => null)
  ]);
  const gitCommit = execFileSync('git', ['rev-parse', 'HEAD'], {
    cwd: ROOT,
    encoding: 'utf8'
  }).trim();
  const actualArgs = sharedE2eRuntime.browserProcess?.spawnargs ?? [];
  const featureArgs = actualArgs.filter(arg =>
    arg.startsWith('--auto-select-tab-capture-source-by-title=') ||
    arg === '--disable-background-timer-throttling' ||
    arg === '--disable-renderer-backgrounding' ||
    arg === '--disable-backgrounding-occluded-windows' ||
    arg === '--allow-insecure-localhost' ||
    arg.startsWith('--use-fake-')
  );
  return {
    capturedAt: new Date().toISOString(),
    os: osInfo ? scrubOsInfo(osInfo) : null,
    cpu: cpu ? {
      manufacturer: cpu.manufacturer,
      brand: cpu.brand,
      speedGHz: cpu.speed,
      physicalCores: cpu.physicalCores,
      logicalCores: cpu.cores
    } : null,
    gpu: Array.isArray(graphics?.controllers)
      ? graphics.controllers.map(controller => ({
          vendor: controller.vendor,
          model: controller.model,
          vramMB: controller.vram ?? null
        }))
      : null,
    memory: memory ? {
      totalBytes: memory.total,
      availableBytes: memory.available,
      usedBytes: memory.used
    } : {
      totalBytes: os.totalmem(),
      availableBytes: os.freemem(),
      usedBytes: os.totalmem() - os.freemem()
    },
    chrome: {
      version: await browser.version().catch(() => null),
      headless: false,
      temporaryProfile: true,
      executable: path.basename(sharedE2eRuntime.browserProcess?.spawnfile ?? 'UNKNOWN'),
      appliedCaptureAndSchedulingArgs: featureArgs,
      fakeMediaFlagsApplied: featureArgs.some(arg => arg.startsWith('--use-fake-'))
    },
    node: process.version,
    gitCommit,
    execution: {
      backendPort: SIGNALING_PORT,
      browserPort: DEV_PORT,
      patternPageTitle: PATTERN_TITLE,
      simulcastEnabled: true,
      producerCount: 1,
      consumerCount: 2,
      sameHost: true,
      actualMedia: 'Chrome display capture of the existing moving test pattern; no fake MediaStream or RTCStatsReport',
      consumerGetStatsPolling: 'existing application poll only; no additional browser Consumer getStats calls',
      diagnosticProbeIntervalMs: DIAGNOSTIC_PROBE_INTERVAL_MS,
      stallConfirmationMs: STALL_CONFIRMATION_MS,
      anomalyServerStats: 'two E2E-gated, one-shot SFU snapshots spaced 5 seconds apart after a confirmed stall; no background polling',
      renderEvidence: 'video-element screenshot pixel hashes every 5 seconds; requestVideoFrameCallback counters are interpreted only for visible documents'
    }
  };
}

function processTree(rootPid, processList) {
  if (!Number.isInteger(rootPid)) return [];
  const byParent = new Map();
  for (const process of processList) {
    const key = Number(process.parentPid);
    const list = byParent.get(key) ?? [];
    list.push(process);
    byParent.set(key, list);
  }
  const found = new Map();
  const pending = [rootPid];
  while (pending.length > 0) {
    const parent = pending.pop();
    if (found.has(parent)) continue;
    const process = processList.find(item => Number(item.pid) === parent);
    if (process) found.set(parent, process);
    for (const child of byParent.get(parent) ?? []) pending.push(Number(child.pid));
  }
  return [...found.values()];
}

async function processMetrics(rootPid, processList) {
  const tree = processTree(rootPid, processList);
  let root = null;
  try {
    root = Number.isInteger(rootPid) ? await pidusage(rootPid) : null;
  } catch {
    // Root process metrics may be unavailable after a child exits.
  }
  return {
    rootPid: Number.isInteger(rootPid) ? rootPid : null,
    treePids: tree.map(item => Number(item.pid)),
    processTreeRssKiB: tree.length > 0
      ? tree.reduce((sum, item) => sum + (Number(item.memRss) || 0), 0)
      : null,
    processTreeRssBytes: tree.length > 0
      ? tree.reduce((sum, item) => sum + (Number(item.memRss) || 0), 0) * 1024
      : null,
    processTreeCpuPercent: tree.length > 0
      ? tree.reduce((sum, item) => sum + (Number(item.cpu) || 0), 0)
      : null,
    rootRssBytes: root?.memory ?? null,
    rootCpuPercent: root?.cpu ?? null
  };
}

async function monitorOnce(previous) {
  const capturedAt = new Date().toISOString();
  const [health, uiProducer, uiConsumer1, uiConsumer2, processInfo, currentLoad, memory] =
    await Promise.all([
      fetchJson(HEALTH_URL),
      readUiSnapshot(pages.producer.producerPage),
      readUiSnapshot(pages.consumer1.page),
      readUiSnapshot(pages.consumer2.page),
      si.processes().catch(() => null),
      si.currentLoad().catch(() => null),
      si.mem().catch(() => null)
    ]);
  const processList = processInfo?.list ?? [];
  const browserMemory = await processMetrics(
    sharedE2eRuntime.browserProcess?.pid,
    processList
  );
  const backendMemory = await processMetrics(serverRecord?.child?.pid, processList);
  const runnerHeap = process.memoryUsage();
  const browserHeaps = await Promise.all(participantNames.map(async name => {
    const page = name === 'producer'
      ? pages.producer.producerPage
      : pages[name].page;
    try {
      return await page.evaluate(() => {
        const value = performance.memory?.usedJSHeapSize;
        return typeof value === 'number' && Number.isFinite(value) ? value : null;
      });
    } catch {
      return null;
    }
  }));
  const currentLayers = Object.fromEntries(['consumer1', 'consumer2'].map(name => {
    const peerId = manifest.participants[name]?.peerId;
    const audit = health.e2eConsumerAudit?.find(item => item.peerId === peerId);
    return [name, {
      preferredLayers: audit?.preferredLayers ?? null,
      currentLayers: audit?.currentLayers ?? null
    }];
  }));
  const snapshot = {
    wallTime: capturedAt,
    elapsedMs: elapsedMs(),
    serverAlive: !serverRecord?.exit,
    previewAlive: !previewRecord?.exit,
    chromeConnected: sharedE2eRuntime.browser?.connected ?? false,
    health: {
      ok: health.ok ?? false,
      simulcastEnabled: health.simulcastEnabled ?? null,
      consumers: health.consumers ?? null,
      producerPeerId: health.producerPeerId ?? null,
      activeLayers: health.activeLayers ?? null,
      inputFps: health.inputFps ?? null
    },
    currentLayers,
    producer: {
      peerId: uiProducer.peerId,
      socketState: uiProducer.socketState,
      role: uiProducer.role,
      localTrackState: uiProducer.localVideo?.tracks?.[0]?.readyState ?? null,
      encodedChunks: uiProducer.encodedChunks,
      inputFps: uiProducer.inputFps,
      measurementStatus: uiProducer.measurementStatus
    },
    consumer1: summarizeConsumerUi(uiConsumer1),
    consumer2: summarizeConsumerUi(uiConsumer2),
    system: {
      cpuLoadPercent: currentLoad?.currentLoad ?? null,
      totalRamBytes: memory?.total ?? null,
      availableRamBytes: memory?.available ?? null,
      usedRamBytes: memory?.used ?? null,
      chromeProcess: browserMemory,
      backendProcessTree: backendMemory,
      soakRunnerRssBytes: runnerHeap.rss,
      soakRunnerHeapUsedBytes: runnerHeap.heapUsed
    },
    browserJsHeapUsedBytes: Object.fromEntries(participantNames.map((name, index) => [name, browserHeaps[index]]))
  };
  const conditions = [
    ['producer', uiProducer.role === 'producer' && uiProducer.socketState === 'connected' &&
      uiProducer.localVideo?.tracks?.[0]?.readyState === 'live'],
    ['consumer1', consumerUiHealthy(uiConsumer1)],
    ['consumer2', consumerUiHealthy(uiConsumer2)]
  ];
  for (const [name, healthy] of conditions) {
    if (!healthy) await recordAnomaly('participant-health-failed', name, snapshot[name]);
  }
  const fatal = !snapshot.chromeConnected || !snapshot.serverAlive || !snapshot.previewAlive ||
    conditions.some(([, healthy]) => !healthy);
  if (fatal) {
    await recordEvent('fatal-health-condition', { snapshot });
    throw new Error('A required soak participant, Chrome, or owned service became unhealthy; no process was restarted.');
  }

  for (const name of ['consumer1', 'consumer2']) {
    const current = snapshot[name];
    const before = previous?.[name];
    const countersAdvanced = !before || (current.rtpReceived > before.rtpReceived &&
      current.framesDecoded > before.framesDecoded);
    const key = `${name}:stagnantIntervals`;
    manifest.healthStreaks ??= {};
    manifest.healthStreaks[key] = countersAdvanced ? 0 : (manifest.healthStreaks[key] ?? 0) + 1;
    if (before && !countersAdvanced) {
      await recordAnomaly('consumer-media-stalled', name, {
        previousRtpReceived: before?.rtpReceived ?? null,
        currentRtpReceived: current.rtpReceived,
        previousFramesDecoded: before?.framesDecoded ?? null,
        currentFramesDecoded: current.framesDecoded,
        consecutiveIntervals: manifest.healthStreaks[key]
      });
      if (manifest.healthStreaks[key] >= 2) {
        await recordEvent('fatal-health-condition', { kind: 'consumer-media-stalled', participant: name });
        throw new Error(`${name} RTP/decode counters did not advance in two consecutive one-minute observations.`);
      }
    }
    if (before && (!(current.receivedFps > 0) || !(current.decodedFps > 0))) {
      const fpsKey = `${name}:unavailableFpsIntervals`;
      manifest.healthStreaks[fpsKey] = (manifest.healthStreaks[fpsKey] ?? 0) + 1;
      await recordAnomaly('consumer-fps-unavailable', name, {
        receivedFps: current.receivedFps,
        decodedFps: current.decodedFps,
        consecutiveIntervals: manifest.healthStreaks[fpsKey]
      });
      if (manifest.healthStreaks[fpsKey] >= 3) {
        await recordEvent('fatal-health-condition', { kind: 'consumer-fps-unavailable', participant: name });
        throw new Error(`${name} FPS remained unavailable for three consecutive minute observations.`);
      }
    } else {
      manifest.healthStreaks[`${name}:unavailableFpsIntervals`] = 0;
    }
  }

  manifest.lastSnapshot = snapshot;
  monitorSnapshots.push(snapshot);
  await recordEvent('minute-observation', { elapsedMinute: Math.round((snapshot.elapsedMs ?? 0) / 60_000), snapshot });
  return snapshot;
}

function summarizeConsumerUi(snapshot) {
  return {
    peerId: snapshot.peerId,
    socketState: snapshot.socketState,
    role: snapshot.role,
    consumerState: snapshot.consumerState,
    remoteVideo: snapshot.remoteVideo,
    rtpReceived: snapshot.rtpReceived,
    framesDecoded: snapshot.framesDecoded,
    receivedFps: snapshot.receivedFps,
    decodedFps: snapshot.decodedFps,
    packetCount: snapshot.packetCount,
    renderedFrames: renderedFrameCounter(snapshot),
    playbackQualityFrames: snapshot.remoteVideo?.totalVideoFrames ?? null,
    renderedPixelHash: snapshot.renderedPixelHash ?? null,
    documentVisibilityState: snapshot.documentVisibilityState ?? null,
    documentHasFocus: snapshot.documentHasFocus ?? null,
    renderObserver: snapshot.renderObserver,
    measurementStatus: snapshot.measurementStatus
  };
}

function consumerUiHealthy(snapshot) {
  return snapshot.role === 'consumer' && snapshot.socketState === 'connected' &&
    snapshot.consumerState === 'receiving' && snapshot.remoteVideo?.hasStream === true &&
    snapshot.remoteVideo.readyState >= 2 && !snapshot.remoteVideo.paused &&
    snapshot.remoteVideo.width > 0 && snapshot.remoteVideo.height > 0 &&
    snapshot.remoteVideo.tracks?.some(track => track.readyState === 'live');
}

function summarizeDiagnosticConsumer(snapshot) {
  const observer = snapshot.renderObserver;
  return {
    peerId: snapshot.peerId,
    socketState: snapshot.socketState,
    consumerState: snapshot.consumerState,
    rtpReceived: snapshot.rtpReceived,
    packetCount: snapshot.packetCount,
    framesDecoded: snapshot.framesDecoded,
    receivedFps: snapshot.receivedFps,
    decodedFps: snapshot.decodedFps,
    renderedFrames: renderedFrameCounter(snapshot),
    playbackQualityFrames: snapshot.remoteVideo?.totalVideoFrames ?? null,
    renderedPixelHash: snapshot.renderedPixelHash ?? null,
    documentVisibilityState: snapshot.documentVisibilityState ?? null,
    documentHasFocus: snapshot.documentHasFocus ?? null,
    renderSignal: observer?.source ?? 'getVideoPlaybackQuality.totalVideoFrames',
    renderObserver: observer,
    remoteVideo: snapshot.remoteVideo,
    measurementStatus: snapshot.measurementStatus
  };
}

async function captureDiagnosticProbe() {
  const [health, uiProducer, uiConsumer1, uiConsumer2, c1VideoHash, c2VideoHash] = await Promise.all([
    fetchJson(HEALTH_URL),
    readUiSnapshot(pages.producer.producerPage),
    readUiSnapshot(pages.consumer1.page),
    readUiSnapshot(pages.consumer2.page),
    captureVideoHash(pages.consumer1.page).catch(error => ({ error: String(error) })),
    captureVideoHash(pages.consumer2.page).catch(error => ({ error: String(error) }))
  ]);
  uiConsumer1.renderedPixelHash = c1VideoHash.sha256 ?? null;
  uiConsumer1.renderedPixelHashError = c1VideoHash.error ?? null;
  uiConsumer2.renderedPixelHash = c2VideoHash.sha256 ?? null;
  uiConsumer2.renderedPixelHashError = c2VideoHash.error ?? null;
  const currentLayers = Object.fromEntries(['consumer1', 'consumer2'].map(name => {
    const peerId = manifest.participants[name]?.peerId;
    const audit = health.e2eConsumerAudit?.find(item => item.peerId === peerId);
    return [name, {
      preferredLayers: audit?.preferredLayers ?? null,
      currentLayers: audit?.currentLayers ?? null
    }];
  }));
  const probe = {
    wallTime: new Date().toISOString(),
    elapsedMs: elapsedMs(),
    health: {
      ok: health.ok ?? false,
      consumers: health.consumers ?? null,
      inputFps: health.inputFps ?? null,
      injectedChunks: health.injectedChunks ?? null,
      injectedPackets: health.injectedPackets ?? null
    },
    currentLayers,
    producer: {
      peerId: uiProducer.peerId,
      socketState: uiProducer.socketState,
      encodedChunks: uiProducer.encodedChunks,
      packetCount: uiProducer.packetCount,
      inputFps: uiProducer.inputFps,
      localTrackState: uiProducer.localVideo?.tracks?.[0]?.readyState ?? null
    },
    consumer1: summarizeDiagnosticConsumer(uiConsumer1),
    consumer2: summarizeDiagnosticConsumer(uiConsumer2)
  };
  await appendFile(
    path.join(runDir, manifest.files.diagnosticProbes),
    JSON.stringify(probe) + '\n'
  );
  manifest.diagnosticProbeCount += 1;
  diagnosticProbeHistory.push(probe);
  while (diagnosticProbeHistory.length > 10) diagnosticProbeHistory.shift();
  return probe;
}

function detectStalledConsumer(probe) {
  const baseline = diagnosticProbeHistory.findLast(candidate =>
    candidate.elapsedMs <= probe.elapsedMs - STALL_CONFIRMATION_MS
  );
  if (!baseline || probe.elapsedMs - baseline.elapsedMs < STALL_CONFIRMATION_MS) {
    return null;
  }
  for (const participant of ['consumer1', 'consumer2']) {
    const before = baseline[participant];
    const current = probe[participant];
    const metrics = [
      ['rtpPackets', before.rtpReceived, current.rtpReceived],
      ['decodedFrames', before.framesDecoded, current.framesDecoded],
      ['compositorPresentedFrames', before.renderedFrames, current.renderedFrames],
      ['renderedPixels', before.renderedPixelHash, current.renderedPixelHash]
    ];
    const stalledAxes = metrics.flatMap(([name, previous, latest]) => {
      if (name === 'renderedPixels') {
        return typeof previous === 'string' && typeof latest === 'string' &&
          latest === previous ? [name] : [];
      }
      return Number.isFinite(previous) && Number.isFinite(latest) && latest <= previous
        ? [name]
        : [];
    });
    if (stalledAxes.length > 0) {
      return {
        participant,
        detectedAt: probe.wallTime,
        elapsedMs: probe.elapsedMs,
        baselineAt: baseline.wallTime,
        baselineElapsedMs: baseline.elapsedMs,
        confirmationWindowMs: probe.elapsedMs - baseline.elapsedMs,
        stalledAxes,
        baseline: before,
        current
      };
    }
  }
  return null;
}

async function captureDiagnosticSystemSnapshot() {
  const [processInfo, currentLoad, memory] = await Promise.all([
    si.processes().catch(() => null),
    si.currentLoad().catch(() => null),
    si.mem().catch(() => null)
  ]);
  const processList = processInfo?.list ?? [];
  const browserProcess = await processMetrics(
    sharedE2eRuntime.browserProcess?.pid,
    processList
  );
  const backendProcess = await processMetrics(serverRecord?.child?.pid, processList);
  return {
    capturedAt: new Date().toISOString(),
    cpuLoadPercent: currentLoad?.currentLoad ?? null,
    totalRamBytes: memory?.total ?? null,
    availableRamBytes: memory?.available ?? null,
    usedRamBytes: memory?.used ?? null,
    chromeProcess: browserProcess,
    backendProcess,
    soakRunner: process.memoryUsage()
  };
}

async function captureStallEvidence(trigger) {
  const diagnosticId = `${runId}-${trigger.participant}-${trigger.elapsedMs}`;
  const diagnosticDirectory = path.join(runDir, 'diagnostics');
  await mkdir(diagnosticDirectory, { recursive: true });
  const captures = [];
  for (const [index, stage] of ['immediate', 'after-5s'].entries()) {
    if (index > 0) await wait(STALL_EVIDENCE_SPACING_MS);
    const [health, uiProducer, uiConsumer1, uiConsumer2, system] = await Promise.all([
      fetchJson(HEALTH_URL).catch(error => ({ error: error.message })),
      readUiSnapshot(pages.producer.producerPage).catch(error => ({ error: error.message })),
      readUiSnapshot(pages.consumer1.page).catch(error => ({ error: error.message })),
      readUiSnapshot(pages.consumer2.page).catch(error => ({ error: error.message })),
      captureDiagnosticSystemSnapshot().catch(error => ({ error: error.message }))
    ]);
    let sfu = null;
    try {
      sfu = await requestOverAuditedSocket(
        pages[trigger.participant].page,
        'e2eCaptureMediaDiagnostics',
        {}
      );
    } catch (error) {
      sfu = { e2eOk: false, e2eError: error instanceof Error ? error.message : String(error) };
    }
    const screenshots = {};
    const screenshotErrors = {};
    for (const participant of participantNames) {
      const page = participant === 'producer'
        ? pages.producer.producerPage
        : pages[participant].page;
      const filename = `${diagnosticId}-${stage}-${participant}.png`;
      try {
        await page.screenshot({ path: path.join(diagnosticDirectory, filename), fullPage: true });
        screenshots[participant] = path.relative(runDir, path.join(diagnosticDirectory, filename));
      } catch (error) {
        screenshotErrors[participant] = error instanceof Error ? error.message : String(error);
      }
    }
    const snapshot = {
      capturedAt: new Date().toISOString(),
      elapsedMs: elapsedMs(),
      health: health.error ? health : {
        ok: health.ok ?? false,
        inputFps: health.inputFps ?? null,
        injectedChunks: health.injectedChunks ?? null,
        injectedPackets: health.injectedPackets ?? null,
        consumers: health.e2eConsumerAudit ?? null
      },
      producer: uiProducer.error ? uiProducer : {
        peerId: uiProducer.peerId,
        socketState: uiProducer.socketState,
        encodedChunks: uiProducer.encodedChunks,
        packetCount: uiProducer.packetCount,
        inputFps: uiProducer.inputFps,
        localVideo: uiProducer.localVideo
      },
      consumers: {
        consumer1: uiConsumer1.error ? uiConsumer1 : summarizeDiagnosticConsumer(uiConsumer1),
        consumer2: uiConsumer2.error ? uiConsumer2 : summarizeDiagnosticConsumer(uiConsumer2)
      },
      sfu,
      system,
      screenshots,
      screenshotErrors
    };
    captures.push({ stage, ...snapshot });
    await recordEvent('consumer-stall-diagnostic-snapshot', {
      diagnosticId,
      participant: trigger.participant,
      stage,
      snapshot
    });
  }
  const diagnostic = {
    diagnosticId,
    participant: trigger.participant,
    detectedAt: trigger.detectedAt,
    elapsedMs: trigger.elapsedMs,
    baselineAt: trigger.baselineAt,
    baselineElapsedMs: trigger.baselineElapsedMs,
    confirmationWindowMs: trigger.confirmationWindowMs,
    stalledAxes: trigger.stalledAxes,
    recentProbes: diagnosticProbeHistory.slice(),
    captures
  };
  manifest.stallDiagnostics.push(diagnostic);
  manifest.status = 'partial';
  const message = `${trigger.participant} made no progress on ${trigger.stalledAxes.join(', ')} for ${Math.round(trigger.confirmationWindowMs / 1000)} seconds; anomaly-only SFU/browser/system evidence was saved.`;
  await recordAnomaly('consumer-media-stalled', trigger.participant, {
    diagnosticId,
    detectedAt: trigger.detectedAt,
    stalledAxes: trigger.stalledAxes,
    confirmationWindowMs: trigger.confirmationWindowMs
  });
  await recordEvent('consumer-media-stall-confirmed', {
    diagnosticId,
    participant: trigger.participant,
    stalledAxes: trigger.stalledAxes,
    confirmationWindowMs: trigger.confirmationWindowMs
  });
  throw new Error(message);
}

function scheduleForRun(minutes) {
  if (minutes >= 20) {
    const schedule = [
      { atElapsedMs: 0, layer: 'high', spatialLayer: 2 },
      { atElapsedMs: 10 * 60_000, layer: 'low', spatialLayer: 0 },
      { atElapsedMs: 15 * 60_000, layer: 'mid', spatialLayer: 1 },
      { atElapsedMs: 20 * 60_000, layer: 'high', spatialLayer: 2 }
    ];
    return minutes === 20 ? schedule.slice(0, 3) : schedule;
  }
  return [
    { atElapsedMs: 0, layer: 'high', spatialLayer: 2 },
    { atElapsedMs: 45_000, layer: 'low', spatialLayer: 0 },
    { atElapsedMs: 90_000, layer: 'mid', spatialLayer: 1 },
    { atElapsedMs: 120_000, layer: 'high', spatialLayer: 2 }
  ];
}

function formatScheduleBoundary(elapsedMs) {
  const minutes = Math.floor(elapsedMs / 60_000);
  const seconds = Math.floor((elapsedMs % 60_000) / 1000);
  return `scheduled ${minutes}:${String(seconds).padStart(2, '0')} boundary`;
}

async function selectAndVerifyLayer(participant, layer, reason) {
  const page = pages[participant].page;
  const peerId = manifest.participants[participant].peerId;
  const requestedAt = new Date().toISOString();
  const response = await requestOverAuditedSocket(page, 'setConsumerLayer', { layer });
  const responseAt = new Date().toISOString();
  const result = {
    participant,
    reason,
    requestedAt,
    requestedLayer: layer,
    requestedSpatialLayer: { low: 0, mid: 1, high: 2 }[layer],
    signalingResponseAt: responseAt,
    signalingOk: response.e2eOk === true,
    response: {
      layer: response.layer ?? null,
      spatialLayer: response.spatialLayer ?? null,
      preferredLayers: response.preferredLayers ?? null,
      actionSentAt: response.actionSentAt ?? null,
      error: response.e2eError ?? null
    }
  };
  if (response.e2eOk === true) {
    const stable = await waitForStableConsumerSpatialLayer(peerId, layer, 30_000);
    result.stableLayer = stable;
    result.actualSpatialLayer = stable.status === 'reached'
      ? stable.targetSpatialLayer
      : stable.lastObservedCurrentLayers?.spatialLayer ?? null;
    result.actualLayerReachedAt = stable.stableSamples?.at(-1)?.sampledAt ?? null;
  } else {
    result.stableLayer = null;
    result.actualSpatialLayer = null;
    result.actualLayerReachedAt = null;
  }
  result.success = result.signalingOk && result.actualSpatialLayer === result.requestedSpatialLayer;
  layerChanges.push(result);
  if (!result.success) {
    await recordAnomaly('layer-transition-not-confirmed', participant, result);
  }
  await recordEvent('layer-change', result);
  return result;
}

function finalFilePath(participant, stage, attempt, originalFilename) {
  const directory = path.join(runDir, participant, stage);
  const suffix = attempt === 0 ? '' : `-retry${attempt}`;
  const basename = path.basename(originalFilename).replace(/\.json$/i, '') + suffix + '.json';
  return { directory, absolute: path.join(directory, basename), relative: path.relative(runDir, path.join(directory, basename)) };
}

function expectedPeerId(participant) {
  return manifest.participants[participant].peerId;
}

async function downloadParticipant(participant, stage) {
  const page = participant === 'producer'
    ? pages.producer.producerPage
    : pages[participant].page;
  let lastError;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const labelPart = `${runId}-${stage}-${participant}${attempt === 0 ? '' : `-retry${attempt}`}`;
    const startedAt = new Date().toISOString();
    try {
      const result = await downloadStatsDumpFromPage(page, labelPart);
      const destination = finalFilePath(participant, stage, attempt, result.filename);
      await mkdir(destination.directory, { recursive: true });
      const sourcePath = path.join(sharedE2eRuntime.tempDir, 'downloads', labelPart, result.filename);
      await copyFile(sourcePath, destination.absolute);
      const fileInfo = await stat(destination.absolute);
      ensure(fileInfo.isFile() && fileInfo.size > 0, `${participant}/${stage} JSON is missing or empty after copy.`);
      const parsedDump = JSON.parse(await readFile(destination.absolute, 'utf8'));
      const expectedRole = participant === 'producer' ? 'producer' : 'consumer';
      ensure(parsedDump.schemaVersion === 1, `${participant}/${stage} JSON is not schemaVersion 1.`);
      ensure(parsedDump.metadata?.role === expectedRole, `${participant}/${stage} JSON has the wrong role.`);
      ensure(parsedDump.metadata?.peerId === expectedPeerId(participant), `${participant}/${stage} JSON peerId does not match the live page.`);
      ensure(parsedDump.metadata?.simulcastEnabled === true, `${participant}/${stage} JSON does not report Simulcast ON.`);
      ensure(parsedDump.metadata?.collectionStatus === 'recording', `${participant}/${stage} JSON collection is not still recording.`);
      ensure(parsedDump.metadata?.sampleIntervalMs === 1000, `${participant}/${stage} JSON does not use the existing 1-second sample interval.`);
      ensure(Number.isFinite(Date.parse(parsedDump.metadata?.measurementStartTime ?? '')) &&
        Number.isFinite(Date.parse(parsedDump.metadata?.dumpTime ?? '')),
      `${participant}/${stage} JSON has invalid measurement start or dump timestamps.`);
      const checked = participant === 'producer'
        ? verifyProducerStatsDump({ dump: parsedDump, filename: result.filename }, true)
        : verifyConsumerStatsDump({ dump: parsedDump, filename: result.filename });
      const samples = participant === 'producer' ? parsedDump.producerStats : parsedDump.consumerStats;
      ensure(samples.length >= 2, `${participant}/${stage} JSON has too few samples.`);
      ensure(samples.every(sample => typeof sample.timestamp === 'string' && Number.isFinite(Date.parse(sample.timestamp))),
        `${participant}/${stage} JSON contains an invalid sample timestamp.`);
      const saved = {
        participant,
        stage,
        attempt,
        startedAt,
        finishedAt: new Date().toISOString(),
        relativePath: destination.relative,
        filename: path.basename(destination.absolute),
        fileBytes: fileInfo.size,
        schemaVersion: parsedDump.schemaVersion,
        role: parsedDump.metadata.role,
        peerId: parsedDump.metadata.peerId,
        sampleCount: samples.length,
        sampleIntervalMs: parsedDump.metadata.sampleIntervalMs,
        measurementStartTime: parsedDump.metadata.measurementStartTime,
        measurementEndTime: parsedDump.metadata.measurementEndTime ?? null,
        dumpTime: parsedDump.metadata.dumpTime,
        collectionStatus: parsedDump.metadata.collectionStatus,
        timestampRange: {
          first: samples[0]?.timestamp ?? null,
          last: samples.at(-1)?.timestamp ?? null
        },
        extraValidation: checked,
        validated: true
      };
      downloads.push(saved);
      await recordEvent('download-completed', saved);
      return saved;
    } catch (error) {
      lastError = error;
      await recordEvent(attempt === 0 ? 'download-failed' : 'download-retry-failed', {
        participant,
        stage,
        attempt,
        startedAt,
        failedAt: new Date().toISOString(),
        reason: error instanceof Error ? error.message : String(error)
      });
    }
  }
  throw new Error(`${participant}/${stage} stats download failed after one retry: ${lastError instanceof Error ? lastError.message : String(lastError)}`);
}

async function downloadCheckpoint(stage) {
  const startedAt = new Date().toISOString();
  await recordEvent('checkpoint-started', { stage, startedAt });
  const saved = [];
  for (const participant of participantNames) {
    saved.push(await downloadParticipant(participant, stage));
  }
  const finishedAt = new Date().toISOString();
  await recordEvent('checkpoint-finished', { stage, startedAt, finishedAt, downloads: saved.map(item => item.relativePath) });
  return saved;
}

async function visualizerRun(browser, finalFiles) {
  const baseUrl = PRODUCTION_APP_URL.replace(/\/$/, '') + '/stats-visualizer.html';
  const participants = {};
  const pageErrors = [];
  const page = await browser.newPage();
  page.on('pageerror', error => pageErrors.push(error.message));
  page.on('console', message => {
    if (message.type() === 'error') pageErrors.push(message.text());
  });
  try {
    const response = await page.goto(baseUrl, { waitUntil: 'domcontentloaded' });
    ensure(response?.status() === 200, 'Actual-data Visualizer returned a non-200 response.');
    await page.waitForFunction(() => document.documentElement.dataset.statsVisualizerReady === 'true');
    for (const participant of participantNames) {
      const item = downloads.findLast(entry => entry.stage === 'final' && entry.participant === participant && entry.validated);
      ensure(item, `Missing final validated JSON for ${participant} Visualizer load.`);
      const filePath = path.resolve(runDir, item.relativePath);
      const startedAt = Date.now();
      await (await page.$('#stats-file')).uploadFile(filePath);
      const chartCount = participant === 'producer' ? 5 : 6;
      await page.waitForFunction(expected => {
        const renderer = document.querySelector('#renderer-status')?.textContent ?? '';
        const charts = document.querySelectorAll('#charts canvas').length;
        const dashboard = document.querySelector('#dashboard');
        return dashboard && !dashboard.hidden && renderer === 'Renderer: Chart.js' && charts === expected;
      }, { timeout: 120_000 }, chartCount);
      const result = await page.evaluate(() => ({
        title: document.querySelector('#dump-title')?.textContent ?? '',
        metadata: document.querySelector('#metadata')?.textContent ?? '',
        renderer: document.querySelector('#renderer-status')?.textContent ?? '',
        canvasCount: document.querySelectorAll('#charts canvas').length,
        svgCount: document.querySelectorAll('#charts svg').length,
        error: document.querySelector('#load-error')?.textContent ?? '',
        status: document.querySelector('#load-status')?.textContent ?? ''
      }));
      ensure(result.title.toLowerCase().includes(participant === 'producer' ? 'producer' : 'consumer'),
        `Visualizer loaded the wrong role for ${participant}.`);
      ensure(result.metadata.includes(expectedPeerId(participant)), `Visualizer omitted ${participant} peerId metadata.`);
      ensure(!result.error, `Visualizer rejected ${participant} final JSON: ${result.error}`);
      participants[participant] = {
        status: 'passed',
        file: item.relativePath,
        fileBytes: item.fileBytes,
        sampleCount: item.sampleCount,
        role: participant === 'producer' ? 'producer' : 'consumer',
        renderer: result.renderer,
        canvasCount: result.canvasCount,
        svgCount: result.svgCount,
        renderMs: Date.now() - startedAt,
        loadStatus: result.status
      };
    }
    const repeated = downloads.findLast(entry => entry.stage === 'final' && entry.participant === 'producer' && entry.validated);
    await (await page.$('#stats-file')).uploadFile(path.resolve(runDir, repeated.relativePath));
    await page.waitForFunction(() => document.querySelectorAll('#charts canvas').length === 5 &&
      document.querySelector('#renderer-status')?.textContent === 'Renderer: Chart.js');
    return { status: 'passed', url: baseUrl, participants, repeatedProducerLoad: true, pageErrors };
  } finally {
    await page.close().catch(() => {});
  }
}

async function visualizerFallback(browser, finalFiles) {
  const baseUrl = PRODUCTION_APP_URL.replace(/\/$/, '') + '/stats-visualizer.html';
  const page = await browser.newPage();
  let blockedChartRequests = 0;
  const pageErrors = [];
  page.on('pageerror', error => pageErrors.push(error.message));
  await page.setRequestInterception(true);
  page.on('request', request => {
    if (/\/assets\/chartjs-[^/]+\.js(?:\?.*)?$/.test(request.url())) {
      blockedChartRequests += 1;
      void request.abort('failed').catch(() => {});
    } else {
      void request.continue().catch(() => {});
    }
  });
  try {
    const response = await page.goto(baseUrl, { waitUntil: 'domcontentloaded' });
    ensure(response?.status() === 200, 'Actual-data SVG fallback page returned a non-200 response.');
    await page.waitForSelector('#stats-file');
    for (const participant of ['producer', 'consumer1', 'consumer2']) {
      const item = downloads.findLast(entry => entry.stage === 'final' && entry.participant === participant && entry.validated);
      ensure(item, `Missing final JSON for SVG fallback ${participant}.`);
      const startedAt = Date.now();
      await (await page.$('#stats-file')).uploadFile(path.resolve(runDir, item.relativePath));
      const expectedCharts = participant === 'producer' ? 5 : 6;
      await page.waitForFunction(expected =>
        document.querySelector('#renderer-status')?.textContent === 'Renderer: SVG (Chart.js unavailable)' &&
        document.querySelectorAll('#charts svg').length === expected,
      { timeout: 120_000 }, expectedCharts);
      ensure(!(await page.$eval('#load-error', node => !node.hidden)),
        `SVG fallback rejected actual ${participant} JSON.`);
      finalFiles.svgFallbackParticipants[participant] = {
        status: 'passed',
        svgCount: await page.$$eval('#charts svg', nodes => nodes.length),
        canvasCount: await page.$$eval('#charts canvas', nodes => nodes.length),
        renderMs: Date.now() - startedAt,
        file: item.relativePath
      };
    }
    ensure(blockedChartRequests >= 1, 'Chart.js module request was not blocked during fallback validation.');
    ensure(pageErrors.length === 0, `SVG fallback logged page errors: ${pageErrors.join(' | ')}`);
    return { status: 'passed', blockedChartRequests, participants: finalFiles.svgFallbackParticipants, pageErrors };
  } finally {
    await page.close().catch(() => {});
  }
}

async function validateFinalFilesAndVisualizer(browser) {
  const result = {
    finalJson: {},
    svgFallbackParticipants: {},
    chartJs: null,
    svgFallback: null
  };
  for (const participant of participantNames) {
    const item = downloads.findLast(entry => entry.stage === 'final' && entry.participant === participant && entry.validated);
    if (!item) throw new Error(`Final JSON is missing for ${participant}.`);
    const filePath = path.resolve(runDir, item.relativePath);
    const bytes = await stat(filePath);
    const parsed = JSON.parse(await readFile(filePath, 'utf8'));
    ensure(bytes.size === item.fileBytes && bytes.size > 0, `${participant} final JSON size changed after validation.`);
    ensure(parsed.schemaVersion === 1 && parsed.metadata?.peerId === expectedPeerId(participant),
      `${participant} final JSON failed final file-system validation.`);
    result.finalJson[participant] = {
      relativePath: item.relativePath,
      exists: true,
      fileBytes: bytes.size,
      schemaVersion: parsed.schemaVersion,
      role: parsed.metadata.role,
      peerId: parsed.metadata.peerId,
      sampleCount: participant === 'producer' ? parsed.producerStats.length : parsed.consumerStats.length,
      dumpTime: parsed.metadata.dumpTime,
      collectionStatus: parsed.metadata.collectionStatus,
      measurementStartTime: parsed.metadata.measurementStartTime,
      measurementEndTime: parsed.metadata.measurementEndTime ?? null,
      firstSample: (participant === 'producer' ? parsed.producerStats : parsed.consumerStats)[0]?.timestamp ?? null,
      lastSample: (participant === 'producer' ? parsed.producerStats : parsed.consumerStats).at(-1)?.timestamp ?? null
    };
  }
  result.chartJs = await visualizerRun(browser, result.finalJson);
  result.svgFallback = await visualizerFallback(browser, result);
  return result;
}

async function prepareRun() {
  await mkdir(SOAK_ROOT, { recursive: true });
  await mkdir(runDir, { recursive: false });
  for (const name of participantNames) await mkdir(path.join(runDir, name), { recursive: true });
  sharedE2eRuntime.tempDir = await mkdtemp(path.join(os.tmpdir(), 'webrtc-sfu-media-e2e-'));
  await mkdir(path.join(sharedE2eRuntime.tempDir, 'logs'), { recursive: true });
  manifest.runDirectory = path.relative(ROOT, runDir);
  manifest.backendPort = SIGNALING_PORT;
  await persistManifest();
}

async function startExperiment() {
  await assertPortFree(SIGNALING_PORT);
  await assertPortFree(DEV_PORT);
  const pattern = await startPatternServer();
  manifest.patternPage = { url: pattern.url, title: PATTERN_TITLE, generatedFile: path.basename(pattern.file) };
  await launchTestBrowser();
  const browser = sharedE2eRuntime.browser;
  manifest.browserStartedAt = new Date().toISOString();
  serverRecord = startNpmScript(
    'stats-soak-server',
    'server:prod',
    [],
    { SFU_DEBUG: '0', SFU_E2E_MEDIA: '1', NODE_ENV: 'production' }
  );
  previewRecord = startNpmScript(
    'stats-soak-preview',
    'preview',
    [],
    { SFU_DEBUG: '0', NODE_ENV: 'production' }
  );
  setCurrentServerRecord(serverRecord);
  await waitForHealth(serverRecord, health => health.ok === true, '30-minute soak production /health');
  producerCycle = await startProducerCycle(
    'production',
    PRODUCTION_APP_URL,
    `stats-soak-producer-${runId}`,
    createProducerCycleTrace(),
    { simulcastEnabled: true }
  );
  pages.producer = producerCycle;
  pages.consumer1 = await startConsumerPage('production', PRODUCTION_APP_URL, `stats-soak-consumer1-${runId}`);
  pages.consumer2 = await startConsumerPage('production', PRODUCTION_APP_URL, `stats-soak-consumer2-${runId}`);
  await Promise.all([
    waitForConsumerReady(pages.consumer1.page, 'soak Consumer 1 media and FPS'),
    waitForConsumerReady(pages.consumer2.page, 'soak Consumer 2 media and FPS')
  ]);
  const renderObservers = await Promise.all([
    installRenderedFrameObserver(pages.consumer1.page),
    installRenderedFrameObserver(pages.consumer2.page)
  ]);

  const beforeHashes = await Promise.all([
    captureVideoHash(pages.consumer1.page),
    captureVideoHash(pages.consumer2.page)
  ]);
  const beforeUi = await Promise.all([
    readUiSnapshot(pages.consumer1.page),
    readUiSnapshot(pages.consumer2.page)
  ]);
  await wait(1600);
  const afterHashes = await Promise.all([
    captureVideoHash(pages.consumer1.page),
    captureVideoHash(pages.consumer2.page)
  ]);
  const afterUi = await Promise.all([
    readUiSnapshot(pages.consumer1.page),
    readUiSnapshot(pages.consumer2.page)
  ]);
  for (let index = 0; index < 2; index += 1) {
    ensure(beforeHashes[index].sha256 !== afterHashes[index].sha256,
      `Consumer ${index + 1} video pixels did not change in the moving-source readiness check.`);
    ensure(afterUi[index].rtpReceived > beforeUi[index].rtpReceived &&
      afterUi[index].framesDecoded > beforeUi[index].framesDecoded,
    `Consumer ${index + 1} did not advance received RTP and decoded frames before T0.`);
  }

  const uiProducer = await readUiSnapshot(pages.producer.producerPage);
  const uiConsumer1 = afterUi[0];
  const uiConsumer2 = afterUi[1];
  manifest.participants = {
    producer: { role: 'producer', peerId: uiProducer.peerId, pageLabel: 'Producer' },
    consumer1: { role: 'consumer', peerId: uiConsumer1.peerId, pageLabel: 'Consumer 1' },
    consumer2: { role: 'consumer', peerId: uiConsumer2.peerId, pageLabel: 'Consumer 2' }
  };
  ensure(Object.values(manifest.participants).every(item => typeof item.peerId === 'string' && item.peerId.length > 0),
    'One or more participant peerIds could not be read from the UI.');

  const initialHigh = [];
  for (const participant of ['consumer1', 'consumer2']) {
    initialHigh.push(await selectAndVerifyLayer(participant, 'high', 'initial high before T0'));
  }
  ensure(initialHigh.every(item => item.success),
    'Both Consumers must reach actual HIGH spatialLayer before starting the shared measurement interval.');

  const health = await fetchJson(HEALTH_URL);
  ensure(health.ok === true && health.simulcastEnabled === true && health.consumers === 2,
    'T0 requires one active Simulcast Producer and two connected Consumers.');
  ensure(!serverRecord.exit && !previewRecord.exit && browser.connected,
    'A soak process exited before T0.');
  const capture = await pages.producer.producerPage.evaluate(() => {
    const video = document.querySelector('#localVideo');
    const track = video?.srcObject?.getVideoTracks?.()[0];
    const settings = track?.getSettings?.() ?? {};
    return {
      videoWidth: video?.videoWidth ?? null,
      videoHeight: video?.videoHeight ?? null,
      trackWidth: settings.width ?? null,
      trackHeight: settings.height ?? null,
      frameRate: settings.frameRate ?? null,
      displaySurface: settings.displaySurface ?? null,
      trackReadyState: track?.readyState ?? null
    };
  });
  const trace = producerCycle.producerMetrics;
  ensure(capture.trackReadyState === 'live' && capture.videoWidth > 0 && capture.videoHeight > 0,
    'Actual Chrome display capture is not live with a decoded Producer preview at T0.');
  manifest.environment = await environmentSnapshot(browser);
  manifest.environment.capture = {
    sourceTitle: PATTERN_TITLE,
    canvasSize: '1280x720',
    actual: capture,
    producerCodecEvidence: trace.actualEncoderCodec ?? null,
    consumerRenderObservers: renderObservers,
    producerStartMetrics: {
      inputFps: trace.inputFps ?? null,
      encodedChunks: trace.deltas?.encodedChunks ?? null,
      codecEvidenceSource: trace.codecEvidenceSource ?? null
    }
  };
  manifest.environment.chrome.appliedFlagsVerified = true;
  manifest.environment.chrome.noFakeMediaFlags = true;
  await atomicWrite(path.join(runDir, 'environment.json'), JSON.stringify(manifest.environment, null, 2) + '\n');

  t0Monotonic = performance.now();
  t0WallTime = new Date().toISOString();
  manifest.t0 = {
    wallTime: t0WallTime,
    monotonicMs: Math.round(t0Monotonic),
    definition: 'all three participants active, both Consumer videos advancing, real captured media confirmed, and both current spatial layers stably HIGH'
  };
  manifest.schedule = scheduleForRun(durationMinutes);
  manifest.status = 'running';
  manifest.startedAt = t0WallTime;
  await recordEvent('t0-all-participants-ready', {
    health: {
      consumers: health.consumers,
      simulcastEnabled: health.simulcastEnabled,
      activeLayers: health.activeLayers
    },
    capture,
    participants: manifest.participants,
    initialHighLayerChanges: initialHigh
  });
  return browser;
}

async function runTimedExperiment(browser) {
  const targetMs = durationMinutes * 60_000;
  let nextMonitorAt = Math.min(60_000, targetMs);
  let monitorBaseline = await monitorOnce(null);
  let nextDiagnosticProbeAt = Math.min(DIAGNOSTIC_PROBE_INTERVAL_MS, targetMs);
  await captureDiagnosticProbe();
  let scheduleIndex = durationMinutes === 30 ? 1 : 1;
  const checkpointTimes = durationMinutes >= 20 ? [9 * 60_000, 19 * 60_000] : [];
  let checkpointIndex = 0;
  while (true) {
    checkInterrupted();
    const currentElapsed = elapsedMs() ?? 0;
    if (currentElapsed >= targetMs) break;
    const nextLayer = manifest.schedule[scheduleIndex];
    const nextCheckpointAt = checkpointTimes[checkpointIndex] ?? Infinity;
    const nextTarget = Math.min(
      nextMonitorAt,
      nextDiagnosticProbeAt,
      nextLayer?.atElapsedMs ?? Infinity,
      nextCheckpointAt,
      targetMs
    );
    if (nextTarget > currentElapsed) await wait(nextTarget - currentElapsed);
    const nowElapsed = elapsedMs() ?? 0;
    if (nextLayer && nowElapsed >= nextLayer.atElapsedMs) {
      const transition = await selectAndVerifyLayer(
        'consumer2',
        nextLayer.layer,
        formatScheduleBoundary(nextLayer.atElapsedMs)
      );
      if (!transition.success) {
        manifest.status = 'partial';
        manifest.errors.push(`Consumer 2 did not reach the requested ${nextLayer.layer.toUpperCase()} layer at its scheduled boundary.`);
      }
      scheduleIndex += 1;
    }
    if (nowElapsed >= nextCheckpointAt) {
      const label = checkpointIndex === 0 ? 'checkpoint-09m' : 'checkpoint-19m';
      try {
        await downloadCheckpoint(label);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        manifest.errors.push(message);
        manifest.status = 'partial';
        await recordAnomaly('checkpoint-download-failed', null, { stage: label, error: message });
      }
      checkpointIndex += 1;
    }
    if (nowElapsed >= nextMonitorAt) {
      monitorBaseline = await monitorOnce(monitorBaseline);
      nextMonitorAt = Math.min(nextMonitorAt + 60_000, targetMs);
    }
    if (nowElapsed >= nextDiagnosticProbeAt) {
      const probe = await captureDiagnosticProbe();
      nextDiagnosticProbeAt = Math.min(
        (elapsedMs() ?? nowElapsed) + DIAGNOSTIC_PROBE_INTERVAL_MS,
        targetMs
      );
      const stalledConsumer = detectStalledConsumer(probe);
      if (stalledConsumer) await captureStallEvidence(stalledConsumer);
    }
  }
  manifest.measurementDeadlineReached = (elapsedMs() ?? 0) >= targetMs;
  manifest.finishedAt = new Date().toISOString();
  await recordEvent('measurement-deadline-reached', { elapsedMs: elapsedMs(), finishedAt: manifest.finishedAt });
  await downloadCheckpoint('final');
  manifest.visualizer = await validateFinalFilesAndVisualizer(browser);
  manifest.status = manifest.errors.length === 0 && anomalies.length === 0 && manifest.measurementDeadlineReached
    ? 'completed'
    : 'partial';
  await recordEvent('experiment-data-and-visualizer-complete', {
    status: manifest.status,
    visualizer: manifest.visualizer
  });
}

async function persistFailureDumps() {
  if (!manifest.t0) return;
  for (const participant of participantNames) {
    const alreadySaved = downloads.some(item => item.stage === 'partial-final' && item.participant === participant && item.validated);
    if (alreadySaved) continue;
    try {
      await downloadParticipant(participant, 'partial-final');
    } catch (error) {
      manifest.errors.push(`Partial dump unavailable for ${participant}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
}

async function main() {
  await prepareRun();
  await recordEvent('run-directory-created', { runDirectory: manifest.runDirectory, backendPort: SIGNALING_PORT, browserPort: DEV_PORT });
  let browser = null;
  let failure = null;
  try {
    browser = await startExperiment();
    await runTimedExperiment(browser);
  } catch (error) {
    failure = error instanceof Error ? error : new Error(String(error));
    abortCause = failure.message;
    manifest.errors.push(failure.message);
    manifest.status = manifest.t0 ? 'partial' : 'failed-before-t0';
    await recordEvent('experiment-error', { error: failure.message });
    await persistFailureDumps();
  } finally {
    manifest.finishedAt ??= new Date().toISOString();
    try {
      await cleanupRuntime();
      manifest.cleanup = {
        browserClosed: sharedE2eReport.cleanup.browserClosed,
        childProcessesStopped: sharedE2eReport.cleanup.childProcessesStopped,
        patternServerStopped: sharedE2eReport.cleanup.patternServerStopped,
        temporaryProfileRemoved: sharedE2eReport.cleanup.tempDirectoryRemoved,
        ownedProcesses: sharedE2eReport.runtimeLogs?.map(item => ({
          name: item.name,
          exit: item.exit,
          stoppedByHarness: item.stoppedByHarness,
          spawnError: item.spawnError
        })) ?? []
      };
      await atomicWrite(path.join(runDir, 'runtime-logs.json'), JSON.stringify(sharedE2eReport.runtimeLogs ?? [], null, 2) + '\n');
    } catch (error) {
      manifest.errors.push(`Cleanup failed: ${error instanceof Error ? error.message : String(error)}`);
      manifest.cleanup = { status: 'failed', error: error instanceof Error ? error.message : String(error) };
    }
    if (manifest.cleanup && Object.values(manifest.cleanup).some(value => value === false)) {
      manifest.status = 'partial';
      manifest.errors.push('Owned browser/server/pattern resources were not all confirmed clean.');
    }
    manifest.interruptedBy = abortCause;
    await persistManifest();
  }

  try {
    const analysis = await analyzeRun(runDir);
    manifest.analysisStatus = analysis.analysisStatus;
    await persistManifest();
  } catch (error) {
    manifest.errors.push(`Analysis failed: ${error instanceof Error ? error.message : String(error)}`);
    manifest.analysisStatus = 'failed';
    await persistManifest();
    failure ??= error instanceof Error ? error : new Error(String(error));
  }
  console.log(`Run directory: ${runDir}`);
  console.log(`Status: ${manifest.status}`);
  console.log(`Report: ${path.join(runDir, 'report.md')}`);
  if (manifest.status !== 'completed' || failure) {
    console.error(`Stats soak ${manifest.status}: ${manifest.errors.join(' | ') || 'check the run report for anomalies'}`);
    process.exitCode = 1;
  } else {
    console.log(`Stats soak ${durationMinutes} min: PASS`);
  }
}

await main();
