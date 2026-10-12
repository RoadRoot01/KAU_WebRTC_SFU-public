#!/usr/bin/env node

import { readFile, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function percentile(sortedValues, fraction) {
  if (sortedValues.length === 0) return null;
  const index = Math.max(0, Math.ceil(fraction * sortedValues.length) - 1);
  return sortedValues[Math.min(index, sortedValues.length - 1)];
}

function summarize(values) {
  const numeric = values.filter(value => typeof value === 'number' && Number.isFinite(value));
  const sorted = [...numeric].sort((left, right) => left - right);
  const nullCount = values.length - numeric.length;
  return {
    sampleCount: values.length,
    validCount: numeric.length,
    nullCount,
    unavailablePercent: values.length > 0 ? (nullCount / values.length) * 100 : null,
    mean: numeric.length > 0
      ? numeric.reduce((sum, value) => sum + value, 0) / numeric.length
      : null,
    median: percentile(sorted, 0.5),
    min: sorted[0] ?? null,
    p05: percentile(sorted, 0.05),
    p95: percentile(sorted, 0.95),
    max: sorted.at(-1) ?? null
  };
}

function maxNullRun(values) {
  let current = 0;
  let maximum = 0;
  for (const value of values) {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      current += 1;
      maximum = Math.max(maximum, current);
    } else {
      current = 0;
    }
  }
  return maximum;
}

function analyzeTimestamps(samples) {
  const parsed = samples.map(sample => Date.parse(sample?.timestamp));
  let monotonicViolations = 0;
  let duplicateTimestamps = 0;
  const intervals = [];
  for (let index = 1; index < parsed.length; index += 1) {
    const previous = parsed[index - 1];
    const current = parsed[index];
    if (!Number.isFinite(previous) || !Number.isFinite(current)) continue;
    const delta = current - previous;
    if (delta === 0) duplicateTimestamps += 1;
    if (delta <= 0) monotonicViolations += 1;
    else intervals.push(delta);
  }
  const sorted = intervals.toSorted((left, right) => left - right);
  const validTimes = parsed.filter(Number.isFinite);
  return {
    validTimestampCount: validTimes.length,
    invalidTimestampCount: parsed.length - validTimes.length,
    firstTimestamp: validTimes.length > 0 ? new Date(validTimes[0]).toISOString() : null,
    lastTimestamp: validTimes.length > 0
      ? new Date(validTimes.at(-1)).toISOString()
      : null,
    measuredSpanMs: validTimes.length > 1 ? validTimes.at(-1) - validTimes[0] : 0,
    monotonicViolations,
    duplicateTimestamps,
    intervalMs: summarize(intervals),
    intervalsOver2s: intervals.filter(interval => interval > 2000).length,
    intervalsOver3s: intervals.filter(interval => interval > 3000).length,
    maxIntervalMs: sorted.at(-1) ?? null
  };
}

function analyzeCounters(samples, fields) {
  return Object.fromEntries(fields.map(field => {
    const values = samples.map(sample => sample?.[field]);
    const numeric = values.filter(value => typeof value === 'number' && Number.isFinite(value));
    let decreases = 0;
    for (let index = 1; index < numeric.length; index += 1) {
      if (numeric[index] < numeric[index - 1]) decreases += 1;
    }
    return [field, {
      first: numeric[0] ?? null,
      last: numeric.at(-1) ?? null,
      observedNetDelta: numeric.length > 1 ? numeric.at(-1) - numeric[0] : null,
      decreasingSteps: decreases,
      negativeSampleCount: numeric.filter(value => value < 0).length,
      unavailableCount: values.length - numeric.length
    }];
  }));
}

function roleSamples(dump) {
  if (Array.isArray(dump.samples)) return dump.samples;
  return dump.metadata.role === 'producer' ? dump.producerStats : dump.consumerStats;
}

function analyzeParticipant(name, download, expected) {
  const { dump, path: filePath, relativePath } = download;
  const samples = roleSamples(dump);
  const timestamps = analyzeTimestamps(samples);
  const fields = dump.metadata.role === 'producer'
    ? {
        sourceFps: summarize(samples.map(sample => sample.sourceFps)),
        lowEncodedFps: summarize(samples.map(sample => sample.layers?.low?.encodedFps)),
        midEncodedFps: summarize(samples.map(sample => sample.layers?.mid?.encodedFps)),
        highEncodedFps: summarize(samples.map(sample => sample.layers?.high?.encodedFps)),
        lowPayloadBitrateKbps: summarize(samples.map(sample => sample.layers?.low?.encodedPayloadBitrateKbps)),
        midPayloadBitrateKbps: summarize(samples.map(sample => sample.layers?.mid?.encodedPayloadBitrateKbps)),
        highPayloadBitrateKbps: summarize(samples.map(sample => sample.layers?.high?.encodedPayloadBitrateKbps))
      }
    : {
        receivedFps: summarize(samples.map(sample => sample.receivedFps)),
        decodedFps: summarize(samples.map(sample => sample.decodedFps)),
        receivedMediaBitrateKbps: summarize(samples.map(sample => sample.receivedMediaBitrateKbps)),
        jitterMs: summarize(samples.map(sample => sample.maxJitterMs)),
        framesDropped: summarize(samples.map(sample => sample.framesDropped))
      };
  const nullableFields = dump.metadata.role === 'producer'
    ? ['sourceFps', 'layers.low.encodedFps', 'layers.mid.encodedFps', 'layers.high.encodedFps']
    : ['receivedFps', 'decodedFps', 'receivedMediaBitrateKbps'];
  const nullRuns = Object.fromEntries(nullableFields.map(field => {
    const values = samples.map(sample => field.split('.').reduce((value, key) => value?.[key], sample));
    return [field, { nullSamples: values.filter(value => value == null).length, maxConsecutiveNullSamples: maxNullRun(values) }];
  }));
  const counters = dump.metadata.role === 'producer'
    ? analyzeCounters(samples, ['sourceFramesTotal'])
    : analyzeCounters(samples, [
        'packetsReceived', 'packetsLost', 'framesReceived', 'framesDecoded',
        'framesDropped', 'bytesReceived'
      ]);

  const validation = {
    fileExists: true,
    fileBytes: stat(download.filePath).then(info => info.size),
    jsonParsed: true,
    schemaVersion: dump.schemaVersion === 1,
    roleMatches: dump.metadata.role === expected.role,
    peerIdMatches: dump.metadata.peerId === expected.peerId,
    sampleCount: samples.length,
    sampleIntervalMs: dump.metadata.sampleIntervalMs,
    collectionStatus: dump.metadata.collectionStatus ?? null,
    measurementStartTime: dump.metadata.measurementStartTime ?? null,
    measurementEndTime: dump.metadata.measurementEndTime ?? null,
    dumpTime: dump.metadata.dumpTime ?? null,
    retainedLimitReached: dump.metadata.collectionStatus === 'limit-reached',
    timestamps
  };
  return Promise.resolve(validation.fileBytes).then(fileBytes => ({
    name,
    relativePath,
    validation: { ...validation, fileBytes },
    fields,
    counters,
    nullRuns,
    samples
  }));
}

function phaseWindowSummaries(participants, manifest) {
  const t0 = Date.parse(manifest.t0?.wallTime ?? '');
  if (!Number.isFinite(t0)) return [];
  const phases = manifest.durationMinutes >= 25
    ? [
        { label: '0-10 min HIGH', startMs: 0, endMs: 10 * 60_000, layer: 'high' },
        { label: '10-15 min LOW', startMs: 10 * 60_000, endMs: 15 * 60_000, layer: 'low' },
        { label: '15-20 min MID', startMs: 15 * 60_000, endMs: 20 * 60_000, layer: 'mid' },
        { label: `20-${manifest.durationMinutes} min HIGH`, startMs: 20 * 60_000, endMs: manifest.durationMinutes * 60_000, layer: 'high' }
      ]
    : manifest.durationMinutes >= 20
      ? [
          { label: '0-10 min HIGH', startMs: 0, endMs: 10 * 60_000, layer: 'high' },
          { label: '10-15 min LOW', startMs: 10 * 60_000, endMs: 15 * 60_000, layer: 'low' },
          { label: '15-20 min MID', startMs: 15 * 60_000, endMs: manifest.durationMinutes * 60_000, layer: 'mid' }
        ]
    : (manifest.schedule ?? []).map((item, index, list) => ({
        label: `${item.layer.toUpperCase()} rehearsal phase`,
        startMs: item.atElapsedMs,
        endMs: list[index + 1]?.atElapsedMs ?? manifest.durationMinutes * 60_000,
        layer: item.layer
      }));
  const consumer2Transitions = (manifest.layerChanges ?? [])
    .filter(change => change.participant === 'consumer2');
  const layerObservationSummary = (name, startMs, endMs) => {
    const observed = (manifest.monitorSnapshots ?? []).filter(snapshot =>
      snapshot.elapsedMs >= startMs && snapshot.elapsedMs < endMs
    );
    const counts = {};
    let preferredHighSamples = 0;
    for (const snapshot of observed) {
      const layers = snapshot.currentLayers?.[name];
      const spatialLayer = layers?.currentLayers?.spatialLayer;
      if (Number.isInteger(spatialLayer)) counts[spatialLayer] = (counts[spatialLayer] ?? 0) + 1;
      if (layers?.preferredLayers?.spatialLayer === 2) preferredHighSamples += 1;
    }
    return { sampleCount: observed.length, spatialLayerCounts: counts, preferredHighSamples };
  };
  return phases.map(phase => {
    const start = t0 + phase.startMs;
    const end = t0 + phase.endMs;
    const scheduleIndex = (manifest.schedule ?? []).findIndex(item =>
      item.atElapsedMs === phase.startMs && item.layer === phase.layer
    );
    const transition = scheduleIndex > 0 ? consumer2Transitions[scheduleIndex] : null;
    const layerReached = transition?.actualLayerReachedAt
      ? Date.parse(transition.actualLayerReachedAt)
      : phase.startMs === 0 ? start : null;
    const stableStart = Number.isFinite(layerReached)
      ? layerReached + (phase.startMs === 0 ? 0 : 30_000)
      : end;
    const items = Object.entries(participants).map(([name, participant]) => {
      const role = participant.metadata.role;
      const samples = roleSamples(participant);
      const inWindow = samples.filter(sample => {
        const time = Date.parse(sample.timestamp);
        return Number.isFinite(time) && time >= start && time < end;
      });
      const transitionSamples = inWindow.filter(sample => {
        const time = Date.parse(sample.timestamp);
        return phase.startMs > 0 && time < stableStart;
      });
      const stableSamples = inWindow.filter(sample => Date.parse(sample.timestamp) >= stableStart);
      const metricFields = role === 'producer'
        ? ['sourceFps']
        : ['receivedFps', 'decodedFps', 'receivedMediaBitrateKbps', 'maxJitterMs'];
      const summarizeFields = selectedSamples => Object.fromEntries(metricFields.map(field => [
        field,
        summarize(selectedSamples.map(sample => sample[field]))
      ]));
      return [name, {
        sampleCount: inWindow.length,
        transitionSampleCount: transitionSamples.length,
        stableSampleCount: stableSamples.length,
        metrics: summarizeFields(inWindow),
        transitionMetrics: summarizeFields(transitionSamples),
        stableMetrics: summarizeFields(stableSamples),
        consumer2RequestedLayer: name === 'consumer2' ? phase.layer : undefined
      }];
    });
    return {
      ...phase,
      observedLayerReachedAt: transition?.actualLayerReachedAt ?? (phase.startMs === 0 ? manifest.t0?.wallTime : null),
      stableWindowStartsAt: Number.isFinite(stableStart) ? new Date(stableStart).toISOString() : null,
      stableWindowPolicy: phase.startMs === 0 ? 'from T0' : '30 seconds after Consumer 2 actual layer confirmation',
      currentLayerObservations: {
        consumer1: layerObservationSummary('consumer1', phase.startMs, phase.endMs),
        consumer2: layerObservationSummary('consumer2', phase.startMs, phase.endMs)
      },
      participants: Object.fromEntries(items)
    };
  });
}

function resolvedAnomalies(manifest) {
  const snapshots = manifest.monitorSnapshots ?? [];
  return (manifest.anomalies ?? []).map(anomaly => {
    if (anomaly.recoveredAt) return anomaly;
    const later = snapshots.filter(snapshot => snapshot.elapsedMs > (anomaly.elapsedMs ?? -1));
    for (const snapshot of later) {
      const participant = anomaly.participant;
      const state = snapshot[participant];
      let recovered = false;
      let recoveryEvidence = null;
      if (anomaly.kind === 'consumer-fps-unavailable') {
        recovered = state?.receivedFps > 0 && state?.decodedFps > 0;
        recoveryEvidence = recovered ? 'received and decoded FPS both became positive' : null;
      } else if (anomaly.kind === 'consumer-media-stalled') {
        recovered = state?.rtpReceived > anomaly.details?.currentRtpReceived &&
          state?.framesDecoded > anomaly.details?.currentFramesDecoded;
        recoveryEvidence = recovered ? 'RTP and decoded-frame counters advanced' : null;
      } else if (anomaly.kind === 'layer-transition-not-confirmed') {
        recovered = snapshot.currentLayers?.[participant]?.currentLayers?.spatialLayer ===
          anomaly.details?.requestedSpatialLayer;
        recoveryEvidence = recovered ? 'later SFU current spatial-layer observation matched the request' : null;
      }
      if (recovered) {
        return { ...anomaly, recoveredAt: snapshot.wallTime, recoveryEvidence };
      }
    }
    return anomaly;
  });
}

function memoryCheckpoints(manifest) {
  const snapshots = manifest.monitorSnapshots ?? [];
  const duration = manifest.durationMinutes ?? 0;
  const isWindows = /^(win32|windows)$/i.test(String(manifest.environment?.os?.platform ?? ''));
  const targets = duration >= 20
    ? [0, 10, 20].map(targetMinute => ({ targetMinute, targetMs: targetMinute * 60_000 }))
    : [0, 1, 2, duration].map(targetMinute => ({ targetMinute, targetMs: targetMinute * 60_000 }));
  if (duration >= 20 && manifest.measurementDeadlineReached !== true && snapshots.length > 0) {
    targets.push({ targetMinute: 'last monitor', targetMs: snapshots.at(-1).elapsedMs });
  } else if (duration >= 20) {
    targets.push({ targetMinute: duration, targetMs: duration * 60_000 });
  }
  return targets.map(({ targetMinute, targetMs }) => {
    const snapshot = snapshots.reduce((best, candidate) =>
      !best || Math.abs(candidate.elapsedMs - targetMs) < Math.abs(best.elapsedMs - targetMs)
        ? candidate
        : best,
    null);
    if (targetMinute !== 0 && typeof targetMinute === 'number' &&
      Math.abs((snapshot?.elapsedMs ?? Infinity) - targetMs) > 60_000) return null;
    const legacyWindowsTreeRssScale = isWindows &&
      (manifest.memoryMetricVersion ?? 1) < 3;
    const normalizeTreeRss = value => typeof value === 'number' && Number.isFinite(value)
      ? (legacyWindowsTreeRssScale ? value / 1024 : value)
      : null;
    return {
      targetMinute,
      observedElapsedMs: snapshot?.elapsedMs ?? null,
      wallTime: snapshot?.wallTime ?? null,
      systemUsedRamBytes: snapshot?.system?.usedRamBytes ?? null,
      chromeProcessTreeRssKiB: snapshot?.system?.chromeProcess?.processTreeRssKiB ?? null,
      chromeProcessTreeRssBytesRecorded: snapshot?.system?.chromeProcess?.processTreeRssBytes ?? null,
      chromeProcessTreeRssBytes: normalizeTreeRss(snapshot?.system?.chromeProcess?.processTreeRssBytes),
      backendProcessTreeRssKiB: snapshot?.system?.backendProcessTree?.processTreeRssKiB ?? null,
      backendProcessTreeRssBytesRecorded: snapshot?.system?.backendProcessTree?.processTreeRssBytes ?? null,
      backendProcessTreeRssBytes: normalizeTreeRss(snapshot?.system?.backendProcessTree?.processTreeRssBytes),
      legacyTreeRssCorrectionApplied: legacyWindowsTreeRssScale,
      chromeProcessRootRssBytes: snapshot?.system?.chromeProcess?.rootRssBytes ?? null,
      backendProcessRootRssBytes: snapshot?.system?.backendProcessTree?.rootRssBytes ?? null,
      producerBrowserJsHeapBytes: snapshot?.browserJsHeapUsedBytes?.producer ?? null,
      consumer1BrowserJsHeapBytes: snapshot?.browserJsHeapUsedBytes?.consumer1 ?? null,
      consumer2BrowserJsHeapBytes: snapshot?.browserJsHeapUsedBytes?.consumer2 ?? null
    };
  }).filter(Boolean);
}

function ms(value) {
  return typeof value === 'number' && Number.isFinite(value) ? `${(value / 1000).toFixed(1)} s` : 'UNKNOWN';
}

function renderVisualizerResults(visualizer) {
  const chartParticipants = visualizer?.chartJs?.participants ?? {};
  const svgParticipants = visualizer?.svgFallback?.participants ??
    visualizer?.svgFallbackParticipants ?? {};
  const participantNames = [...new Set([
    ...Object.keys(chartParticipants),
    ...Object.keys(svgParticipants)
  ])];

  if (participantNames.length === 0) {
    return ['- No per-participant renderer results recorded.'];
  }

  const status = value => typeof value === 'string' ? value : 'UNKNOWN';
  const count = value => Number.isFinite(value) ? String(value) : 'UNKNOWN';
  const duration = value => Number.isFinite(value) ? `${value} ms` : 'UNKNOWN';
  return participantNames.map(name => {
    const chart = chartParticipants[name];
    const svg = svgParticipants[name];
    const chartResult = chart
      ? `${status(chart.status)} (${count(chart.canvasCount)} canvases, ${duration(chart.renderMs)})`
      : 'UNKNOWN';
    const svgResult = svg
      ? `${status(svg.status)} (${count(svg.svgCount)} charts, ${duration(svg.renderMs)})`
      : 'UNKNOWN';
    return `- ${name}: Chart.js ${chartResult}; SVG fallback ${svgResult}`;
  });
}

export function reportMarkdown(analysis) {
  const lines = [
    '# 30-minute WebRTC SFU statistics soak report',
    '',
    `- Run: \`${analysis.runId}\` (${analysis.durationMinutes} minute target)`,
    `- Status: **${analysis.status}**`,
    `- Commit: \`${analysis.gitCommit ?? 'UNKNOWN'}\``,
    `- T0: ${analysis.t0 ?? 'UNKNOWN'}`,
    `- End: ${analysis.finishedAt ?? 'UNKNOWN'}`,
    `- Common measured overlap: ${ms(analysis.commonOverlapMs)}`,
    `- Common overlap meets target duration: ${analysis.commonOverlapMeetsTarget === true}`,
    `- Same host: ${analysis.sameHost ? 'yes' : 'UNKNOWN'}`,
    '',
    '## Participant dump summary',
    '',
    '| Participant | Role | Samples | Bytes | Span | Sample gap median / max | >2 s / >3 s gaps | Collection status |',
    '| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |'
  ];
  for (const participant of Object.values(analysis.participants ?? {})) {
    const v = participant.validation;
    const gaps = v.timestamps.intervalMs;
    lines.push(
      `| ${participant.name} | ${participant.role} | ${v.sampleCount} | ${v.fileBytes} | ${ms(v.timestamps.measuredSpanMs)} | ${gaps.median?.toFixed(1) ?? 'UNKNOWN'} / ${gaps.max?.toFixed(1) ?? 'UNKNOWN'} ms | ${v.timestamps.intervalsOver2s} / ${v.timestamps.intervalsOver3s} | ${v.collectionStatus ?? 'UNKNOWN'} |`
    );
  }
  lines.push('', '## Metric summaries', '');
  for (const [name, participant] of Object.entries(analysis.participants ?? {})) {
    lines.push(`### ${name} (${participant.role})`, '');
    lines.push('| Metric | Mean | Median | Min | p05 | p95 | Max | Null % |');
    lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
    for (const [field, summary] of Object.entries(participant.fields ?? {})) {
      const fmt = value => typeof value === 'number' ? value.toFixed(2) : 'UNKNOWN';
      lines.push(`| ${field} | ${fmt(summary.mean)} | ${fmt(summary.median)} | ${fmt(summary.min)} | ${fmt(summary.p05)} | ${fmt(summary.p95)} | ${fmt(summary.max)} | ${fmt(summary.unavailablePercent)} |`);
    }
    lines.push('', `Raw counter summary: \`${JSON.stringify(participant.counters)}\`.`, '');
  }
  lines.push('## Scheduled phase comparison', '');
  for (const phase of analysis.phaseComparisons ?? []) {
    lines.push(`### ${phase.label}`, '');
    lines.push('| Participant | Transition / stable samples | Stable received FPS mean | Stable decoded FPS mean | Stable bitrate mean Kbps | Stable jitter median ms |');
    lines.push('| --- | ---: | ---: | ---: | ---: | ---: |');
    for (const [name, item] of Object.entries(phase.participants ?? {})) {
      const m = item.stableMetrics;
      const fmt = value => typeof value === 'number' ? value.toFixed(2) : 'UNKNOWN';
      lines.push(`| ${name} | ${item.transitionSampleCount} / ${item.stableSampleCount} | ${fmt(m.receivedFps?.mean)} | ${fmt(m.decodedFps?.mean)} | ${fmt(m.receivedMediaBitrateKbps?.mean)} | ${fmt(m.maxJitterMs?.median)} |`);
    }
    for (const name of ['consumer1', 'consumer2']) {
      const observation = phase.currentLayerObservations?.[name];
      lines.push(`${name} observed SFU current spatial layers: ${JSON.stringify(observation?.spatialLayerCounts ?? {})}; preferred HIGH: ${observation?.preferredHighSamples ?? 0}/${observation?.sampleCount ?? 0} minute observations.`);
    }
    lines.push('');
  }
  lines.push('## Consumer 2 layer changes', '');
  lines.push('| Reason | Requested at | Layer | Signaling accepted | Observed spatial layer | Confirmed at |', '| --- | --- | --- | --- | ---: | --- |');
  for (const change of analysis.layerChanges ?? []) {
    if (change.participant !== 'consumer2') continue;
    lines.push(`| ${change.reason} | ${change.requestedAt} | ${change.requestedLayer.toUpperCase()} | ${change.signalingOk} | ${change.actualSpatialLayer ?? 'UNKNOWN'} | ${change.actualLayerReachedAt ?? 'UNKNOWN'} |`);
  }
  lines.push('', '## Memory observations', '');
  lines.push('| Target minute | Observed | System used RAM bytes | Chrome tree RSS bytes | Chrome root RSS bytes | Backend tree RSS bytes | Backend root RSS bytes | Producer JS heap | Consumer 1 JS heap | Consumer 2 JS heap |', '| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const item of analysis.memoryCheckpoints ?? []) {
    lines.push(`| ${item.targetMinute} | ${item.wallTime ?? 'UNKNOWN'} | ${item.systemUsedRamBytes ?? 'UNKNOWN'} | ${item.chromeProcessTreeRssBytes ?? 'UNKNOWN'} | ${item.chromeProcessRootRssBytes ?? 'UNKNOWN'} | ${item.backendProcessTreeRssBytes ?? 'UNKNOWN'} | ${item.backendProcessRootRssBytes ?? 'UNKNOWN'} | ${item.producerBrowserJsHeapBytes ?? 'UNKNOWN'} | ${item.consumer1BrowserJsHeapBytes ?? 'UNKNOWN'} | ${item.consumer2BrowserJsHeapBytes ?? 'UNKNOWN'} |`);
  }
  for (const item of analysis.memoryCheckpoints ?? []) {
    lines.push(`Tree RSS recorded → normalized bytes at minute ${item.targetMinute}: Chrome ${item.chromeProcessTreeRssBytesRecorded ?? 'UNKNOWN'} → ${item.chromeProcessTreeRssBytes ?? 'UNKNOWN'}; backend ${item.backendProcessTreeRssBytesRecorded ?? 'UNKNOWN'} → ${item.backendProcessTreeRssBytes ?? 'UNKNOWN'}.`);
  }
  if (analysis.memoryMetricNote) lines.push('', `RSS method: ${analysis.memoryMetricNote}`, '');
  lines.push('', '## Checkpoint downloads', '');
  lines.push('| Stage | Participant | Started | Finished | Bytes | Samples |', '| --- | --- | --- | --- | ---: | ---: |');
  for (const item of analysis.downloads ?? []) {
    lines.push(`| ${item.stage} | ${item.participant} | ${item.startedAt} | ${item.finishedAt} | ${item.fileBytes} | ${item.sampleCount} |`);
  }
  lines.push('', '## Anomaly and recovery history', '');
  lines.push('| Occurred | Elapsed | Kind | Participant | Recovered at | Recovery evidence |', '| --- | ---: | --- | --- | --- | --- |');
  for (const item of analysis.anomalies ?? []) {
    lines.push(`| ${item.wallTime} | ${ms(item.elapsedMs)} | ${item.kind} | ${item.participant ?? 'n/a'} | ${item.recoveredAt ?? 'not observed'} | ${item.recoveryEvidence ?? 'n/a'} |`);
  }
  if ((analysis.anomalies ?? []).length === 0) lines.push('| none | — | — | — | — | — |');
  lines.push(
    '## Event, download, and visualization results',
    '',
    `- Selected participant dump set validated: ${analysis.selectedDumpsValid === true} (${analysis.selectedDumpStage ?? 'UNKNOWN'})`,
    `- Full final downloads saved: ${analysis.finalDownloadsValid === true}`,
    `- All scheduled layer changes reached the requested SFU spatial layer: ${analysis.layerScheduleReached === true}`,
    `- Unexpected disconnects: ${analysis.unexpectedDisconnectCount ?? 'UNKNOWN'}`,
    `- Media stall/FPS anomaly events: ${analysis.mediaStallCount ?? 'UNKNOWN'}`,
    `- 5-second diagnostic probes: ${analysis.diagnosticProbeCount ?? 0} (${analysis.diagnosticProbeFile ?? 'not recorded'})`,
    `- Stall-triggered SFU diagnostic episodes: ${analysis.stallDiagnostics?.length ?? 0}`,
    `- Chart.js validation: ${analysis.visualizer?.chartJs?.status ?? 'UNKNOWN'}`,
    `- SVG fallback validation: ${analysis.visualizer?.svgFallback?.status ?? 'UNKNOWN'}`,
    '- Final chart rendering results:',
    ...renderVisualizerResults(analysis.visualizer),
    '',
    '## Limits',
    '',
    '- Producer and both Consumers ran on one Windows host and one temporary headful Chrome profile; this does not validate WAN or multi-device network behavior.',
    '- JSON checkpoints were downloaded while collection remained active. Their metadata reports `collectionStatus=recording`; `measurementEndTime` may therefore be absent. The final sample timestamp is reported as the observed measurement end.',
    '- `browserJsHeapUsedBytes`, process RSS, and system RAM are separate measures. Missing values are retained as unavailable.',
    '- RTX retransmission volume is UNKNOWN when Chrome did not provide `retransmittedPacketsReceived`.',
    ''
  );
  return lines.join('\n');
}

export async function analyzeRun(runDir) {
  const resolvedRunDir = path.resolve(runDir);
  const manifest = JSON.parse(await readFile(path.join(resolvedRunDir, 'manifest.json'), 'utf8'));
  const names = ['producer', 'consumer1', 'consumer2'];
  const allDownloads = manifest.downloads ?? [];
  const selectedDumpStage = ['final', 'partial-final', 'checkpoint-19m', 'checkpoint-09m']
    .find(stage => names.every(name => allDownloads.some(item =>
      item.stage === stage && item.participant === name && item.validated
    ))) ?? null;
  const selectedByName = new Map(allDownloads
    .filter(item => item.stage === selectedDumpStage && item.validated)
    .map(item => [item.participant, item]));
  const participantEntries = [];
  let maxStart = -Infinity;
  let minEnd = Infinity;
  let allValid = true;
  for (const name of names) {
    const item = selectedByName.get(name);
    if (!item) {
      allValid = false;
      continue;
    }
    try {
      const filePath = path.resolve(resolvedRunDir, item.relativePath);
      if (!filePath.startsWith(resolvedRunDir + path.sep)) throw new Error('final JSON path escapes run directory');
      const dump = JSON.parse(await readFile(filePath, 'utf8'));
      const expectedRole = name === 'producer' ? 'producer' : 'consumer';
      const expectedPeerId = manifest.participants?.[name]?.peerId;
      const participant = await analyzeParticipant(name, { dump, filePath, relativePath: item.relativePath }, {
        role: expectedRole,
        peerId: expectedPeerId
      });
      participant.role = dump.metadata.role;
      participant.metadata = dump.metadata;
      allValid &&= participant.validation.schemaVersion && participant.validation.roleMatches &&
        participant.validation.peerIdMatches && participant.validation.sampleCount > 0 &&
        participant.validation.timestamps.monotonicViolations === 0 &&
        !participant.validation.retainedLimitReached;
      participant.validation.selectedDumpStage = item.stage;
      const first = Date.parse(participant.validation.timestamps.firstTimestamp ?? '');
      const last = Date.parse(participant.validation.timestamps.lastTimestamp ?? '');
      if (Number.isFinite(first)) maxStart = Math.max(maxStart, first);
      if (Number.isFinite(last)) minEnd = Math.min(minEnd, last);
      participantEntries.push([name, participant]);
    } catch (error) {
      allValid = false;
      participantEntries.push([name, { name, error: error instanceof Error ? error.message : String(error) }]);
    }
  }
  const selectedDumpsValid = names.every(name => {
    const item = selectedByName.get(name);
    return item?.validated === true;
  }) && participantEntries.length === 3 && participantEntries.every(([, item]) => !item.error);
  const finalDownloadsValid = selectedDumpStage === 'final' && selectedDumpsValid;
  const commonOverlapMs = Number.isFinite(maxStart) && Number.isFinite(minEnd) && minEnd > maxStart
    ? minEnd - maxStart
    : 0;
  const commonOverlapMeetsTarget = commonOverlapMs >= Number(manifest.targetDurationMs ?? Infinity);
  allValid &&= commonOverlapMeetsTarget;
  const layerEvents = (manifest.events ?? []).filter(event => event.type === 'layer-change');
  const consumer2LayerEvents = layerEvents.filter(event => event.participant === 'consumer2');
  const layerScheduleReached = (manifest.schedule ?? []).every((schedule, index) => {
    const event = consumer2LayerEvents[index];
    return event?.requestedLayer === schedule.layer &&
      event?.actualSpatialLayer === schedule.spatialLayer && event?.signalingOk === true;
  });
  const events = manifest.events ?? [];
  const unexpectedDisconnectCount = events.filter(event => event.type === 'anomaly' &&
    /disconnect|capture-track-ended|server-exit|browser-disconnected/i.test(event.kind ?? '')
  ).length;
  const mediaStallCount = events.filter(event => event.type === 'anomaly' &&
    /media-stalled|fps-unavailable/i.test(event.kind ?? '')
  ).length;
  const analyzer = {
    runId: manifest.runId,
    durationMinutes: manifest.durationMinutes,
    gitCommit: manifest.environment?.gitCommit ?? null,
    t0: manifest.t0?.wallTime ?? null,
    finishedAt: manifest.finishedAt ?? null,
    status: manifest.status,
    sameHost: manifest.environment?.execution?.sameHost ?? null,
    commonOverlapMs,
    commonOverlapMeetsTarget,
    selectedDumpStage,
    selectedDumpsValid,
    finalDownloadsValid,
    layerScheduleReached,
    layerChanges: manifest.layerChanges ?? [],
    downloads: (manifest.downloads ?? []).filter(item =>
      item.stage === 'checkpoint-09m' || item.stage === 'checkpoint-19m' ||
      item.stage === 'partial-final' || item.stage === 'final'
    ),
    memoryCheckpoints: memoryCheckpoints(manifest),
    diagnosticProbeCount: manifest.diagnosticProbeCount ?? 0,
    diagnosticProbeFile: manifest.files?.diagnosticProbes ?? null,
    stallDiagnostics: (manifest.stallDiagnostics ?? []).map(diagnostic => ({
      diagnosticId: diagnostic.diagnosticId,
      participant: diagnostic.participant,
      detectedAt: diagnostic.detectedAt,
      stalledAxes: diagnostic.stalledAxes,
      snapshots: diagnostic.captures?.length ?? 0
    })),
    memoryMetricNote: /^(win32|windows)$/i.test(String(manifest.environment?.os?.platform ?? '')) &&
      (manifest.memoryMetricVersion ?? 1) < 3
      ? 'Legacy Windows process-tree RSS in memoryMetricVersion 1/2 was scaled 1024x. The report retains the recorded byte values and shows an inferred normalized value after dividing by 1024. New version 3 samples also retain raw memRss KiB; root RSS (pidusage bytes) and system RAM are direct measures.'
      : 'Version 3 retains the raw systeminformation process-tree memRss KiB sum and its byte conversion; root RSS (pidusage bytes) and system RAM are separate direct measures.',
    unexpectedDisconnectCount,
    mediaStallCount,
    anomalies: resolvedAnomalies(manifest),
    visualizer: manifest.visualizer ?? null,
    participants: Object.fromEntries(participantEntries.map(([name, participant]) => {
      const { samples, metadata, ...summary } = participant;
      return [name, summary];
    })),
    phaseComparisons: phaseWindowSummaries(
      Object.fromEntries(participantEntries.map(([name, item]) => [name, item.samples ? { ...item, metadata: item.metadata } : null]).filter(([, item]) => item)),
      manifest
    )
  };
  analyzer.analysisStatus = finalDownloadsValid && allValid && layerScheduleReached ? 'complete' : 'partial';
  await writeFile(path.join(resolvedRunDir, 'analysis.json'), JSON.stringify(analyzer, null, 2) + '\n', 'utf8');
  await writeFile(path.join(resolvedRunDir, 'report.md'), reportMarkdown(analyzer), 'utf8');
  return analyzer;
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url))) {
  const runDir = process.argv[2];
  if (!runDir) {
    console.error('Usage: node scripts/analyze-stats-soak.mjs <run-directory>');
    process.exitCode = 2;
  } else {
    try {
      const result = await analyzeRun(runDir);
      console.log(`Soak analysis: ${result.analysisStatus}`);
      console.log(path.join(path.resolve(runDir), 'report.md'));
    } catch (error) {
      console.error(error instanceof Error ? error.stack ?? error.message : String(error));
      process.exitCode = 1;
    }
  }
}
