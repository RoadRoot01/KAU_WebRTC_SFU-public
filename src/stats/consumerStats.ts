export type ConsumerInboundStats = {
  id: string | null;
  timestamp: number | null;
  packetsReceived: number | null;
  packetsLost: number | null;
  maxJitterSeconds: number | null;
  framesReceived: number | null;
  framesDecoded: number | null;
  framesDropped: number | null;
  bytesReceived: number | null;
};

export type ConsumerStatsSnapshot = ReadonlyMap<string, ConsumerInboundStats>;

/** Ignore asynchronous poll completions after stop or session replacement. */
export function canApplyConsumerStatsResult(
  stopped: boolean,
  isCurrentSession: boolean
): boolean {
  return !stopped && isCurrentSession;
}

export type ConsumerStatsCalculation = {
  elapsedMs: number | null;
  packetsReceived: number | null;
  packetsLost: number | null;
  maxJitterMs: number | null;
  framesReceived: number | null;
  framesDecoded: number | null;
  framesDropped: number | null;
  bytesReceived: number | null;
  receivedFps: number | null;
  decodedFps: number | null;
  receivedMediaBitrateKbps: number | null;
  nextSnapshot: Map<string, ConsumerInboundStats>;
  inboundVideoStatsCount: number;
};

type CounterField =
  | 'packetsReceived'
  | 'packetsLost'
  | 'framesReceived'
  | 'framesDecoded'
  | 'framesDropped'
  | 'bytesReceived';

/**
 * Select Consumer video media RTP records and normalize absent/invalid fields to null.
 * mediasoup-client creates an additional video RTP probation receiver whose MID and
 * track identifier are "probator". It is part of the shared transport stats, not the
 * Consumer's media stream, so it must not contribute to Consumer media totals/rates.
 */
export function readVideoInboundStats(reportValues: Iterable<unknown>): ConsumerInboundStats[] {
  const inbound: ConsumerInboundStats[] = [];
  for (const candidate of reportValues) {
    if (!isRecord(candidate) || candidate.type !== 'inbound-rtp' || candidate.kind !== 'video') {
      continue;
    }
    if (candidate.mid === 'probator' || candidate.trackIdentifier === 'probator') {
      continue;
    }

    inbound.push({
      id: typeof candidate.id === 'string' && candidate.id.length > 0 ? candidate.id : null,
      timestamp: toFiniteTimestamp(candidate.timestamp),
      packetsReceived: toFiniteNonNegative(candidate.packetsReceived),
      packetsLost: toFiniteSigned(candidate.packetsLost),
      maxJitterSeconds: toFiniteNonNegative(candidate.jitter),
      framesReceived: toFiniteNonNegative(candidate.framesReceived),
      framesDecoded: toFiniteNonNegative(candidate.framesDecoded),
      framesDropped: toFiniteNonNegative(candidate.framesDropped),
      bytesReceived: toFiniteNonNegative(candidate.bytesReceived)
    });
  }
  return inbound;
}

/**
 * Calculate complete totals and per-SSRC rates from one RTCStatsReport.
 * A changed stream set invalidates rates for that interval; raw totals remain
 * available when every current stream reports that particular metric.
 */
export function calculateConsumerStats(
  currentStats: readonly ConsumerInboundStats[],
  previousSnapshot: ConsumerStatsSnapshot
): ConsumerStatsCalculation {
  const uniqueIds = currentStats.every(stat => stat.id !== null) &&
    new Set(currentStats.map(stat => stat.id)).size === currentStats.length;

  if (currentStats.length === 0 || !uniqueIds) {
    return createEmptyCalculation(currentStats.length);
  }

  const currentById = new Map(currentStats.map(stat => [stat.id!, stat]));
  const elapsedValues: number[] = [];
  if (previousSnapshot.size === currentStats.length) {
    for (const stat of currentStats) {
      const previous = previousSnapshot.get(stat.id!);
      if (
        previous &&
        stat.timestamp !== null &&
        previous.timestamp !== null &&
        stat.timestamp > previous.timestamp
      ) {
        elapsedValues.push(stat.timestamp - previous.timestamp);
      }
    }
  }

  const nextSnapshot = new Map<string, ConsumerInboundStats>();
  for (const stat of currentStats) {
    const previous = previousSnapshot.get(stat.id!);
    if (stat.timestamp === null) {
      if (previous) {
        // Keep the last trustworthy baseline until a valid RTCStats timestamp returns.
        nextSnapshot.set(stat.id!, previous);
      }
    } else if (previous?.timestamp !== null && previous && stat.timestamp <= previous.timestamp) {
      // A duplicate or decreasing timestamp cannot establish a new rate baseline.
      nextSnapshot.set(stat.id!, previous);
    } else {
      nextSnapshot.set(stat.id!, stat);
    }
  }

  const elapsedMs = elapsedValues.length === currentStats.length
    ? median(elapsedValues)
    : null;

  return {
    elapsedMs,
    packetsReceived: sumComplete(currentStats, 'packetsReceived'),
    packetsLost: sumComplete(currentStats, 'packetsLost'),
    maxJitterMs: maximumCompleteJitterMs(currentStats),
    framesReceived: sumComplete(currentStats, 'framesReceived'),
    framesDecoded: sumComplete(currentStats, 'framesDecoded'),
    framesDropped: sumComplete(currentStats, 'framesDropped'),
    bytesReceived: sumComplete(currentStats, 'bytesReceived'),
    receivedFps: sumRate(currentStats, previousSnapshot, currentById, 'framesReceived'),
    decodedFps: sumRate(currentStats, previousSnapshot, currentById, 'framesDecoded'),
    receivedMediaBitrateKbps: sumRate(currentStats, previousSnapshot, currentById, 'bytesReceived', 8),
    nextSnapshot,
    inboundVideoStatsCount: currentStats.length
  };
}

function sumComplete(stats: readonly ConsumerInboundStats[], field: CounterField): number | null {
  if (stats.length === 0) {
    return null;
  }

  let total = 0;
  for (const stat of stats) {
    const value = stat[field];
    if (value === null) {
      return null;
    }
    total += value;
  }
  return Number.isFinite(total) ? total : null;
}

function maximumCompleteJitterMs(stats: readonly ConsumerInboundStats[]): number | null {
  if (stats.length === 0) {
    return null;
  }

  let maximum = 0;
  for (const stat of stats) {
    if (stat.maxJitterSeconds === null) {
      return null;
    }
    maximum = Math.max(maximum, stat.maxJitterSeconds);
  }
  const jitterMs = maximum * 1000;
  return Number.isFinite(jitterMs) ? jitterMs : null;
}

function sumRate(
  currentStats: readonly ConsumerInboundStats[],
  previousSnapshot: ConsumerStatsSnapshot,
  currentById: ReadonlyMap<string, ConsumerInboundStats>,
  field: 'framesReceived' | 'framesDecoded' | 'bytesReceived',
  numeratorMultiplier = 1000
): number | null {
  if (currentStats.length === 0 || previousSnapshot.size !== currentStats.length) {
    return null;
  }

  let totalRate = 0;
  for (const stat of currentStats) {
    const id = stat.id;
    if (id === null) {
      return null;
    }
    const current = currentById.get(id);
    const previous = previousSnapshot.get(id);
    const currentValue = current?.[field];
    const previousValue = previous?.[field];
    if (
      !current ||
      !previous ||
      currentValue === null || currentValue === undefined ||
      previousValue === null || previousValue === undefined ||
      current.timestamp === null || previous.timestamp === null
    ) {
      return null;
    }

    const elapsedMs = current.timestamp - previous.timestamp;
    const delta = currentValue - previousValue;
    if (elapsedMs <= 0 || delta < 0) {
      return null;
    }
    // Frames use 1000/ms to produce per-second rates. Bytes use 8/ms for decimal Kbps.
    totalRate += delta * numeratorMultiplier / elapsedMs;
  }

  return Number.isFinite(totalRate) ? totalRate : null;
}

function median(values: number[]): number | null {
  if (values.length === 0 || values.some(value => !Number.isFinite(value))) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle]!;
  }
  return (sorted[middle - 1]! + sorted[middle]!) / 2;
}

function createEmptyCalculation(inboundVideoStatsCount: number): ConsumerStatsCalculation {
  return {
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
    receivedMediaBitrateKbps: null,
    nextSnapshot: new Map(),
    inboundVideoStatsCount
  };
}

function toFiniteTimestamp(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0
    ? value
    : null;
}

function toFiniteNonNegative(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0
    ? value
    : null;
}

function toFiniteSigned(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
