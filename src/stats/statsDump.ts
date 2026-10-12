export const MAX_STATS_SAMPLES = 4200;
export const MAX_STATS_DURATION_MS = 70 * 60 * 1000;

export type StatsRole = 'producer' | 'consumer';
export type StatsLayer = 'low' | 'mid' | 'high';
export type StatsCollectionStatus = 'recording' | 'stopped' | 'limit-reached';

export type ProducerLayerStatsSample = {
  targetBitrateBps: number | null;
  encodedFps: number | null;
  encodedChunksDelta: number;
  encodedChunksTotal: number;
  encodedPayloadBytesDelta: number;
  encodedPayloadBytesTotal: number;
  encodedPayloadBitrateKbps: number | null;
  keyFramesDelta: number;
  keyFramesTotal: number;
  deltaFramesDelta: number;
  deltaFramesTotal: number;
  encodeQueueSize: number | null;
  width: number | null;
  height: number | null;
};

export type ProducerStatsSample = {
  timestamp: string;
  elapsedMs: number | null;
  sourceFps: number | null;
  sourceFramesDelta: number;
  sourceFramesTotal: number;
  layers: Partial<Record<StatsLayer, ProducerLayerStatsSample>>;
  socketBufferedAmountBytes: number | null;
  browserJsHeapUsedBytes: number | null;
};

export type ConsumerStatsSample = {
  timestamp: string;
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
  browserJsHeapUsedBytes: number | null;
};

export type StatsDumpMetadata = {
  peerId: string;
  role: StatsRole;
  measurementStartTime: string;
  dumpTime: string;
  sampleIntervalMs: number;
  userAgent: string;
  simulcastEnabled: boolean;
  availableLayers: StatsLayer[];
  collectionStatus?: StatsCollectionStatus;
  measurementEndTime?: string;
};

export type StatsDump = {
  schemaVersion: 1;
  metadata: StatsDumpMetadata;
  producerStats: ProducerStatsSample[];
  consumerStats: ConsumerStatsSample[];
};

export type StatsDumpClock = {
  monotonicNow: () => number;
  wallTimeIso: () => string;
};

type StatsDumpSessionMetadata = Omit<
  StatsDumpMetadata,
  'dumpTime' | 'collectionStatus' | 'measurementEndTime'
>;

const systemClock: StatsDumpClock = {
  monotonicNow: () => globalThis.performance.now(),
  wallTimeIso: () => new Date().toISOString()
};

export class StatsDumpSession {
  readonly metadata: StatsDumpSessionMetadata;
  private readonly producerSamples: ProducerStatsSample[] = [];
  private readonly consumerSamples: ConsumerStatsSample[] = [];
  private readonly startedAtMonotonicMs: number;
  private status: StatsCollectionStatus = 'recording';
  private endedAt: string | undefined;

  constructor(metadata: StatsDumpSessionMetadata, private readonly clock: StatsDumpClock = systemClock) {
    this.metadata = metadata;
    this.startedAtMonotonicMs = clock.monotonicNow();
  }

  get collectionStatus(): StatsCollectionStatus {
    return this.status;
  }

  get sampleCount(): number {
    return this.metadata.role === 'producer'
      ? this.producerSamples.length
      : this.consumerSamples.length;
  }

  get producerStats(): readonly ProducerStatsSample[] {
    return this.producerSamples;
  }

  get consumerStats(): readonly ConsumerStatsSample[] {
    return this.consumerSamples;
  }

  appendProducerSample(sample: ProducerStatsSample): boolean {
    return this.appendSample('producer', sample);
  }

  appendConsumerSample(sample: ConsumerStatsSample): boolean {
    return this.appendSample('consumer', sample);
  }

  stop(): void {
    this.checkDurationLimit();
    if (this.status !== 'recording') {
      return;
    }
    this.status = 'stopped';
    this.endedAt = this.clock.wallTimeIso();
  }

  createDump(): StatsDump {
    this.checkDurationLimit();
    return {
      schemaVersion: 1,
      metadata: {
        ...this.metadata,
        dumpTime: this.clock.wallTimeIso(),
        collectionStatus: this.status,
        ...(this.endedAt ? { measurementEndTime: this.endedAt } : {})
      },
      producerStats: this.producerSamples.slice(),
      consumerStats: this.consumerSamples.slice()
    };
  }

  private appendSample(
    role: StatsRole,
    sample: ProducerStatsSample | ConsumerStatsSample
  ): boolean {
    if (this.status !== 'recording' || this.metadata.role !== role) {
      return false;
    }

    if (this.checkDurationLimit()) {
      return false;
    }

    const samples = role === 'producer' ? this.producerSamples : this.consumerSamples;
    if (samples.length >= MAX_STATS_SAMPLES) {
      this.reachLimit();
      return false;
    }

    if (role === 'producer') {
      this.producerSamples.push(sample as ProducerStatsSample);
    } else {
      this.consumerSamples.push(sample as ConsumerStatsSample);
    }
    if (samples.length >= MAX_STATS_SAMPLES) {
      this.reachLimit();
    }
    return true;
  }

  private reachLimit(): void {
    if (this.status !== 'recording') {
      return;
    }
    this.status = 'limit-reached';
    this.endedAt = this.clock.wallTimeIso();
  }

  private checkDurationLimit(): boolean {
    if (
      this.status === 'recording' &&
      this.clock.monotonicNow() - this.startedAtMonotonicMs >= MAX_STATS_DURATION_MS
    ) {
      this.reachLimit();
    }
    return this.status === 'limit-reached';
  }
}

export function readBrowserJsHeapUsedBytes(): number | null {
  const browserPerformance = performance as Performance & {
    memory?: { usedJSHeapSize?: unknown };
  };
  const value = browserPerformance.memory?.usedJSHeapSize;
  return typeof value === 'number' && Number.isFinite(value) && value >= 0
    ? value
    : null;
}

export function downloadStatsDump(session: StatsDumpSession): string {
  const dump = session.createDump();
  const json = JSON.stringify(dump, null, 2);
  const blob = new Blob([json], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const timestamp = dump.metadata.dumpTime.replace(/[:.]/g, '-').slice(0, 19);
  const peerId = dump.metadata.peerId.replace(/[^a-z0-9_-]/gi, '-') || 'unknown';
  const filename = `webrtc-dump-${dump.metadata.role}-${peerId}-${timestamp}.json`;
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.hidden = true;
  document.body.append(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  return filename;
}
