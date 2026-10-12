import assert from 'node:assert/strict';
import test from 'node:test';
import {
  canApplyConsumerStatsResult,
  calculateConsumerStats,
  readVideoInboundStats
} from '../src/stats/consumerStats.ts';

function rawStat(overrides = {}) {
  return {
    id: 'inbound-a',
    type: 'inbound-rtp',
    kind: 'video',
    timestamp: 1000,
    packetsReceived: 1000,
    packetsLost: 2,
    jitter: 0.01,
    framesReceived: 100,
    framesDecoded: 98,
    framesDropped: 1,
    bytesReceived: 100_000,
    ...overrides
  };
}

function read(...stats) {
  return readVideoInboundStats(stats);
}

function calculate(stats, previous = new Map()) {
  return calculateConsumerStats(stats, previous);
}

function baseline(...stats) {
  return calculate(read(...stats)).nextSnapshot;
}

test('normal one-second sample matches the published FPS and decimal Kbps example', () => {
  const previous = baseline(rawStat());
  const result = calculate(read(rawStat({
    timestamp: 2000,
    packetsReceived: 1100,
    framesReceived: 115,
    framesDecoded: 112,
    bytesReceived: 150_000
  })), previous);

  assert.equal(result.elapsedMs, 1000);
  assert.equal(result.receivedFps, 15);
  assert.equal(result.decodedFps, 14);
  assert.equal(result.receivedMediaBitrateKbps, 400);
});

test('a 1.2 second interval uses RTCStats timestamp delta', () => {
  const previous = baseline(rawStat());
  const result = calculate(read(rawStat({
    timestamp: 2200,
    framesReceived: 118,
    framesDecoded: 116,
    bytesReceived: 160_000
  })), previous);

  assert.equal(result.elapsedMs, 1200);
  assert.equal(result.receivedFps, 15);
  assert.equal(result.decodedFps, 15);
  assert.equal(result.receivedMediaBitrateKbps, 400);
});

test('signed packetsLost preserves negative values and zero distinctly', () => {
  assert.equal(calculate(read(rawStat({ packetsLost: -2 }))).packetsLost, -2);
  assert.equal(calculate(read(rawStat({ packetsLost: 0 }))).packetsLost, 0);
});

test('missing and non-finite packetsLost values become null, not zero', () => {
  for (const value of [undefined, Number.NaN, Number.POSITIVE_INFINITY]) {
    const overrides = value === undefined ? {} : { packetsLost: value };
    const result = calculate(read(rawStat({ packetsLost: undefined, ...overrides })));
    assert.equal(result.packetsLost, null);
    assert.equal(result.packetsReceived, 1000);
  }
});

test('an absent metric invalidates only that metric and its rate', () => {
  const previous = baseline(rawStat());
  const current = read(rawStat({
    timestamp: 2000,
    framesReceived: undefined,
    framesDecoded: 113,
    bytesReceived: 125_000,
    jitter: undefined
  }));
  const result = calculate(current, previous);

  assert.equal(result.framesReceived, null);
  assert.equal(result.receivedFps, null);
  assert.equal(result.framesDecoded, 113);
  assert.equal(result.decodedFps, 15);
  assert.equal(result.bytesReceived, 125_000);
  assert.equal(result.receivedMediaBitrateKbps, 200);
  assert.equal(result.maxJitterMs, null);
});

test('unsigned counters reject negative/non-finite values while signed loss remains valid', () => {
  const result = calculate(read(rawStat({
    packetsReceived: -1,
    framesReceived: Number.NaN,
    framesDecoded: Number.POSITIVE_INFINITY,
    bytesReceived: -100,
    packetsLost: -2
  })));

  assert.equal(result.packetsReceived, null);
  assert.equal(result.framesReceived, null);
  assert.equal(result.framesDecoded, null);
  assert.equal(result.bytesReceived, null);
  assert.equal(result.packetsLost, -2);
});

test('multiple inbound video objects aggregate complete totals and per-SSRC rates', () => {
  const previous = baseline(
    rawStat({ id: 'a', timestamp: 1000, framesReceived: 100, framesDecoded: 90, bytesReceived: 100_000 }),
    rawStat({ id: 'b', timestamp: 1000, framesReceived: 200, framesDecoded: 180, bytesReceived: 200_000 })
  );
  const result = calculate(read(
    rawStat({ id: 'a', timestamp: 2000, framesReceived: 110, framesDecoded: 100, bytesReceived: 150_000 }),
    rawStat({ id: 'b', timestamp: 2200, framesReceived: 224, framesDecoded: 204, bytesReceived: 230_000 })
  ), previous);

  assert.equal(result.framesReceived, 334);
  assert.equal(result.framesDecoded, 304);
  assert.equal(result.bytesReceived, 380_000);
  assert.equal(result.receivedFps, 30);
  assert.equal(result.decodedFps, 30);
  assert.equal(result.receivedMediaBitrateKbps, 600);
  assert.equal(result.elapsedMs, 1100);
});

test('mediasoup RTP probator is excluded from Consumer media totals and rates', () => {
  const probator = rawStat({
    id: 'IT01V1234',
    ssrc: 1234,
    mid: 'probator',
    trackIdentifier: 'probator',
    packetsReceived: 80,
    packetsLost: 32,
    jitter: 0.126,
    framesReceived: 0,
    framesDecoded: 0,
    bytesReceived: 20_000
  });
  const media = rawStat({
    id: 'consumer-media',
    mid: '0',
    trackIdentifier: 'remote-track',
    timestamp: 1000,
    packetsReceived: 100,
    packetsLost: 1,
    jitter: 0.002,
    framesReceived: 30,
    framesDecoded: 29,
    framesDropped: 1,
    bytesReceived: 100_000
  });
  const previous = baseline(probator, media);
  const current = read(
    { ...probator, timestamp: 2000, packetsReceived: 88, packetsLost: 40, bytesReceived: 22_000 },
    {
      ...media,
      timestamp: 2000,
      packetsReceived: 160,
      packetsLost: 2,
      jitter: 0.003,
      framesReceived: 60,
      framesDecoded: 58,
      bytesReceived: 150_000
    }
  );
  const result = calculate(current, previous);

  assert.deepEqual(current.map(stat => stat.id), ['consumer-media']);
  assert.equal(result.inboundVideoStatsCount, 1);
  assert.equal(result.packetsReceived, 160);
  assert.equal(result.packetsLost, 2);
  assert.equal(result.maxJitterMs, 3);
  assert.equal(result.framesReceived, 60);
  assert.equal(result.framesDecoded, 58);
  assert.equal(result.framesDropped, 1);
  assert.equal(result.bytesReceived, 150_000);
  assert.equal(result.receivedFps, 30);
  assert.equal(result.decodedFps, 29);
  assert.equal(result.receivedMediaBitrateKbps, 400);
});

test('RTP probator is excluded when either explicit MID or track marker is present', () => {
  const media = rawStat({ id: 'consumer-media', mid: '0', trackIdentifier: 'remote-track' });
  assert.deepEqual(
    read(rawStat({ id: 'by-mid', mid: 'probator' }), media).map(stat => stat.id),
    ['consumer-media']
  );
  assert.deepEqual(
    read(rawStat({ id: 'by-track', trackIdentifier: 'probator' }), media).map(stat => stat.id),
    ['consumer-media']
  );
});

test('partial fields across inbound objects do not produce partial totals', () => {
  const result = calculate(read(
    rawStat({ id: 'a', packetsReceived: 100, bytesReceived: 1000 }),
    rawStat({ id: 'b', packetsReceived: 200, bytesReceived: undefined })
  ));

  assert.equal(result.packetsReceived, 300);
  assert.equal(result.bytesReceived, null);
  assert.equal(result.framesReceived, 200);
});

test('a decreased counter invalidates only its rate and the next interval recovers', () => {
  const previous = baseline(rawStat());
  const decreased = calculate(read(rawStat({
    timestamp: 2000,
    framesReceived: 90,
    framesDecoded: 113,
    bytesReceived: 150_000
  })), previous);
  assert.equal(decreased.receivedFps, null);
  assert.equal(decreased.decodedFps, 15);
  assert.equal(decreased.receivedMediaBitrateKbps, 400);

  const recovered = calculate(read(rawStat({
    timestamp: 3000,
    framesReceived: 95,
    framesDecoded: 128,
    bytesReceived: 200_000
  })), decreased.nextSnapshot);
  assert.equal(recovered.receivedFps, 5);
  assert.equal(recovered.decodedFps, 15);
  assert.equal(recovered.receivedMediaBitrateKbps, 400);
});

test('a newly appearing stat ID invalidates the changed interval then establishes a baseline', () => {
  const previous = baseline(rawStat({ id: 'old' }));
  const changed = calculate(read(
    rawStat({ id: 'old', timestamp: 2000, framesReceived: 110 }),
    rawStat({ id: 'new', timestamp: 2000, framesReceived: 50 })
  ), previous);
  assert.equal(changed.framesReceived, 160);
  assert.equal(changed.receivedFps, null);
  assert.equal(changed.nextSnapshot.size, 2);

  const recovered = calculate(read(
    rawStat({ id: 'old', timestamp: 3000, framesReceived: 120 }),
    rawStat({ id: 'new', timestamp: 3000, framesReceived: 60 })
  ), changed.nextSnapshot);
  assert.equal(recovered.receivedFps, 20);
});

test('a disappearing stat ID invalidates only the changed interval then recovers', () => {
  const previous = baseline(
    rawStat({ id: 'a' }),
    rawStat({ id: 'b', framesReceived: 200, framesDecoded: 190, bytesReceived: 200_000 })
  );
  const changed = calculate(read(rawStat({
    id: 'a', timestamp: 2000, framesReceived: 110, framesDecoded: 108, bytesReceived: 150_000
  })), previous);
  assert.equal(changed.receivedFps, null);
  assert.equal(changed.nextSnapshot.size, 1);

  const recovered = calculate(read(rawStat({
    id: 'a', timestamp: 3000, framesReceived: 125, framesDecoded: 123, bytesReceived: 200_000
  })), changed.nextSnapshot);
  assert.equal(recovered.receivedFps, 15);
  assert.equal(recovered.decodedFps, 15);
  assert.equal(recovered.receivedMediaBitrateKbps, 400);
});

test('a changed member in a multi-object report never yields a partial aggregate rate', () => {
  const previous = baseline(
    rawStat({ id: 'a' }),
    rawStat({ id: 'b', framesReceived: 200, framesDecoded: 190, bytesReceived: 200_000 })
  );
  const result = calculate(read(
    rawStat({ id: 'a', timestamp: 2000, framesReceived: 110 }),
    rawStat({ id: 'c', timestamp: 2000, framesReceived: 20 })
  ), previous);
  assert.equal(result.framesReceived, 130);
  assert.equal(result.receivedFps, null);
  assert.equal(result.decodedFps, null);
  assert.equal(result.receivedMediaBitrateKbps, null);
});

test('an empty report models a rejected getStats call and resets all baselines', () => {
  const previous = baseline(rawStat());
  const failure = calculate([], previous);
  assert.equal(failure.elapsedMs, null);
  assert.equal(failure.framesReceived, null);
  assert.equal(failure.receivedFps, null);
  assert.equal(failure.receivedMediaBitrateKbps, null);
  assert.equal(failure.nextSnapshot.size, 0);

  const recoveredBaseline = calculate(read(rawStat({ timestamp: 3000 })), failure.nextSnapshot);
  assert.equal(recoveredBaseline.receivedFps, null);
  assert.equal(recoveredBaseline.receivedMediaBitrateKbps, null);
  const recovered = calculate(read(rawStat({
    timestamp: 4000,
    framesReceived: 115,
    framesDecoded: 113,
    bytesReceived: 150_000
  })), recoveredBaseline.nextSnapshot);
  assert.equal(recovered.receivedFps, 15);
  assert.equal(recovered.decodedFps, 15);
  assert.equal(recovered.receivedMediaBitrateKbps, 400);
});

test('the first valid sample after failure is a baseline; the following one calculates rates', () => {
  const failed = calculate([], baseline(rawStat()));
  const first = calculate(read(rawStat({ timestamp: 3000 })), failed.nextSnapshot);
  const second = calculate(read(rawStat({
    timestamp: 4000,
    framesReceived: 130,
    framesDecoded: 128,
    bytesReceived: 160_000
  })), first.nextSnapshot);
  assert.equal(first.receivedFps, null);
  assert.equal(second.receivedFps, 30);
  assert.equal(second.decodedFps, 30);
  assert.equal(second.receivedMediaBitrateKbps, 480);
});

test('same timestamps invalidate rates but preserve the last trustworthy baseline', () => {
  const previous = baseline(rawStat());
  const duplicate = calculate(read(rawStat({ timestamp: 1000, framesReceived: 110 })), previous);
  assert.equal(duplicate.elapsedMs, null);
  assert.equal(duplicate.receivedFps, null);
  assert.equal(duplicate.nextSnapshot.get('inbound-a')?.framesReceived, 100);

  const recovered = calculate(read(rawStat({ timestamp: 2000, framesReceived: 115 })), duplicate.nextSnapshot);
  assert.equal(recovered.receivedFps, 15);
});

test('decreasing timestamps invalidate rates then recover from the prior valid timestamp', () => {
  const previous = baseline(rawStat());
  const decreased = calculate(read(rawStat({ timestamp: 900, framesReceived: 110 })), previous);
  assert.equal(decreased.elapsedMs, null);
  assert.equal(decreased.receivedFps, null);
  assert.equal(decreased.nextSnapshot.get('inbound-a')?.timestamp, 1000);

  const recovered = calculate(read(rawStat({ timestamp: 2000, framesReceived: 120 })), decreased.nextSnapshot);
  assert.equal(recovered.receivedFps, 20);
});

test('missing timestamps never fall back to performance.now and rates resume from a valid interval', () => {
  const previous = baseline(rawStat());
  const missing = calculate(read(rawStat({ timestamp: undefined })), previous);
  assert.equal(missing.elapsedMs, null);
  assert.equal(missing.receivedFps, null);
  assert.equal(missing.nextSnapshot.get('inbound-a')?.timestamp, 1000);

  const recovered = calculate(read(rawStat({ timestamp: 2000, framesReceived: 115 })), missing.nextSnapshot);
  assert.equal(recovered.receivedFps, 15);
});

test('an inbound-rtp gap is recorded as missing and resets the next rate baseline', () => {
  const previous = baseline(rawStat());
  const empty = calculate(read({ type: 'inbound-rtp', kind: 'audio', id: 'audio', timestamp: 2000 }), previous);
  assert.equal(empty.inboundVideoStatsCount, 0);
  assert.equal(empty.framesReceived, null);
  assert.equal(empty.receivedFps, null);
  assert.equal(empty.nextSnapshot.size, 0);

  const next = calculate(read(rawStat({ timestamp: 3000 })), empty.nextSnapshot);
  assert.equal(next.receivedFps, null);
});

test('duplicate stat IDs are rejected rather than double-counted', () => {
  const result = calculate(read(rawStat({ id: 'same' }), rawStat({ id: 'same' })));
  assert.equal(result.packetsReceived, null);
  assert.equal(result.framesReceived, null);
  assert.equal(result.nextSnapshot.size, 0);
});

test('a getStats result arriving after polling stops cannot append a sample', async () => {
  let resolveReport;
  const reportPromise = new Promise(resolve => {
    resolveReport = resolve;
  });
  let stopped = false;
  let currentSession = true;
  let appendedSamples = 0;

  const completion = reportPromise.then(report => {
    if (!canApplyConsumerStatsResult(stopped, currentSession)) return;
    appendedSamples += report.length;
  });

  stopped = true;
  currentSession = false;
  resolveReport([rawStat()]);
  await completion;

  assert.equal(appendedSamples, 0);
});
