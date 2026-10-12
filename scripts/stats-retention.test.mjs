import assert from 'node:assert/strict';
import test from 'node:test';
import {
  MAX_STATS_DURATION_MS,
  MAX_STATS_SAMPLES,
  StatsDumpSession
} from '../src/stats/statsDump.ts';
import {
  createChartSeries,
  createElapsedSeconds,
  decimateChartSeries,
  formatElapsedTime
} from '../src/stats/statsChartData.ts';

function makeClock() {
  let now = 1000;
  let isoIndex = 0;
  return {
    clock: {
      monotonicNow: () => now,
      wallTimeIso: () => `2026-09-23T00:00:${String(isoIndex++).padStart(2, '0')}.000Z`
    },
    setNow(value) {
      now = value;
    }
  };
}

function makeSession(role, clock) {
  return new StatsDumpSession({
    peerId: 'peer-test',
    role,
    measurementStartTime: '2026-09-23T00:00:00.000Z',
    sampleIntervalMs: 1000,
    userAgent: 'retention-test',
    simulcastEnabled: role === 'producer',
    availableLayers: role === 'producer' ? ['low', 'mid', 'high'] : []
  }, clock);
}

function appendSample(session, role, index) {
  if (role === 'producer') {
    return session.appendProducerSample({
      timestamp: new Date(Date.UTC(2026, 8, 23, 0, 0, 0) + index * 1000).toISOString(),
      elapsedMs: 1000,
      sourceFps: 30,
      sourceFramesDelta: 30,
      sourceFramesTotal: index,
      layers: { high: { encodedChunksTotal: index } },
      socketBufferedAmountBytes: 0,
      browserJsHeapUsedBytes: null
    });
  }
  return session.appendConsumerSample({
    timestamp: new Date(Date.UTC(2026, 8, 23, 0, 0, 0) + index * 1000).toISOString(),
    elapsedMs: 1000,
    packetsReceived: index,
    packetsLost: 0,
    maxJitterMs: 0,
    framesReceived: 30,
    framesDecoded: 30,
    framesDropped: 0,
    bytesReceived: index * 100,
    receivedFps: 30,
    decodedFps: 30,
    receivedMediaBitrateKbps: 24,
    browserJsHeapUsedBytes: null
  });
}

for (const role of ['producer', 'consumer']) {
  test(`${role}: 1800, 3600, and 4199 samples record; 4200 is retained and stops`, () => {
    const { clock } = makeClock();
    const session = makeSession(role, clock);

    for (let index = 1; index <= 4199; index += 1) {
      assert.equal(appendSample(session, role, index), true);
      if ([1800, 3600, 4199].includes(index)) {
        assert.equal(session.sampleCount, index);
        assert.equal(session.collectionStatus, 'recording');
      }
    }

    assert.equal(appendSample(session, role, 4200), true);
    assert.equal(session.sampleCount, MAX_STATS_SAMPLES);
    assert.equal(session.collectionStatus, 'limit-reached');
    assert.equal(appendSample(session, role, 4201), false);
    assert.equal(session.sampleCount, MAX_STATS_SAMPLES);

    const dump = session.createDump();
    const samples = role === 'producer' ? dump.producerStats : dump.consumerStats;
    assert.equal(samples.length, MAX_STATS_SAMPLES);
    assert.equal(role === 'producer'
      ? dump.producerStats[0].sourceFramesTotal
      : dump.consumerStats[0].packetsReceived, 1);
    assert.equal(role === 'producer'
      ? dump.producerStats.at(-1).sourceFramesTotal
      : dump.consumerStats.at(-1).packetsReceived, 4200);
    assert.equal(dump.schemaVersion, 1);
    assert.equal(dump.metadata.collectionStatus, 'limit-reached');
    assert.equal(typeof dump.metadata.measurementEndTime, 'string');
  });

  test(`${role}: duration limit stops appends and preserves earlier samples`, () => {
    const { clock, setNow } = makeClock();
    const session = makeSession(role, clock);
    assert.equal(appendSample(session, role, 1), true);
    setNow(1000 + MAX_STATS_DURATION_MS - 1);
    assert.equal(appendSample(session, role, 2), true);
    setNow(1000 + MAX_STATS_DURATION_MS);
    assert.equal(appendSample(session, role, 3), false);
    assert.equal(session.sampleCount, 2);
    assert.equal(session.collectionStatus, 'limit-reached');
    assert.equal(appendSample(session, role, 4), false);
  });

  test(`${role}: stop is idempotent and the collected dump stays available`, () => {
    const { clock } = makeClock();
    const session = makeSession(role, clock);
    assert.equal(appendSample(session, role, 1), true);
    session.stop();
    const endTime = session.createDump().metadata.measurementEndTime;
    session.stop();
    assert.equal(session.collectionStatus, 'stopped');
    assert.equal(session.createDump().metadata.measurementEndTime, endTime);
    assert.equal(appendSample(session, role, 2), false);
    assert.equal(session.sampleCount, 1);
  });
}

test('a producer sample limit does not create separate per-layer sample caps', () => {
  const { clock } = makeClock();
  const session = makeSession('producer', clock);
  for (let index = 1; index <= MAX_STATS_SAMPLES; index += 1) {
    assert.equal(appendSample(session, 'producer', index), true);
  }
  assert.equal(session.sampleCount, MAX_STATS_SAMPLES);
  assert.equal(session.producerStats.length, MAX_STATS_SAMPLES);
});

test('elapsed-time coordinates use real timestamps and reject invalid or unordered time', () => {
  assert.deepEqual(createElapsedSeconds(
    '2026-09-23T00:00:00.000Z',
    ['2026-09-23T00:00:01.000Z', '2026-09-23T00:00:05.000Z']
  ), [1, 5]);
  assert.throws(
    () => createElapsedSeconds('invalid', []),
    /measurement start time is invalid/i
  );
  assert.throws(
    () => createElapsedSeconds('2026-09-23T00:00:00.000Z', [
      '2026-09-23T00:00:02.000Z',
      '2026-09-23T00:00:01.000Z'
    ]),
    /valid, unique, and increasing/i
  );
  assert.equal(formatElapsedTime(0), '00:00');
  assert.equal(formatElapsedTime(300), '05:00');
  assert.equal(formatElapsedTime(4200), '01:10:00');
});

test('render decimation preserves zero, peaks, and null gaps', () => {
  const elapsed = Array.from({ length: 4200 }, (_, index) => index);
  const values = elapsed.map(() => 1);
  values[0] = 0;
  values[2111] = 9999;
  values[2200] = null;
  values[2201] = null;
  const source = createChartSeries('Source FPS', '#000', elapsed, values);
  const reduced = decimateChartSeries(source);
  assert.ok(reduced.data.length < source.data.length);
  assert.ok(reduced.data.some(point => point.y === 9999));
  assert.ok(reduced.data.some(point => point.y === 0));
  assert.equal(reduced.data.filter(point => point.y === null).length, 1);
  const gapIndex = reduced.data.findIndex(point => point.y === null);
  assert.ok(gapIndex > 0 && gapIndex < reduced.data.length - 1);
  assert.ok(reduced.data[gapIndex - 1].x < reduced.data[gapIndex + 1].x);
  assert.equal(source.data.length, 4200);
  assert.equal(source.data[2200].y, null);
});
