import { createServer } from 'node:http';
import { networkInterfaces } from 'node:os';
import { performance } from 'node:perf_hooks';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { WebSocketServer, type RawData, type WebSocket } from 'ws';
import * as mediasoup from '../../mediasoup/node/lib/index.js';
import {
  getPipelineStats,
  injectEncodedChunk,
  setupPipeline
} from '../../mediasoup/node/lib/webCodecsPipeline.js';
import type * as MediasoupTypes from '../../mediasoup/node/lib/types.js';
import { resolveAnnouncedIp } from './serverNetwork.js';
import { IS_DEBUG, logger } from './serverLogger.js';
import { SystemMetricsCollector } from './systemMetricsCollector.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');
const localWorkerBin = path.resolve(
  rootDir,
  '..',
  'mediasoup',
  'worker',
  'out',
  'Release',
  process.platform === 'win32' ? 'mediasoup-worker.exe' : 'mediasoup-worker'
);

const HTTP_PORT = Number(process.env.PORT ?? 3000);
const SFU_E2E_MEDIA_ENABLED = process.env.SFU_E2E_MEDIA === '1';
const WEBRTC_PORT_RANGE = { min: 40000, max: 40100 } as const;

function getLocalIp(): string {
  const nets = networkInterfaces();
  let fallbackIp = '127.0.0.1';

  for (const name of Object.keys(nets)) {
    // 가상 네트워크 어댑터(WSL, Docker, VMware 등)는 건너뜁니다.
    const lowerName = name.toLowerCase();
    if (
      lowerName.includes('veth') ||
      lowerName.includes('wsl') ||
      lowerName.includes('virtual') ||
      lowerName.includes('vmware') ||
      lowerName.includes('docker')
    ) {
      continue;
    }

    for (const net of nets[name]!) {
      if (net.family === 'IPv4' && !net.internal) {
        // VirtualBox 가상 어댑터 대역(192.168.56.x)은 건너뜀.
        if (net.address.startsWith('192.168.56.')) {
          continue;
        }
        // 192.168.x.x (일반적인 가정용 공유기 IP)를 발견하면 최우선으로 반환
        if (net.address.startsWith('192.168.')) {
          return net.address;
        }
        // 다른 대역(예: 회사망 10.x.x.x 등)일 경우 일단 임시 저장
        if (fallbackIp === '127.0.0.1') {
          fallbackIp = net.address;
        }
      }
    }
  }
  return fallbackIp;
}

const SERVER_IP = resolveAnnouncedIp(process.env.SFU_ANNOUNCED_IP, getLocalIp());
logger.info('서버 주소: ', SERVER_IP);

const PIPELINE_PAYLOAD_TYPE = 96;

type PeerRole = 'producer' | 'consumer';
type SimulcastLayer = 'low' | 'mid' | 'high';

type SimulcastEncoding = {
  layer: SimulcastLayer;
  ssrc: number;
  scalabilityMode: 'L1T1';
};

type InputFpsSnapshot = {
  source: number;
  low: number;
  mid: number;
  high: number;
  windowMs: number;
};

type FeedbackLayerCounters = {
  pliReceived: number;
  firReceived: number;
  keyframeRequestsForwarded: number;
};

type ProducerFeedbackTraceAudit = {
  requestId?: string;
  traceType: 'pli' | 'fir';
  ssrc: number | null;
  layer: SimulcastLayer | null;
  traceAt: number;
  forwardedAt?: number;
};

type ProducerFeedbackAudit = {
  pliReceived: number;
  firReceived: number;
  keyframeRequestsForwarded: number;
  byLayer: Record<SimulcastLayer, FeedbackLayerCounters>;
  traces: ProducerFeedbackTraceAudit[];
};

type PendingE2EKeyframeRequest = {
  requestId: string;
  requestedAt: number;
};

type E2EConsumerLayersSnapshot = {
  spatialLayer: number;
  temporalLayer?: number;
} | null;

type E2ELayerChangeSample = {
  timestamp: number;
  spatialLayer: number | null;
  temporalLayer: number | null;
};

type E2ELayerSelectionSample = {
  timestamp: number;
  sampleDurationMs: number;
  preferredLayers: E2EConsumerLayersSnapshot;
  currentLayers: E2EConsumerLayersSnapshot;
  priority: number;
  score: {
    score: number;
    producerScore: number;
    producerScores: number[];
  };
  paused: boolean;
  producerPaused: boolean;
  consumerStats: Array<{
    type: string;
    ssrc: number;
    score: number;
    packetCount: number;
    byteCount: number;
    bitrate: number;
  }> | null;
  consumerStatsError: string | null;
  transportStats: Array<{
    type: string;
    availableOutgoingBitrate: number | null;
    availableIncomingBitrate: number | null;
    maxOutgoingBitrate: number | null;
    bytesSent: number;
    sendBitrate: number;
    rtpBytesSent: number;
    rtpSendBitrate: number;
    iceState: string;
    dtlsState: string;
    iceSelectedTuple: unknown;
  }> | null;
  transportStatsError: string | null;
};

type E2ELayerSelectionDiagnostics = {
  stage: 'waitForConsumerSpatialLayer';
  result: 'reached' | 'timed-out';
  requestKeyFrameReached: false;
  requestedSpatialLayer: number;
  setPreferredLayersStartedAt: number;
  setPreferredLayersCompletedAt: number;
  setPreferredLayersDurationMs: number;
  waitStartedAt: number;
  waitDeadlineAt: number;
  waitElapsedMs: number;
  transitionElapsedMs: number | null;
  targetReachedAt: number | null;
  preferredLayersBefore: E2EConsumerLayersSnapshot;
  preferredLayersAfterSet: E2EConsumerLayersSnapshot;
  initialCurrentLayers: E2EConsumerLayersSnapshot;
  finalPreferredLayers: E2EConsumerLayersSnapshot;
  finalCurrentLayers: E2EConsumerLayersSnapshot;
  finalPriority: number;
  finalScore: {
    score: number;
    producerScore: number;
    producerScores: number[];
  };
  finalPaused: boolean;
  finalProducerPaused: boolean;
  finalAvailableOutgoingBitrate: number | null;
  observedCurrentSpatialLayerSequence: Array<{
    timestamp: number;
    spatialLayer: number | null;
  }>;
  layerschangeSamples: E2ELayerChangeSample[];
  availableOutgoingBitrateSamples: Array<{
    timestamp: number;
    availableOutgoingBitrate: number | null;
  }>;
  samples: E2ELayerSelectionSample[];
};

type E2EPreferredLayersFailureDiagnostics = {
  stage: 'setPreferredLayers';
  result: 'failed';
  requestKeyFrameReached: false;
  requestedSpatialLayer: number;
  setPreferredLayersStartedAt: number;
  setPreferredLayersFailedAt: number;
  preferredLayersBefore: E2EConsumerLayersSnapshot;
  preferredLayersAfterFailure: E2EConsumerLayersSnapshot;
  initialCurrentLayers: E2EConsumerLayersSnapshot;
  error: string;
};

type E2ELayerSelectionFailureDiagnostics =
  | E2ELayerSelectionDiagnostics
  | E2EPreferredLayersFailureDiagnostics;

class ConsumerSpatialLayerSelectionTimeout extends Error {
  constructor(readonly diagnostics: E2ELayerSelectionDiagnostics) {
    super(`Consumer did not switch to spatial layer ${diagnostics.requestedSpatialLayer}`);
    this.name = 'ConsumerSpatialLayerSelectionTimeout';
  }
}

class ConsumerPreferredLayersSelectionFailure extends Error {
  constructor(readonly diagnostics: E2EPreferredLayersFailureDiagnostics) {
    super(`setPreferredLayers failed for spatial layer ${diagnostics.requestedSpatialLayer}: ${diagnostics.error}`);
    this.name = 'ConsumerPreferredLayersSelectionFailure';
  }
}

const MAX_E2E_LAYER_DIAGNOSTIC_SAMPLES = 100;
const E2E_LAYER_DIAGNOSTIC_SAMPLE_INTERVAL_MS = 200;
const e2eLayerChangeSamples = new WeakMap<
  MediasoupTypes.Consumer,
  E2ELayerChangeSample[]
>();

const SIMULCAST_ENCODINGS: readonly SimulcastEncoding[] = [
  {
    layer: 'low',
    ssrc: 12345678,
    scalabilityMode: 'L1T1'
  },
  {
    layer: 'mid',
    ssrc: 12345679,
    scalabilityMode: 'L1T1'
  },
  {
    layer: 'high',
    ssrc: 12345680,
    scalabilityMode: 'L1T1'
  }
];
const HIGH_ENCODING = SIMULCAST_ENCODINGS[2]!;

type JsonMessage = {
  id?: string;
  action?: string;
  data?: unknown;
};

type EncodedChunkHeader = {
  event: 'encodedChunk';
  layer: SimulcastLayer;
  timestamp: number;
  type: 'key' | 'delta';
  duration?: number;
  metadata?: {
    decoderConfig?: {
      codec?: string;
      descriptionBase64?: string;
    };
  };
};

type PeerState = {
  id: string;
  role: PeerRole;
  socket: WebSocket;
  transport?: MediasoupTypes.WebRtcTransport;
  consumer?: MediasoupTypes.Consumer;
  e2eConsumerCreatedAt?: number;
  statsTimer?: NodeJS.Timeout;
  consumerCleanupTimer?: NodeJS.Timeout;
};

const CONSUMER_DISCONNECT_GRACE_MS = 15_000;

const app = express();
const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
const peers = new Map<string, PeerState>();

type ShutdownReason =
  | NodeJS.Signals
  | 'startup-error'
  | 'server-error'
  | 'worker-died';

let worker: MediasoupTypes.Worker | undefined;
let pingTimer: NodeJS.Timeout | undefined;
let shuttingDown = false;
let shutdownPromise: Promise<void> | undefined;
let router: MediasoupTypes.Router;
let producer: MediasoupTypes.Producer | undefined;
let producerPeerId: string | undefined;
let producerSimulcastEnabled: boolean | undefined;
let activeProducerEncodings: readonly SimulcastEncoding[] = [];
let injectedChunks = 0;
let peerSeq = 0;
let inputFpsWindowStartedAt: number | undefined;
let lastInputSourceTimestamp: number | undefined;
let inputFpsSourceFrames = 0;
let inputFpsLowFrames = 0;
let inputFpsMidFrames = 0;
let inputFpsHighFrames = 0;
let latestInputFps: InputFpsSnapshot = createEmptyInputFps();
let keyframeRequestSequence = 0;
const pendingE2EKeyframeRequests = new Map<
  SimulcastLayer,
  PendingE2EKeyframeRequest
>();
const producerFeedbackAudit = SFU_E2E_MEDIA_ENABLED
  ? createProducerFeedbackAudit()
  : undefined;

app.use(express.static(path.join(rootDir, 'dist')));

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    announcedIp: SERVER_IP,
    webrtcPortRange: WEBRTC_PORT_RANGE,
    producerPeerId,
    producerId: producer?.id,
    simulcastEnabled: producerSimulcastEnabled ?? null,
    activeLayers: activeProducerEncodings.map(encoding => encoding.layer),
    producerType: producer?.type ?? null,
    consumers: countConsumers(),
    injectedChunks,
    injectedPackets: getPipelineStats().rtpPackets,
    inputFps: latestInputFps,
    ...(SFU_E2E_MEDIA_ENABLED
      ? { e2eConsumerAudit: snapshotE2EConsumerAudit() }
      : {}),
    ...(producerFeedbackAudit
      ? { keyframeFeedback: snapshotProducerFeedbackAudit() }
      : {})
  });
});

// 클라이언트가 접속하면 이벤트를 감지하고 registerPeer 함수를 통해 역할을 나눈다.
// 웹소켓 연결 수립
wss.on('connection', async (socket, request) => {
  if (shuttingDown) {
    socket.terminate();
    return;
  }

  const peer = await registerPeer(socket, parseSimulcastPreference(request.url)); // 접속자 정보 등록 및 역할 부여

  socket.send( // 부여받은 역할(Producer/Consumer)을 클라이언트에게 응답
    JSON.stringify({
      event: 'roleAssigned',
      data: {
        peerId: peer.id,
        role: peer.role,
        producerPeerId,
        producerId: producer?.id,
        simulcastEnabled: producerSimulcastEnabled ?? true,
        availableLayers: activeProducerEncodings.map(encoding => encoding.layer),
        ssrc: activeProducerEncodings[0]?.ssrc,
        ssrcs: activeProducerEncodings.map(encoding => encoding.ssrc),
        payloadType: PIPELINE_PAYLOAD_TYPE,
        consumerCount: countConsumers()
      }
    })
  );
  broadcastRoomState();

  socket.on('message', async (message, isBinary) => {
    let requestId: string | undefined;

    try {
      if (isBinary) { // A. 바이너리 데이터는 Producer의 영상 프레임으로 간주
        handleEncodedChunk(peer, message);
        return;
      }

      // B. 일반 텍스트는 Consumer의 시그널링 요청으로 간주
      const parsed = JSON.parse(message.toString()) as JsonMessage;
      requestId = parsed.id;
      await handleRequest(socket, peer, parsed);
    } catch (error) {
      sendError(socket, error, requestId);
    }
  });

  socket.on('close', () => {
    if (!shuttingDown) {
      if (peer.role === 'consumer') {
        stopStatsPump(peer);
        checkConsumerAfterSignalingClose(peer);
      } else {
        cleanupPeer(peer);
      }
    }
  });
});

function handleServerError(error: Error): void {
  if (shuttingDown) {
    return;
  }

  logger.error('[signaling] HTTP/WebSocket server error', error);
  void shutdown('server-error', 1);
}

httpServer.on('error', handleServerError);
wss.on('error', handleServerError);

process.once('SIGINT', () => {
  void shutdown('SIGINT');
});
process.once('SIGTERM', () => {
  void shutdown('SIGTERM');
});

async function startServer(): Promise<void> {
  SystemMetricsCollector.start();

  try {
    await bootstrapMediasoup();
    if (shuttingDown) {
      return;
    }

    httpServer.listen(HTTP_PORT, '0.0.0.0', () => {
      if (shuttingDown) {
        return;
      }

      logger.info(`[signaling] backend ready on http://${SERVER_IP}:${HTTP_PORT}`);
      pingTimer = setInterval(() => {
        wss.clients.forEach(client => {
          if (client.readyState === 1) {
            client.ping();
          }
        });
      }, 30000);
    });
  } catch (error) {
    if (shuttingDown) {
      return;
    }

    logger.error('[signaling] startup failed', error);
    await shutdown('startup-error', 1);
  }
}

async function bootstrapMediasoup(): Promise<void> {
  const createdWorker = await mediasoup.createWorker({ // 워커(프로세스) 실행
    workerBin: localWorkerBin,
    logLevel: IS_DEBUG ? 'debug' : 'warn',
    rtcMinPort: 40000,
    rtcMaxPort: 40100
  });
  worker = createdWorker;

  createdWorker.on('died', error => {
    if (shuttingDown) {
      return;
    }

    logger.error('mediasoup worker died', error);
    void shutdown('worker-died', 1);
  });

  if (shuttingDown) {
    createdWorker.close();
    if (worker === createdWorker) {
      worker = undefined;
    }
    return;
  }

  router = await createdWorker.createRouter({ // 라우터(방) 생성
    mediaCodecs: [
      {
        kind: 'video',
        mimeType: 'video/H264', // H264 코덱 사용
        clockRate: 90000,
        parameters: {
          'packetization-mode': 1,
          'profile-level-id': '42e01f',
          'level-asymmetry-allowed': 1
        },
        rtcpFeedback: [
          { type: 'nack' },
          { type: 'nack', parameter: 'pli' },
          { type: 'ccm', parameter: 'fir' }
        ]
      }
    ]
  });

  logger.info('mediasoup router ready, waiting for first producer peer');
}

function shutdown(
  reason: ShutdownReason,
  exitCode = 0
): Promise<void> {
  if (exitCode !== 0) {
    process.exitCode = exitCode;
  }
  if (shutdownPromise) {
    return shutdownPromise;
  }

  shuttingDown = true;
  SystemMetricsCollector.stop();
  logger.info(`[shutdown] ${reason}`);
  shutdownPromise = (async () => {
    if (pingTimer) {
      clearInterval(pingTimer);
      pingTimer = undefined;
    }

    const webSocketServerClosed = closeWebSocketServer();
    const httpServerClosed = closeHttpServer();

    for (const peer of peers.values()) {
      clearTimeout(peer.consumerCleanupTimer);
      stopStatsPump(peer);
      try {
        peer.consumer?.close();
      } catch (error) {
        logShutdownFailure('consumer', error);
      }
      try {
        peer.transport?.close();
      } catch (error) {
        logShutdownFailure('transport', error);
      }
      try {
        peer.socket.terminate();
      } catch (error) {
        logShutdownFailure('WebSocket client', error);
      }
    }
    peers.clear();

    try {
      producer?.close();
    } catch (error) {
      logShutdownFailure('producer', error);
    }
    producer = undefined;
    producerPeerId = undefined;
    producerSimulcastEnabled = undefined;
    activeProducerEncodings = [];
    pendingE2EKeyframeRequests.clear();

    await Promise.all([webSocketServerClosed, httpServerClosed]);

    const closingWorker = worker;
    worker = undefined;
    try {
      closingWorker?.close();
    } catch (error) {
      logShutdownFailure('mediasoup worker', error);
    }
  })();

  return shutdownPromise;
}

function closeWebSocketServer(): Promise<void> {
  return new Promise(resolve => {
    try {
      wss.close(error => {
        if (error) {
          logShutdownFailure('WebSocket server', error);
        }
        resolve();
      });
    } catch (error) {
      logShutdownFailure('WebSocket server', error);
      resolve();
    }

    for (const client of wss.clients) {
      try {
        client.terminate();
      } catch (error) {
        logShutdownFailure('WebSocket client', error);
      }
    }
  });
}

function closeHttpServer(): Promise<void> {
  return new Promise(resolve => {
    if (!httpServer.listening) {
      resolve();
      return;
    }

    try {
      httpServer.close(error => {
        if (error && (error as NodeJS.ErrnoException).code !== 'ERR_SERVER_NOT_RUNNING') {
          logShutdownFailure('HTTP server', error);
        }
        resolve();
      });
      httpServer.closeAllConnections?.();
    } catch (error) {
      logShutdownFailure('HTTP server', error);
      resolve();
    }
  });
}

function logShutdownFailure(resource: string, error: unknown): void {
  process.exitCode = 1;
  logger.error(`[shutdown] failed to close ${resource}`, error);
}

void startServer();

function parseSimulcastPreference(requestUrl: string | undefined): boolean {
  if (!requestUrl) {
    return true;
  }

  try {
    const value = new URL(requestUrl, 'http://localhost').searchParams.get('simulcast');
    return value === '0' ? false : true;
  } catch {
    return true;
  }
}

async function registerPeer(
  socket: WebSocket,
  requestedSimulcastEnabled = true
): Promise<PeerState> {
  const id = `peer-${++peerSeq}`;
  const role: PeerRole = producerPeerId ? 'consumer' : 'producer'; // 프로듀서가 이미 있으면 Consumer, 없으면 Producer
  const peer: PeerState = { id, role, socket };

  peers.set(id, peer);


  // 송신자를 위해 라우터(router)를 넘겨서 WebCodecs용 파이프라인을 연결
  // (이 내부 코드에서 router.createDirectTransport()가 호출되어 라우터와 연결됨.)
  if (role === 'producer') {
    producerPeerId = id;
    producerSimulcastEnabled = requestedSimulcastEnabled;
    activeProducerEncodings = producerSimulcastEnabled
      ? SIMULCAST_ENCODINGS
      : [HIGH_ENCODING];
    injectedChunks = 0;
    resetInputFps();
    let pipelineProducer: MediasoupTypes.Producer;
    try {
      pipelineProducer = await setupPipeline(router, {
        encodings: activeProducerEncodings,
        payloadType: PIPELINE_PAYLOAD_TYPE,
        profileLevelId: '42e01f'
      });
    } catch (error) {
      producerPeerId = undefined;
      producerSimulcastEnabled = undefined;
      activeProducerEncodings = [];
      peers.delete(id);
      throw error;
    }
    producer = pipelineProducer;
    // PLI/FIR -> WebCodecs keyframe bridge is intentionally disabled.
    // await pipelineProducer.enableTraceEvent(['pli', 'fir']);
    // pipelineProducer.on('trace', handleProducerTrace);
    // 저빈도(startup/peer당 1회) 진단. production에서는 문자열/JSON 생성 자체를 생략.
    if (IS_DEBUG) {
      logger.debug(`[room] ${id} joined as producer, pipeline producer ${pipelineProducer.id}`);
      logger.debug(
        `[pipeline] producer ${pipelineProducer.id} type=${pipelineProducer.type} encodings=${JSON.stringify(pipelineProducer.rtpParameters.encodings)}`
      );
    }
  } else if (IS_DEBUG) {
    logger.debug(`[room] ${id} joined as consumer of ${producerPeerId}`);
  }

  logPeerCount('join', peer);

  return peer;
}

async function handleRequest(
  socket: WebSocket,
  peer: PeerState,
  message: JsonMessage
): Promise<void> {
  const { id, action, data } = message;

  if (!id || !action) {
    throw new Error('Invalid request');
  }

  if (action === 'getRouterRtpCapabilities') {
    reply(socket, id, router.rtpCapabilities);
    return;
  }

  if (action === 'createConsumerTransport') {
    assertConsumerPeer(peer);

    // 1. 라우터 객체를 통해 클라이언트와 통신할 WebRTC Transport를 생성
    peer.transport = await router.createWebRtcTransport({
      listenInfos: [
        { protocol: 'udp', ip: '0.0.0.0', announcedIp: SERVER_IP, portRange: WEBRTC_PORT_RANGE },
        { protocol: 'tcp', ip: '0.0.0.0', announcedIp: SERVER_IP, portRange: WEBRTC_PORT_RANGE }
      ],
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
      initialAvailableOutgoingBitrate: 1_000_000
    });

    peer.transport.on('icestatechange', state => {
      // 진단용 알림: debug 모드에서만 전송 (client는 debugLog로만 사용)
      if (IS_DEBUG && socket.readyState === 1) {
        socket.send(JSON.stringify({ event: 'transportState', data: { ice: state } }));
      }
      checkConsumerAfterSignalingClose(peer);
    });
    peer.transport.on('dtlsstatechange', state => {
      // 진단용 알림: debug 모드에서만 전송 (client는 debugLog로만 사용)
      if (IS_DEBUG && socket.readyState === 1) {
        socket.send(JSON.stringify({ event: 'transportState', data: { dtls: state } }));
      }
      checkConsumerAfterSignalingClose(peer);
    });
    peer.transport.observer.on('close', () => checkConsumerAfterSignalingClose(peer));

    reply(socket, id, {
      id: peer.transport.id,
      iceParameters: peer.transport.iceParameters,
      iceCandidates: peer.transport.iceCandidates,
      dtlsParameters: peer.transport.dtlsParameters
    });
    return;
  }

  // 2. 클라이언트 측에서 만들어진 보안 파라미터(DTLS)를 받아 서버측 Transport와 암호화 통신 채널을 확립(Connect)
  if (action === 'connectConsumerTransport') {
    assertConsumerPeer(peer);

    if (!peer.transport) {
      throw new Error('Consumer transport does not exist');
    }

    const { dtlsParameters } = data as {
      dtlsParameters: MediasoupTypes.DtlsParameters;
    };

    await peer.transport.connect({ dtlsParameters });
    reply(socket, id, {});
    return;
  }

  // 3. 완전히 연결된 Transport 위에 미디어 데이터를 흘려보낼 Consumer 객체를 생성. 여기서 어떤 송신자의 영상(producer.id)을 받을지 결정
  if (action === 'consume') {
    assertConsumerPeer(peer);

    if (!peer.transport) {
      throw new Error('Consumer transport does not exist');
    }
    if (!producer) {
      throw new Error('Producer is not ready yet');
    }

    const { rtpCapabilities } = data as {
      rtpCapabilities: MediasoupTypes.RtpCapabilities;
    };

    if (!router.canConsume({ producerId: producer.id, rtpCapabilities })) {
      throw new Error('Client cannot consume the WebCodecs producer');
    }

    peer.consumer = await peer.transport.consume({
      producerId: producer.id,
      rtpCapabilities,
      paused: true
    });
    peer.consumer.observer.on('close', () => checkConsumerAfterSignalingClose(peer));
    if (SFU_E2E_MEDIA_ENABLED) {
      peer.e2eConsumerCreatedAt = Date.now();
    }

    if (SFU_E2E_MEDIA_ENABLED) {
      const layerChangeSamples: E2ELayerChangeSample[] = [];
      e2eLayerChangeSamples.set(peer.consumer, layerChangeSamples);
      peer.consumer.on('layerschange', layers => {
        if (layerChangeSamples.length >= MAX_E2E_LAYER_DIAGNOSTIC_SAMPLES) {
          return;
        }
        layerChangeSamples.push({
          timestamp: Date.now(),
          spatialLayer: layers?.spatialLayer ?? null,
          temporalLayer: layers?.temporalLayer ?? null
        });
      });
    }

    // Consumer RTP/keyframe traces and human-readable layerschange logs are debug-only.
    // The Producer's PLI/FIR trace above is the production keyframe control path.
    if (IS_DEBUG) {
      await peer.consumer.enableTraceEvent(['rtp', 'keyframe', 'pli', 'fir']);
      peer.consumer.on('layerschange', layers => {
        logger.debug(
          `[simulcast] ${peer.id} current layers ${JSON.stringify(layers)}`
        );
      });
      logger.debug(
        `[simulcast] consumer ${peer.consumer.id} type=${peer.consumer.type}`
      );
    }

    peer.consumer.on('transportclose', () => {
      stopStatsPump(peer);
    });

    reply(socket, id, {
      id: peer.consumer.id,
      producerId: producer.id,
      kind: peer.consumer.kind,
      rtpParameters: peer.consumer.rtpParameters
    });
    return;
  }

  if (action === 'resumeConsumer') {
    assertConsumerPeer(peer);

    if (!peer.consumer) {
      throw new Error('Consumer does not exist');
    }

    await peer.consumer.resume();
    // serverStats 진단은 debug 전용: production에서는 getStats 폴링도 하지 않는다.
    // (FPS telemetry의 consumer-side getStats / SFU Input FPS와 무관)
    if (IS_DEBUG) {
      startStatsPump(socket, peer);
    }
    reply(socket, id, {});
    return;
  }

  if (action === 'setConsumerLayer') {
    assertConsumerPeer(peer);
    if (!peer.consumer) {
      throw new Error('Consumer does not exist');
    }
    if (producerSimulcastEnabled !== true) {
      throw new Error('Consumer layer preference requires an active simulcast Producer');
    }

    const requestedLayer = (data as { layer?: unknown } | undefined)?.layer;
    const encoding = activeProducerEncodings.find(
      candidate => candidate.layer === requestedLayer
    );
    if (!encoding) {
      throw new Error(`Invalid consumer preferred layer: ${String(requestedLayer)}`);
    }

    const spatialLayer = activeProducerEncodings.indexOf(encoding);
    await peer.consumer.setPreferredLayers({ spatialLayer });
    reply(socket, id, {
      layer: encoding.layer,
      spatialLayer,
      preferredLayers: snapshotE2EConsumerLayers(peer.consumer.preferredLayers)
    });
    return;
  }

  if (action === 'e2eRequestKeyFrame') {
    if (!SFU_E2E_MEDIA_ENABLED) {
      socket.send(
        JSON.stringify({ id, ok: false, error: 'E2E media controls are disabled' })
      );
      return;
    }
    assertConsumerPeer(peer);
    if (!peer.consumer) {
      throw new Error('Consumer does not exist');
    }

    const requestedLayer = (data as { layer?: unknown } | undefined)?.layer;
    const encoding = activeProducerEncodings.find(
      candidate => candidate.layer === requestedLayer
    );
    if (!encoding) {
      throw new Error(`Invalid E2E simulcast layer: ${String(requestedLayer)}`);
    }

    const spatialLayer = activeProducerEncodings.indexOf(encoding);
    const layerSelection = await selectE2EConsumerSpatialLayer(
      peer,
      spatialLayer
    );

    const requestedAt = Date.now();
    const requestId = id;
    pendingE2EKeyframeRequests.set(encoding.layer, { requestId, requestedAt });
    try {
      await peer.consumer.requestKeyFrame();
    } catch (error) {
      if (pendingE2EKeyframeRequests.get(encoding.layer)?.requestId === requestId) {
        pendingE2EKeyframeRequests.delete(encoding.layer);
      }
      throw error;
    }

    reply(socket, id, {
      requestId,
      layer: encoding.layer,
      spatialLayer,
      requestedAt,
      requestKeyFrameReached: true,
      layerSelection
    });
    return;
  }

  if (action === 'e2eSelectConsumerLayer') {
    if (!SFU_E2E_MEDIA_ENABLED) {
      socket.send(
        JSON.stringify({ id, ok: false, error: 'E2E media controls are disabled' })
      );
      return;
    }
    assertConsumerPeer(peer);
    if (!peer.consumer) {
      throw new Error('Consumer does not exist');
    }
    if (producerSimulcastEnabled !== true) {
      throw new Error('E2E layer controls require an active simulcast Producer');
    }

    const requestedLayer = (data as { layer?: unknown } | undefined)?.layer;
    const encoding = activeProducerEncodings.find(
      candidate => candidate.layer === requestedLayer
    );
    if (!encoding) {
      throw new Error(`Invalid E2E simulcast layer: ${String(requestedLayer)}`);
    }

    const spatialLayer = activeProducerEncodings.indexOf(encoding);
    const layerSelection = await selectE2EConsumerSpatialLayer(
      peer,
      spatialLayer
    );
    reply(socket, id, {
      layer: encoding.layer,
      spatialLayer,
      layerSelection
    });
    return;
  }

  if (action === 'e2eCaptureMediaDiagnostics') {
    if (!SFU_E2E_MEDIA_ENABLED) {
      socket.send(
        JSON.stringify({ id, ok: false, error: 'E2E media controls are disabled' })
      );
      return;
    }
    assertConsumerPeer(peer);
    if (!peer.consumer) {
      throw new Error('Consumer does not exist');
    }

    reply(socket, id, await captureE2EMediaDiagnostics());
    return;
  }

  throw new Error(`Unknown action: ${action}`);
}

const E2E_MEDIA_STATS_FIELDS = [
  'type', 'timestamp', 'ssrc', 'rid', 'kind', 'mimeType',
  'packetCount', 'byteCount', 'bitrate', 'packetsReceived', 'packetsSent',
  'packetsLost', 'bytesReceived', 'bytesSent', 'rtxPacketsSent',
  'rtxBytesSent', 'nackCount', 'pliCount', 'firCount', 'score',
  'availableOutgoingBitrate', 'availableIncomingBitrate', 'maxOutgoingBitrate',
  'rtpBytesSent', 'rtpSendBitrate', 'iceState', 'dtlsState'
] as const;

function snapshotE2EMediaStatsRows(stats: unknown[] | null) {
  if (!stats) {
    return null;
  }
  return stats.map(stat => {
    const record = stat as Record<string, unknown>;
    return Object.fromEntries(
      E2E_MEDIA_STATS_FIELDS.flatMap(field => {
        const value = record[field];
        return value === null || ['string', 'number', 'boolean'].includes(typeof value)
          ? [[field, value]]
          : [];
      })
    );
  });
}

async function captureE2EMediaDiagnostics() {
  const capturedAt = Date.now();
  const consumerPeers = [...peers.values()].filter(
    candidate => candidate.role === 'consumer'
  );
  const producerStatsPromise = readE2EDiagnosticStats(
    () => producer?.getStats() ?? Promise.resolve([]),
    'Producer'
  );
  const consumerSnapshotsPromise = Promise.all(consumerPeers.map(async candidate => {
    const consumer = candidate.consumer;
    const transport = candidate.transport;
    const [consumerStats, transportStats] = await Promise.all([
      readE2EDiagnosticStats(
        () => consumer?.getStats() ?? Promise.resolve([]),
        `Consumer ${candidate.id}`
      ),
      readE2EDiagnosticStats(
        () => transport?.getStats() ?? Promise.resolve([]),
        `Transport ${candidate.id}`
      )
    ]);
    const score = consumer?.score;
    return {
      peerId: candidate.id,
      consumerId: consumer?.id ?? null,
      producerId: consumer?.producerId ?? null,
      transportId: transport?.id ?? null,
      consumerType: consumer?.type ?? null,
      preferredLayers: snapshotE2EConsumerLayers(consumer?.preferredLayers),
      currentLayers: snapshotE2EConsumerLayers(consumer?.currentLayers),
      paused: consumer?.paused ?? null,
      producerPaused: consumer?.producerPaused ?? null,
      priority: consumer?.priority ?? null,
      score: score
        ? {
            score: score.score,
            producerScore: score.producerScore,
            producerScores: [...score.producerScores]
          }
        : null,
      consumerStats: {
        stats: snapshotE2EMediaStatsRows(consumerStats.value),
        error: consumerStats.error
      },
      transportStats: {
        stats: snapshotE2EMediaStatsRows(transportStats.value),
        error: transportStats.error
      }
    };
  }));
  const [producerStats, consumerSnapshots] = await Promise.all([
    producerStatsPromise,
    consumerSnapshotsPromise
  ]);
  return {
    capturedAt,
    producer: {
      producerId: producer?.id ?? null,
      kind: producer?.kind ?? null,
      type: producer?.type ?? null,
      paused: producer?.paused ?? null,
      stats: snapshotE2EMediaStatsRows(producerStats.value),
      statsError: producerStats.error,
      pipeline: {
        injectedChunks,
        injectedPackets: getPipelineStats().rtpPackets,
        inputFps: { ...latestInputFps }
      }
    },
    consumers: consumerSnapshots
  };
}

async function selectE2EConsumerSpatialLayer(
  peer: PeerState,
  spatialLayer: number
): Promise<E2ELayerSelectionDiagnostics> {
  if (producerSimulcastEnabled !== true) {
    throw new Error('E2E layer controls require an active simulcast Producer');
  }
  if (!peer.consumer) {
    throw new Error('Consumer does not exist');
  }
  if (!peer.transport) {
    throw new Error('Consumer transport does not exist');
  }

  const setPreferredLayersStartedAt = Date.now();
  const preferredLayersBefore = snapshotE2EConsumerLayers(
    peer.consumer.preferredLayers
  );
  const initialCurrentLayers = snapshotE2EConsumerLayers(
    peer.consumer.currentLayers
  );
  try {
    await peer.consumer.setPreferredLayers({ spatialLayer });
  } catch (error) {
    const setPreferredLayersFailedAt = Date.now();
    const message = error instanceof Error ? error.message : String(error);
    throw new ConsumerPreferredLayersSelectionFailure({
      stage: 'setPreferredLayers',
      result: 'failed',
      requestKeyFrameReached: false,
      requestedSpatialLayer: spatialLayer,
      setPreferredLayersStartedAt,
      setPreferredLayersFailedAt,
      preferredLayersBefore,
      preferredLayersAfterFailure: snapshotE2EConsumerLayers(
        peer.consumer.preferredLayers
      ),
      initialCurrentLayers,
      error: message
    });
  }
  const setPreferredLayersCompletedAt = Date.now();
  const preferredLayersAfterSet = snapshotE2EConsumerLayers(
    peer.consumer.preferredLayers
  );
  return waitForConsumerSpatialLayer(
    peer.consumer,
    peer.transport,
    spatialLayer,
    {
      setPreferredLayersStartedAt,
      setPreferredLayersCompletedAt,
      preferredLayersBefore,
      preferredLayersAfterSet,
      initialCurrentLayers
    }
  );
}

function handleProducerTrace(
  trace: MediasoupTypes.ProducerTraceEventData
): void {
  if (trace.type !== 'pli' && trace.type !== 'fir') {
    return;
  }

  const ssrc = getTraceSsrc(trace.info);
  const encoding = ssrc === undefined
    ? undefined
    : activeProducerEncodings.find(candidate => candidate.ssrc === ssrc);
  const traceAt = Date.now();
  const pendingCandidate = encoding
    ? pendingE2EKeyframeRequests.get(encoding.layer)
    : undefined;
  const pending = pendingCandidate && traceAt - pendingCandidate.requestedAt <= 15_000
    ? pendingCandidate
    : undefined;
  if (pending && encoding) {
    pendingE2EKeyframeRequests.delete(encoding.layer);
  }

  const requestId = pending
    ? pending.requestId
    : `sfu-kf-${++keyframeRequestSequence}`;
  const auditRecord: ProducerFeedbackTraceAudit | undefined = producerFeedbackAudit
    ? {
        ...(pending ? { requestId: pending.requestId } : {}),
        traceType: trace.type,
        ssrc: ssrc ?? null,
        layer: encoding?.layer ?? null,
        traceAt
      }
    : undefined;

  if (producerFeedbackAudit && auditRecord) {
    if (trace.type === 'pli') {
      producerFeedbackAudit.pliReceived++;
    } else {
      producerFeedbackAudit.firReceived++;
    }
    if (encoding) {
      producerFeedbackAudit.byLayer[encoding.layer][
        trace.type === 'pli' ? 'pliReceived' : 'firReceived'
      ]++;
    }
    producerFeedbackAudit.traces.push(auditRecord);
    if (producerFeedbackAudit.traces.length > 100) {
      producerFeedbackAudit.traces.shift();
    }
  }

  if (!encoding) {
    if (IS_DEBUG) {
      logger.debug(
        `[simulcast] ignored ${trace.type.toUpperCase()} trace with unknown SSRC ${ssrc ?? 'missing'}`
      );
    }
    return;
  }

  const producerPeer = producerPeerId ? peers.get(producerPeerId) : undefined;
  if (
    !producerPeer ||
    producerPeer.role !== 'producer' ||
    producerPeer.socket.readyState !== 1
  ) {
    if (IS_DEBUG) {
      logger.debug(
        `[simulcast] dropped ${trace.type.toUpperCase()} request because the Producer socket is unavailable`
      );
    }
    return;
  }

  try {
    /* PLI/FIR -> WebCodecs keyframe forwarding is intentionally disabled.
    producerPeer.socket.send(
      JSON.stringify({
        event: 'keyframeRequest',
        data: {
          requestId,
          layer: encoding.layer,
          reason: trace.type,
          requestedAt: pending?.requestedAt,
          traceAt
        }
      })
    );
    */
    if (auditRecord) {
      auditRecord.forwardedAt = Date.now();
    }
    if (producerFeedbackAudit) {
      producerFeedbackAudit.keyframeRequestsForwarded++;
      producerFeedbackAudit.byLayer[encoding.layer].keyframeRequestsForwarded++;
    }
    if (IS_DEBUG) {
      logger.debug(
        `[simulcast] forwarded ${trace.type.toUpperCase()} keyframe request for ${encoding.layer}`
      );
    }
  } catch (error) {
    if (IS_DEBUG) {
      logger.debug(
        `[simulcast] failed to forward ${trace.type.toUpperCase()} keyframe request: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }
}

function getTraceSsrc(info: Record<string, unknown>): number | undefined {
  const directSsrc = info.ssrc;
  if (typeof directSsrc === 'number' && Number.isInteger(directSsrc)) {
    return directSsrc;
  }

  const rtpPacket = info.rtpPacket;
  if (typeof rtpPacket === 'object' && rtpPacket !== null) {
    const packetSsrc = (rtpPacket as { ssrc?: unknown }).ssrc;
    if (typeof packetSsrc === 'number' && Number.isInteger(packetSsrc)) {
      return packetSsrc;
    }
  }
  return undefined;
}

async function waitForConsumerSpatialLayer(
  consumer: MediasoupTypes.Consumer,
  transport: MediasoupTypes.WebRtcTransport,
  spatialLayer: number,
  transition: {
    setPreferredLayersStartedAt: number;
    setPreferredLayersCompletedAt: number;
    preferredLayersBefore: E2EConsumerLayersSnapshot;
    preferredLayersAfterSet: E2EConsumerLayersSnapshot;
    initialCurrentLayers: E2EConsumerLayersSnapshot;
  }
): Promise<E2ELayerSelectionDiagnostics> {
  const waitStartedAt = Date.now();
  const deadline = waitStartedAt + 10_000;
  const samples: E2ELayerSelectionSample[] = [];
  const observedCurrentSpatialLayerSequence: E2ELayerSelectionDiagnostics[
    'observedCurrentSpatialLayerSequence'
  ] = [];
  const availableOutgoingBitrateSamples: E2ELayerSelectionDiagnostics[
    'availableOutgoingBitrateSamples'
  ] = [];
  const layerChangeEvents = e2eLayerChangeSamples.get(consumer) ?? [];
  const layerChangeStartIndex = layerChangeEvents.length;
  let statsRequestPending = false;
  let samplerStopped = false;
  let sampleTimer: NodeJS.Timeout | undefined;

  const sampleLayerState = (): void => {
    if (samples.length >= MAX_E2E_LAYER_DIAGNOSTIC_SAMPLES) {
      return;
    }

    const timestamp = Date.now();
    const score = consumer.score;
    const sample: E2ELayerSelectionSample = {
      timestamp,
      sampleDurationMs: E2E_LAYER_DIAGNOSTIC_SAMPLE_INTERVAL_MS,
      preferredLayers: snapshotE2EConsumerLayers(consumer.preferredLayers),
      currentLayers: snapshotE2EConsumerLayers(consumer.currentLayers),
      priority: consumer.priority,
      score: {
        score: score.score,
        producerScore: score.producerScore,
        producerScores: [...score.producerScores]
      },
      paused: consumer.paused,
      producerPaused: consumer.producerPaused,
      consumerStats: null,
      consumerStatsError: null,
      transportStats: null,
      transportStatsError: null
    };
    samples.push(sample);
    observedCurrentSpatialLayerSequence.push({
      timestamp,
      spatialLayer: sample.currentLayers?.spatialLayer ?? null
    });

    if (statsRequestPending) {
      return;
    }
    statsRequestPending = true;
    void (async () => {
      try {
        const [consumerStatsResult, transportStatsResult] = await Promise.all([
          readE2EDiagnosticStats(() => consumer.getStats(), 'Consumer'),
          readE2EDiagnosticStats(() => transport.getStats(), 'WebRtcTransport')
        ]);

        if (consumerStatsResult.value) {
          sample.consumerStats = consumerStatsResult.value.map(stat => ({
            type: stat.type,
            ssrc: stat.ssrc,
            score: stat.score,
            packetCount: stat.packetCount,
            byteCount: stat.byteCount,
            bitrate: stat.bitrate
          }));
        }
        sample.consumerStatsError = consumerStatsResult.error;

        if (transportStatsResult.value) {
          sample.transportStats = transportStatsResult.value.map(stat => ({
            type: stat.type,
            availableOutgoingBitrate: stat.availableOutgoingBitrate ?? null,
            availableIncomingBitrate: stat.availableIncomingBitrate ?? null,
            maxOutgoingBitrate: stat.maxOutgoingBitrate ?? null,
            bytesSent: stat.bytesSent,
            sendBitrate: stat.sendBitrate,
            rtpBytesSent: stat.rtpBytesSent,
            rtpSendBitrate: stat.rtpSendBitrate,
            iceState: stat.iceState,
            dtlsState: stat.dtlsState,
            iceSelectedTuple: stat.iceSelectedTuple
              ? { ...stat.iceSelectedTuple }
              : null
          }));
        }
        sample.transportStatsError = transportStatsResult.error;
        availableOutgoingBitrateSamples.push({
          timestamp,
          availableOutgoingBitrate:
            transportStatsResult.value?.[0]?.availableOutgoingBitrate ?? null
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        sample.consumerStatsError ??= message;
        sample.transportStatsError ??= message;
      } finally {
        statsRequestPending = false;
      }
    })();
  };

  const scheduleSample = (): void => {
    if (samplerStopped || samples.length >= MAX_E2E_LAYER_DIAGNOSTIC_SAMPLES) {
      return;
    }
    sampleLayerState();
    sampleTimer = setTimeout(
      scheduleSample,
      E2E_LAYER_DIAGNOSTIC_SAMPLE_INTERVAL_MS
    );
  };

  scheduleSample();
  let transitionAt: number | null = null;
  while (Date.now() < deadline) {
    if (consumer.currentLayers?.spatialLayer === spatialLayer) {
      transitionAt = Date.now();
      break;
    }
    await new Promise(resolve => setTimeout(resolve, 50));
  }

  samplerStopped = true;
  if (sampleTimer) {
    clearTimeout(sampleTimer);
  }

  const finishedAt = transitionAt ?? Math.min(Date.now(), deadline);
  const finalScore = consumer.score;
  const diagnostics: E2ELayerSelectionDiagnostics = {
    stage: 'waitForConsumerSpatialLayer',
    result: transitionAt === null ? 'timed-out' : 'reached',
    requestKeyFrameReached: false,
    requestedSpatialLayer: spatialLayer,
    setPreferredLayersStartedAt: transition.setPreferredLayersStartedAt,
    setPreferredLayersCompletedAt: transition.setPreferredLayersCompletedAt,
    setPreferredLayersDurationMs:
      transition.setPreferredLayersCompletedAt -
      transition.setPreferredLayersStartedAt,
    waitStartedAt,
    waitDeadlineAt: deadline,
    waitElapsedMs: finishedAt - waitStartedAt,
    transitionElapsedMs:
      transitionAt === null
        ? null
        : transitionAt - transition.setPreferredLayersCompletedAt,
    targetReachedAt: transitionAt,
    preferredLayersBefore: transition.preferredLayersBefore,
    preferredLayersAfterSet: transition.preferredLayersAfterSet,
    initialCurrentLayers: transition.initialCurrentLayers,
    finalPreferredLayers: snapshotE2EConsumerLayers(consumer.preferredLayers),
    finalCurrentLayers: snapshotE2EConsumerLayers(consumer.currentLayers),
    finalPriority: consumer.priority,
    finalScore: {
      score: finalScore.score,
      producerScore: finalScore.producerScore,
      producerScores: [...finalScore.producerScores]
    },
    finalPaused: consumer.paused,
    finalProducerPaused: consumer.producerPaused,
    finalAvailableOutgoingBitrate:
      availableOutgoingBitrateSamples.at(-1)?.availableOutgoingBitrate ?? null,
    observedCurrentSpatialLayerSequence,
    layerschangeSamples: layerChangeEvents
      .slice(layerChangeStartIndex)
      .map(sample => ({ ...sample })),
    availableOutgoingBitrateSamples,
    samples
  };

  if (transitionAt === null) {
    throw new ConsumerSpatialLayerSelectionTimeout(diagnostics);
  }
  return diagnostics;
}

function snapshotE2EConsumerLayers(
  layers: MediasoupTypes.ConsumerLayers | undefined
): E2EConsumerLayersSnapshot {
  if (!layers) {
    return null;
  }
  return {
    spatialLayer: layers.spatialLayer,
    ...(layers.temporalLayer === undefined
      ? {}
      : { temporalLayer: layers.temporalLayer })
  };
}

function readE2EDiagnosticStats<T>(
  readStats: () => Promise<T>,
  label: string
): Promise<{ value: T | null; error: string | null }> {
  return new Promise(resolve => {
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      resolve({ value: null, error: `${label} getStats timed out after 150ms` });
    }, 150);

    try {
      readStats().then(
        value => {
          if (settled) {
            return;
          }
          settled = true;
          clearTimeout(timeout);
          resolve({ value, error: null });
        },
        error => {
          if (settled) {
            return;
          }
          settled = true;
          clearTimeout(timeout);
          resolve({
            value: null,
            error: error instanceof Error ? error.message : String(error)
          });
        }
      );
    } catch (error) {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      resolve({
        value: null,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });
}

function createProducerFeedbackAudit(): ProducerFeedbackAudit {
  const createLayerCounters = (): FeedbackLayerCounters => ({
    pliReceived: 0,
    firReceived: 0,
    keyframeRequestsForwarded: 0
  });
  return {
    pliReceived: 0,
    firReceived: 0,
    keyframeRequestsForwarded: 0,
    byLayer: {
      low: createLayerCounters(),
      mid: createLayerCounters(),
      high: createLayerCounters()
    },
    traces: []
  };
}

function snapshotProducerFeedbackAudit(): ProducerFeedbackAudit | undefined {
  if (!producerFeedbackAudit) {
    return undefined;
  }
  return {
    ...producerFeedbackAudit,
    byLayer: {
      low: { ...producerFeedbackAudit.byLayer.low },
      mid: { ...producerFeedbackAudit.byLayer.mid },
      high: { ...producerFeedbackAudit.byLayer.high }
    },
    traces: producerFeedbackAudit.traces.map(trace => ({ ...trace }))
  };
}

function handleEncodedChunk(peer: PeerState, raw: RawData): void {
  if (peer.role !== 'producer' || peer.id !== producerPeerId) {
    throw new Error('Only the active producer can send encoded chunks');
  }

  const packet = Buffer.isBuffer(raw) ? raw : Buffer.concat(raw as Buffer[]);

  if (packet.byteLength < 4) {
    throw new Error('Encoded chunk packet is too small');
  }

  const headerLength = packet.readUInt32BE(0);
  const headerEnd = 4 + headerLength;

  if (headerEnd > packet.byteLength) {
    throw new Error('Encoded chunk header is truncated');
  }

  const header = JSON.parse(
    packet.subarray(4, headerEnd).toString('utf8')
  ) as EncodedChunkHeader;
  const encoding = activeProducerEncodings.find(
    candidate => candidate.layer === header.layer
  );

  if (!encoding) {
    throw new Error(
      `Encoded chunk layer ${String(header.layer)} is not active for the current Producer mode`
    );
  }

  const data = packet.subarray(headerEnd);
  const descriptionBase64 =
    header.metadata?.decoderConfig?.descriptionBase64;

  injectEncodedChunk(
    header.duration === undefined
      ? {
        data,
        timestamp: header.timestamp,
        type: header.type
      }
      : {
        data,
        timestamp: header.timestamp,
        type: header.type,
        duration: header.duration
      },
    encoding.ssrc,
    descriptionBase64
      ? {
        decoderConfig: {
          description: Buffer.from(descriptionBase64, 'base64')
        }
      }
      : undefined
  );

  injectedChunks++;
  recordAcceptedInputFrame(header);

  if (injectedChunks % 3000 === 0 || header.type === 'key') {
    // pipelineStats 진단 이벤트는 debug 전용. production에서는
    // getPipelineStats()/객체 생성/JSON.stringify/추가 WS 전송을 모두 생략.
    // roomState/health에 필요한 통계와 broadcastRoomState()는 그대로 유지.
    if (IS_DEBUG) {
      const pipelineStats = getPipelineStats();
      peer.socket.send(
        JSON.stringify({
          event: 'pipelineStats',
          data: {
            injectedChunks,
            injectedPackets: pipelineStats.rtpPackets,
            lastChunkType: header.type,
            consumerCount: countConsumers()
          }
        })
      );
    }
    broadcastRoomState();
  }
}

// A closed signaling socket does not end an established Consumer's media session.
function checkConsumerAfterSignalingClose(peer: PeerState): void {
  if (shuttingDown || peer.role !== 'consumer' ||
      peer.socket.readyState !== 3 || !peers.has(peer.id)) {
    return;
  }

  const transport = peer.transport;
  if (!peer.consumer || peer.consumer.closed || peer.consumer.paused ||
      !transport || transport.closed ||
      transport.dtlsState === 'failed' || transport.dtlsState === 'closed') {
    cleanupPeer(peer);
    return;
  }

  if (transport.dtlsState === 'connected' &&
      (transport.iceState === 'connected' || transport.iceState === 'completed')) {
    clearTimeout(peer.consumerCleanupTimer);
    delete peer.consumerCleanupTimer;
    return;
  }

  if (!peer.consumerCleanupTimer) {
    peer.consumerCleanupTimer = setTimeout(() => {
      delete peer.consumerCleanupTimer;
      if (!shuttingDown && peer.socket.readyState === 3 &&
          (transport.dtlsState !== 'connected' ||
           (transport.iceState !== 'connected' && transport.iceState !== 'completed'))) {
        cleanupPeer(peer);
      }
    }, CONSUMER_DISCONNECT_GRACE_MS);
  }
}

function cleanupPeer(peer: PeerState): void {
  // Remove first: closing media emits observer events that may re-enter cleanup.
  if (!peers.delete(peer.id)) {
    return;
  }
  clearTimeout(peer.consumerCleanupTimer);
  delete peer.consumerCleanupTimer;
  stopStatsPump(peer);
  peer.consumer?.close();
  peer.transport?.close();

  if (peer.id === producerPeerId) {
    if (IS_DEBUG) {
      logger.debug(`[room] producer ${peer.id} disconnected`);
    }
    producer?.close();
    producer = undefined;
    producerPeerId = undefined;
    producerSimulcastEnabled = undefined;
    activeProducerEncodings = [];
    pendingE2EKeyframeRequests.clear();
    resetInputFps();

    for (const other of peers.values()) {
      stopStatsPump(other);
      other.consumer?.close();
      delete other.consumer;
      delete other.e2eConsumerCreatedAt;
      if (other.socket.readyState === 1) {
        other.socket.send(JSON.stringify({ event: 'producerClosed' }));
      }
    }
  }

  logPeerCount('leave', peer);
  broadcastRoomState();
}

function recordAcceptedInputFrame(header: EncodedChunkHeader): void {
  const now = performance.now();

  if (inputFpsWindowStartedAt === undefined) {
    inputFpsWindowStartedAt = now;
  }

  if (
    lastInputSourceTimestamp === undefined ||
    header.timestamp > lastInputSourceTimestamp
  ) {
    inputFpsSourceFrames++;
    lastInputSourceTimestamp = header.timestamp;
  }

  if (header.layer === 'low') {
    inputFpsLowFrames++;
  } else if (header.layer === 'mid') {
    inputFpsMidFrames++;
  } else {
    inputFpsHighFrames++;
  }

  const elapsedMs = now - inputFpsWindowStartedAt;

  if (elapsedMs < 1000) {
    return;
  }

  const elapsedSeconds = elapsedMs / 1000;
  latestInputFps = {
    source: inputFpsSourceFrames / elapsedSeconds,
    low: inputFpsLowFrames / elapsedSeconds,
    mid: inputFpsMidFrames / elapsedSeconds,
    high: inputFpsHighFrames / elapsedSeconds,
    windowMs: elapsedMs
  };
  broadcastInputFps();

  inputFpsWindowStartedAt = now;
  inputFpsSourceFrames = 0;
  inputFpsLowFrames = 0;
  inputFpsMidFrames = 0;
  inputFpsHighFrames = 0;
}

function resetInputFps(): void {
  inputFpsWindowStartedAt = undefined;
  lastInputSourceTimestamp = undefined;
  inputFpsSourceFrames = 0;
  inputFpsLowFrames = 0;
  inputFpsMidFrames = 0;
  inputFpsHighFrames = 0;
  latestInputFps = createEmptyInputFps();
  broadcastInputFps();
}

function createEmptyInputFps(): InputFpsSnapshot {
  return {
    source: 0,
    low: 0,
    mid: 0,
    high: 0,
    windowMs: 0
  };
}

function broadcastInputFps(): void {
  for (const peer of peers.values()) {
    if (peer.socket.readyState !== 1 || (!IS_DEBUG && peer.role !== 'producer')) {
      continue;
    }

    peer.socket.send(JSON.stringify({ event: 'inputFps', data: latestInputFps }));
  }
}

function assertConsumerPeer(peer: PeerState): void {
  if (peer.role !== 'consumer') {
    throw new Error('This action is only available to consumer peers');
  }
}

function startStatsPump(socket: WebSocket, peer: PeerState): void {
  stopStatsPump(peer);
  if (socket.readyState !== 1 || !peers.has(peer.id)) {
    return;
  }

  peer.statsTimer = setInterval(async () => {
    try {
      const [producerStats, consumerStats, transportStats] = await Promise.all([
        producer?.getStats(),
        peer.consumer?.getStats(),
        peer.transport?.getStats()
      ]);

      if (socket.readyState !== 1) {
        return;
      }
      socket.send(
        JSON.stringify({
          event: 'serverStats',
          data: {
            producer: producerStats,
            consumer: consumerStats,
            transport: transportStats
          }
        })
      );
    } catch (error) {
      logger.error('[stats] failed', error);
    }
  }, 100000);
}

function stopStatsPump(peer: PeerState): void {
  if (peer.statsTimer) {
    clearInterval(peer.statsTimer);
    delete peer.statsTimer;
  }
}

function broadcastRoomState(): void {
  const data = {
    producerPeerId,
    producerId: producer?.id,
    consumerCount: countConsumers(),
    injectedChunks,
    injectedPackets: getPipelineStats().rtpPackets
  };

  for (const peer of peers.values()) {
    if (peer.socket.readyState === 1) {
      peer.socket.send(JSON.stringify({ event: 'roomState', data }));
    }
  }
}

function countConsumers(): number {
  return [...peers.values()].filter(peer => peer.role === 'consumer').length;
}

function snapshotE2EConsumerAudit(): Array<{
  peerId: string;
  transportId: string | null;
  consumerId: string | null;
  createdAt: number | null;
  preferredLayers: E2EConsumerLayersSnapshot;
  currentLayers: E2EConsumerLayersSnapshot;
}> {
  return [...peers.values()]
    .filter(peer => peer.role === 'consumer')
    .map(peer => ({
      peerId: peer.id,
      transportId: peer.transport?.id ?? null,
      consumerId: peer.consumer?.id ?? null,
      createdAt: peer.e2eConsumerCreatedAt ?? null,
      preferredLayers: snapshotE2EConsumerLayers(peer.consumer?.preferredLayers),
      currentLayers: snapshotE2EConsumerLayers(peer.consumer?.currentLayers)
    }));
}

function countProducers(): number {
  return [...peers.values()].filter(peer => peer.role === 'producer').length;
}

function logPeerCount(event: 'join' | 'leave', peer: PeerState): void {
  // debug 전용 계산(peers 순회·count·문자열 생성)을 production에서 아예 수행하지 않음.
  // countConsumers()/countProducers()의 실제 기능 사용처(roomState/health)는 그대로 유지.
  if (!IS_DEBUG) {
    return;
  }

  logger.debug(
    `[room] ${event}: ${peer.id} (${peer.role}) | total=${peers.size}, producers=${countProducers()}, consumers=${countConsumers()}`
  );
}

function reply(socket: WebSocket, id: string, data: unknown): void {
  socket.send(JSON.stringify({ id, ok: true, data }));
}

function sendError(socket: WebSocket, error: unknown, id?: string): void {
  const message = error instanceof Error ? error.message : String(error);
  const spatialLayerDiagnostics: E2ELayerSelectionFailureDiagnostics | undefined =
    SFU_E2E_MEDIA_ENABLED &&
    (error instanceof ConsumerSpatialLayerSelectionTimeout ||
      error instanceof ConsumerPreferredLayersSelectionFailure)
      ? error.diagnostics
      : undefined;

  socket.send(
    JSON.stringify({
      id,
      ok: false,
      error: message,
      ...(spatialLayerDiagnostics ? { spatialLayerDiagnostics } : {})
    })
  );
  logger.error(message);
}
