import { Device } from 'mediasoup-client';
import type { types as MediasoupClientTypes } from 'mediasoup-client';
import { IS_DEBUG, debugConsole } from './clientLogger.js';
import {
  downloadStatsDump,
  MAX_STATS_SAMPLES,
  readBrowserJsHeapUsedBytes,
  StatsDumpSession
} from './stats/statsDump.js';
import type {
  ConsumerStatsSample,
  ProducerLayerStatsSample,
  ProducerStatsSample
} from './stats/statsDump.js';
import {
  canApplyConsumerStatsResult,
  calculateConsumerStats,
  readVideoInboundStats,
  type ConsumerStatsSnapshot
} from './stats/consumerStats.js';
import './styles.css';

type PeerRole = 'producer' | 'consumer';

type RoleAssignment = {
  peerId: string;
  role: PeerRole;
  consumerCount: number;
  simulcastEnabled?: boolean;
  availableLayers?: SimulcastLayer[];
  ssrc?: number;
  ssrcs?: number[];
};

type ResponseMessage = {
  id?: string;
  ok?: boolean;
  data?: unknown;
  error?: string;
  event?: string;
};

type PendingRequest = {
  resolve: (data: unknown) => void;
  reject: (error: Error) => void;
};

type BrowserChunkType = 'key' | 'delta';
type KeyframeRequestReason = 'pli' | 'fir';

type PendingKeyframeRequestItem = {
  requestId: string;
  reason: KeyframeRequestReason;
  requestedAt: number;
};

type PendingKeyframeRequest = {
  requests: PendingKeyframeRequestItem[];
};

type RequestedKeyframeEncodingAudit = {
  requests: PendingKeyframeRequestItem[];
  encodeRequestedAt: number;
  periodicKeyframe: boolean;
  resizeKeyframe: boolean;
  keyframeOption: boolean;
};

class RequestedKeyframeEncodeRetryError extends Error {
  constructor(layer: SimulcastLayer, cause: unknown) {
    const causeMessage = cause instanceof Error ? cause.message : String(cause);
    super(`Requested keyframe encode failed for ${layer}; retrying next frame: ${causeMessage}`);
    this.name = 'RequestedKeyframeEncodeRetryError';
  }
}

const MAX_PENDING_KEYFRAME_AUDIT_REQUESTS = 16;

const SIMULCAST_LAYERS = [
  {
    id: 'low',
    scaleDownBy: 4,
    bitrate: 500_000,
    spatialLayer: 0
  },
  {
    id: 'mid',
    scaleDownBy: 2,
    bitrate: 1_000_000,
    spatialLayer: 1
  },
  {
    id: 'high',
    scaleDownBy: 1,
    bitrate: 5_000_000,
    spatialLayer: 2
  }
] as const;

type SimulcastLayer = (typeof SIMULCAST_LAYERS)[number]['id'];

type LayerEncoderState = {
  id: SimulcastLayer;
  scaleDownBy: number;
  bitrate: number;
  width: number;
  height: number;
  canvas: OffscreenCanvas;
  context: OffscreenCanvasRenderingContext2D;
  encoder: VideoEncoder;
  config: VideoEncoderConfig;
  requestedKeyframesByTimestamp: Map<number, RequestedKeyframeEncodingAudit>;
};

const startButton = document.querySelector<HTMLButtonElement>('#startButton')!;
const dumpStatsButton = document.querySelector<HTMLButtonElement>('#dumpStatsButton')!;
const measurementStatus = document.querySelector<HTMLElement>('#measurementStatus')!;
const producerSettings = document.querySelector<HTMLElement>('#producerSettings')!;
const simulcastToggle = document.querySelector<HTMLInputElement>('#simulcastToggle')!;
const simulcastState = document.querySelector<HTMLElement>('#simulcastState')!;
const consumerLayerControls = document.querySelector<HTMLElement>('#consumerLayerControls')!;
const consumerLayerStatus = document.querySelector<HTMLOutputElement>('#consumerLayerStatus')!;
const consumerLayerButtons: Record<SimulcastLayer, HTMLButtonElement> = {
  low: document.querySelector<HTMLButtonElement>('#consumerLayerLow')!,
  mid: document.querySelector<HTMLButtonElement>('#consumerLayerMid')!,
  high: document.querySelector<HTMLButtonElement>('#consumerLayerHigh')!
};
const localVideo = document.querySelector<HTMLVideoElement>('#localVideo')!;
const remoteVideo = document.querySelector<HTMLVideoElement>('#remoteVideo')!;
const localCaption = document.querySelector<HTMLElement>('#localCaption')!;
const remoteCaption = document.querySelector<HTMLElement>('#remoteCaption')!;
const socketState = document.querySelector<HTMLElement>('#socketState')!;
const roleState = document.querySelector<HTMLElement>('#roleState')!;
const peerIdLabel = document.querySelector<HTMLElement>('#peerId')!;
const consumerCount = document.querySelector<HTMLElement>('#consumerCount')!;
const chunkCount = document.querySelector<HTMLElement>('#chunkCount')!;
const packetCount = document.querySelector<HTMLElement>('#packetCount')!;
const consumerState = document.querySelector<HTMLElement>('#consumerState')!;
const rtpReceived = document.querySelector<HTMLElement>('#rtpReceived')!;
const framesDecoded = document.querySelector<HTMLElement>('#framesDecoded')!;
const sfuInputFps = document.querySelector<HTMLElement>('#sfuInputFps')!;
const lowInputFps = document.querySelector<HTMLElement>('#lowInputFps')!;
const midInputFps = document.querySelector<HTMLElement>('#midInputFps')!;
const highInputFps = document.querySelector<HTMLElement>('#highInputFps')!;
const receivedFps = document.querySelector<HTMLElement>('#receivedFps')!;
const decodedFps = document.querySelector<HTMLElement>('#decodedFps')!;
const logBox = document.querySelector<HTMLPreElement>('#log')!;

// 프로덕션 모드에서는 #log 박스 숨김, 개발 디버깅 모드에서만 표시.
// (index.html에서 hidden으로 시작 -> 디버그일 때만 드러냄, FOUC 방지)
logBox.hidden = !IS_DEBUG;

let requestSeq = 0;
let encodedChunks = 0;
let socket: WebSocket;
let role: PeerRole | undefined;
let roleAssignment: RoleAssignment | undefined;
let activeSimulcastEnabled = true;
let availableLayers: readonly SimulcastLayer[] = SIMULCAST_LAYERS.map(layer => layer.id);
let selectedConsumerLayer: SimulcastLayer | undefined;
const pending = new Map<string, PendingRequest>();
const pendingKeyframeRequests = new Map<SimulcastLayer, PendingKeyframeRequest>();

type ProducerLayerCounters = {
  chunks: number;
  bytes: number;
  keyFrames: number;
  deltaFrames: number;
};

let statsDumpSession: StatsDumpSession | undefined;
let producerStatsTimer: number | undefined;
let consumerStatsPollingStop: (() => void) | undefined;
let sourceFramesTotal = 0;
const producerLayerCounters = new Map<SimulcastLayer, ProducerLayerCounters>();

startButton.addEventListener('click', () => {
  startButton.disabled = true;
  simulcastToggle.disabled = true;
  dumpStatsButton.disabled = true;
  start().catch(error => {
    startButton.disabled = false;
    if (!role) {
      simulcastToggle.disabled = false;
      producerSettings.hidden = false;
    }
    logError(`failed: ${error instanceof Error ? error.message : String(error)}`);
  });
});

dumpStatsButton.addEventListener('click', () => {
  if (!statsDumpSession) {
    return;
  }

  const filename = downloadStatsDump(statsDumpSession);
  if (
    statsDumpSession.metadata.role === 'producer' &&
    statsDumpSession.collectionStatus !== 'recording'
  ) {
    clearProducerStatsTimer();
  }
  updateStatsDumpStatus(statsDumpSession);
  writeLog(`dump exported: ${filename}`);
});

for (const [layer, button] of Object.entries(consumerLayerButtons) as Array<[
  SimulcastLayer,
  HTMLButtonElement
]>) {
  button.addEventListener('click', () => {
    void selectConsumerLayer(layer);
  });
}

async function start(): Promise<void> {
  const requestedSimulcastEnabled = simulcastToggle.checked;
  socket = await connectSocket(requestedSimulcastEnabled); // 웹소켓 서버에 연결
  socketState.textContent = 'connected';

  try {
    const assignment = await waitForRoleAssignment(); // 역할 할당 대기
    role = assignment.role;
    applyRoleAssignment(assignment);
    roleState.textContent = role;
    peerIdLabel.textContent = assignment.peerId;

    // 역할 구분
    if (role === 'producer') {
      await startProducer(); // Producer 역할 시작. 카메라, 인코더 시작
    } else {
      await startConsumer(); // Consumer 역할 시작. WebRTC 수신 연결을 만든다.
    }
    dumpStatsButton.disabled = statsDumpSession === undefined;
  } catch (error) {
    if (socket.readyState !== WebSocket.CLOSED) {
      await new Promise<void>(resolve => {
        socket.addEventListener('close', () => resolve(), { once: true });

        if (socket.readyState === WebSocket.CLOSED) {
          resolve();
          return;
        }

        socket.close();
      });
    }

    role = undefined;
    throw error;
  }
}

async function startProducer(): Promise<void> {
  assertWebCodecsSupport();
  pendingKeyframeRequests.clear();

  remoteVideo.removeAttribute('src');
  remoteVideo.srcObject = null;
  remoteCaption.textContent = 'Consumers receive this stream';
  localCaption.textContent = 'Producer screen preview';
  consumerState.textContent = 'producer';

  const stream = await navigator.mediaDevices.getDisplayMedia({ // 화면 공유 열기
    video: {
      width: { ideal: 1280 },
      height: { ideal: 720 },
      frameRate: { ideal: 30, max: 30 }
    },
    audio: false
  });
  stream.getVideoTracks()[0]?.addEventListener(
    'ended',
    () => {
      stopActiveStatsCollection();
      if (socket.readyState !== WebSocket.CLOSED) {
        socket.close();
      }
    },
    { once: true }
  );

  localVideo.srcObject = stream; // 비디오 요소를 열린 화면 공유 스트림으로 연결한다.
  try {
    await startEncoder(stream, activeSimulcastEnabled); // VideoEncoder를 시작한다.
  } catch (error) {
    stream.getTracks().forEach(captureTrack => captureTrack.stop());
    throw error;
  }
  debugLog('producer pipeline started');
}

async function startConsumer(): Promise<void> {
  localVideo.removeAttribute('src');
  localVideo.srcObject = null;
  localCaption.textContent = 'Consumer peer';
  remoteCaption.textContent = 'Producer stream';

  await setupConsumer(); // WebRTC 수신 연결을 만든다.
  debugLog('consumer connected');
}

async function connectSocket(simulcastEnabled: boolean): Promise<WebSocket> {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(
    `${protocol}//${window.location.host}/ws?simulcast=${simulcastEnabled ? '1' : '0'}`
  );

  ws.binaryType = 'arraybuffer';

  ws.addEventListener('message', event => {
    if (typeof event.data !== 'string') {
      return;
    }

    const message = JSON.parse(event.data) as ResponseMessage;

    if (message.id) {
      const entry = pending.get(message.id);

      if (!entry) {
        return;
      }

      pending.delete(message.id);

      if (message.ok) {
        entry.resolve(message.data);
      } else {
        entry.reject(new Error(message.error ?? 'request failed'));
      }

      return;
    }

    handleEvent(message);
  });

  ws.addEventListener('close', () => {
    socketState.textContent = 'closed';
    stopActiveStatsCollection();
  });

  ws.addEventListener('error', () => {
    socketState.textContent = 'error';
  });

  await new Promise<void>((resolve, reject) => {
    ws.addEventListener('open', () => resolve(), { once: true });
    ws.addEventListener('error', () => reject(new Error('WebSocket failed')), {
      once: true
    });
  });

  return ws;
}

function handleEvent(message: ResponseMessage): void {
  if (message.event === 'roleAssigned') {
    const assignment = message.data as RoleAssignment;

    roleAssignment = assignment;
    role = assignment.role;
    applyRoleAssignment(assignment);
    roleState.textContent = assignment.role;
    peerIdLabel.textContent = assignment.peerId;
    consumerCount.textContent = String(assignment.consumerCount);
    debugLog(`assigned as ${assignment.role}`);
    return;
  }

  if (message.event === 'keyframeRequest') {
    const data = message.data as {
      requestId?: unknown;
      layer?: unknown;
      reason?: unknown;
      requestedAt?: unknown;
    } | undefined;
    if (
      role !== 'producer' ||
      !data ||
      !isSimulcastLayer(data.layer) ||
      (data.reason !== 'pli' && data.reason !== 'fir') ||
      typeof data.requestId !== 'string'
    ) {
      debugLog('ignored invalid keyframe request');
      return;
    }

    const request: PendingKeyframeRequestItem = {
      requestId: data.requestId,
      reason: data.reason,
      requestedAt:
        typeof data.requestedAt === 'number' && Number.isFinite(data.requestedAt)
          ? data.requestedAt
          : Date.now()
    };
    const priorRequests = pendingKeyframeRequests.get(data.layer)?.requests ?? [];
    pendingKeyframeRequests.set(data.layer, {
      requests: [...priorRequests, request].slice(
        -MAX_PENDING_KEYFRAME_AUDIT_REQUESTS
      )
    });
    if (IS_DEBUG) {
      debugLog(`received ${request.reason.toUpperCase()} keyframe request for ${data.layer}`);
    }
    return;
  }

  if (message.event === 'roomState') {
    const state = message.data as {
      consumerCount: number;
      injectedChunks: number;
      injectedPackets: number;
    };

    consumerCount.textContent = String(state.consumerCount);
    packetCount.textContent = String(state.injectedPackets);

    if (role === 'consumer') {
      chunkCount.textContent = String(state.injectedChunks);
    }
    return;
  }

  if (message.event === 'inputFps') {
    const inputFps = message.data as {
      source?: number;
      low?: number;
      mid?: number;
      high?: number;
    };

    sfuInputFps.textContent = formatFps(inputFps.source);
    lowInputFps.textContent = formatFps(inputFps.low);
    midInputFps.textContent = formatFps(inputFps.mid);
    highInputFps.textContent = formatFps(inputFps.high);
    return;
  }

  if (message.event === 'pipelineStats') {
    const stats = message.data as {
      injectedChunks: number;
      injectedPackets: number;
      lastChunkType: string;
      consumerCount: number;
    };

    packetCount.textContent = String(stats.injectedPackets);
    consumerCount.textContent = String(stats.consumerCount);
    debugLog(
      `server injected ${stats.injectedChunks} chunks, last=${stats.lastChunkType}`
    );
    return;
  }

  if (message.event === 'transportState') {
    debugLog(`transport ${JSON.stringify(message.data)}`);
    return;
  }

  if (message.event === 'serverStats') {
    debugLog(summarizeServerStats(message.data));
    return;
  }

  if (message.event === 'producerClosed') {
    consumerState.textContent = 'producer closed';
    resetConsumerFps();
    stopActiveStatsCollection();
    debugLog('producer disconnected; refresh to rejoin the room');
  }
}

async function waitForRoleAssignment(): Promise<RoleAssignment> {
  for (let attempt = 0; attempt < 50; attempt++) {
    if (role && roleAssignment) {
      return roleAssignment;
    }

    await new Promise(resolve => setTimeout(resolve, 100));
  }

  throw new Error('Timed out waiting for role assignment');
}

async function setupConsumer(): Promise<void> {
  consumerState.textContent = 'loading';

  // 서버 규격 확인 및 디바이스 준비
  /*
  서버(라우터)가 어떤 비디오/오디오 코덱을 지원하는지(RtpCapabilities)를 서버에 요청해서 알아냅니다. 그리고 Device 객체를 생성해 해당 규격으로 통신할 수 있도록 클라이언트 환경을 준비합니다.
  */
  const routerRtpCapabilities =
    await request<MediasoupClientTypes.RtpCapabilities>(
      'getRouterRtpCapabilities'
    );
  const device = new Device();

  await device.load({ routerRtpCapabilities });
  // ------------------------------------

  // 수신용 Transport 생성
  /*
  스트림을 받아오기 위한 수신용 WebRTC 통로(Transport) 생성에 필요한 옵션들을
  서버에서 받아와 클라이언트에 수신용 Transport를 만듭니다.
  */
  const transportOptions =
    await request<MediasoupClientTypes.TransportOptions>(
      'createConsumerTransport'
    );
  const recvTransport = device.createRecvTransport(transportOptions);

  recvTransport.on('connectionstatechange', state => {
    debugLog(`recv transport ${state}`);
    if (state === 'closed' || state === 'failed') {
      stopActiveStatsCollection();
    }
  });
  // ------------------------------------

  // 암호화 연결 설정
  /* Transport가 실제로 통신을 시작할 때, 보안 통신을 위한 DTLS 정보를 서버에 전달하여 양단 간의 WebRTC 연결을 맺습니다. */
  recvTransport.on('connect', ({ dtlsParameters }, callback, errback) => {
    request('connectConsumerTransport', { dtlsParameters })
      .then(() => callback())
      .catch(error => errback(error as Error));
  });

  // 미디어 트랙 받아오기
  /* 서버에게 "다른 사람(Producer)이 보내는 미디어를 내가 받겠다"고 요청(consume)합니다. 승인이 떨어지면 recvTransport.consume()을 통해 실제 미디어 트랙(consumer.track)을 받아오고, 이를 브라우저에서 재생 가능한 MediaStream 형태로 변환합니다. */
  const consumerOptions = await request<{
    id: string;
    producerId: string;
    kind: MediasoupClientTypes.MediaKind;
    rtpParameters: MediasoupClientTypes.RtpParameters;
  }>('consume', { rtpCapabilities: device.rtpCapabilities });

  const consumer = await recvTransport.consume(consumerOptions);
  const remoteStream = new MediaStream([consumer.track]);
  let consumerTrackEnded = consumer.track.readyState === 'ended';

  consumer.track.addEventListener('ended', () => {
    consumerTrackEnded = true;
    stopActiveStatsCollection();
  }, { once: true });

  consumer.track.addEventListener('unmute', () => {
    debugLog('consumer track unmuted');
    void remoteVideo.play().catch(error => {
      logError(`remote video play failed: ${error.message}`);
    });
  });
  consumer.track.addEventListener('mute', () => {
    debugLog('consumer track muted');
  });

  // 화면(비디오 태그)에 출력 및 재생
  /* 받아온 remoteStream을 HTML의 <video> 요소(remoteVideo)에 연결하여 화면에 띄웁니다. 마지막으로 서버에 일시정지(pause)되어 있던 미디어 스트림 전송을 시작하라고 요청(resumeConsumer)한 뒤 비디오 재생을 시작합니다. */

  remoteVideo.srcObject = remoteStream;
  remoteVideo.muted = true;
  remoteVideo.playsInline = true;

  await request('resumeConsumer');
  void remoteVideo.play().catch(error => {
    logError(`remote video play failed: ${error.message}`);
  });
  beginStatsDumpSession('consumer');
  pollConsumerStats(recvTransport, statsDumpSession!);
  if (consumerTrackEnded || consumer.track.readyState === 'ended') {
    stopActiveStatsCollection();
  }

  consumerState.textContent = 'receiving';
}

function applyRoleAssignment(assignment: RoleAssignment): void {
  activeSimulcastEnabled = assignment.simulcastEnabled !== false;
  availableLayers = assignment.availableLayers?.filter(isSimulcastLayer) ??
    (activeSimulcastEnabled ? SIMULCAST_LAYERS.map(layer => layer.id) : ['high']);
  simulcastState.textContent = activeSimulcastEnabled ? 'ON' : 'OFF';
  simulcastToggle.checked = activeSimulcastEnabled;
  simulcastToggle.disabled = true;

  if (assignment.role === 'producer') {
    producerSettings.hidden = false;
    consumerLayerControls.hidden = true;
    resetConsumerLayerSelection();
    return;
  }

  producerSettings.hidden = true;
  consumerLayerControls.hidden = !activeSimulcastEnabled;
  for (const [layer, button] of Object.entries(consumerLayerButtons) as Array<[
    SimulcastLayer,
    HTMLButtonElement
  ]>) {
    button.disabled = !activeSimulcastEnabled || !availableLayers.includes(layer);
  }
  if (!activeSimulcastEnabled) {
    consumerLayerStatus.value = '단일 화질';
    selectedConsumerLayer = undefined;
  }
}

function resetConsumerLayerSelection(): void {
  selectedConsumerLayer = undefined;
  consumerLayerStatus.value = '자동';
  for (const button of Object.values(consumerLayerButtons)) {
    button.setAttribute('aria-pressed', 'false');
  }
}

async function selectConsumerLayer(layer: SimulcastLayer): Promise<void> {
  if (
    role !== 'consumer' ||
    !activeSimulcastEnabled ||
    !availableLayers.includes(layer)
  ) {
    return;
  }

  for (const button of Object.values(consumerLayerButtons)) {
    button.disabled = true;
  }
  try {
    const response = await request<{
      layer: SimulcastLayer;
      spatialLayer: number;
      preferredLayers?: { spatialLayer?: number } | null;
    }>('setConsumerLayer', { layer });
    selectedConsumerLayer = response.layer;
    for (const [candidate, button] of Object.entries(consumerLayerButtons) as Array<[
      SimulcastLayer,
      HTMLButtonElement
    ]>) {
      button.setAttribute(
        'aria-pressed',
        candidate === selectedConsumerLayer ? 'true' : 'false'
      );
    }
    consumerLayerStatus.value = `선호 화질: ${response.layer.toUpperCase()}`;
  } catch (error) {
    logError(`preferred layer failed: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    for (const [candidate, button] of Object.entries(consumerLayerButtons) as Array<[
      SimulcastLayer,
      HTMLButtonElement
    ]>) {
      button.disabled = !activeSimulcastEnabled || !availableLayers.includes(candidate);
    }
  }
}

function pollConsumerStats(
  recvTransport: MediasoupClientTypes.Transport,
  session: StatsDumpSession
): void {
  let previousStats: ConsumerStatsSnapshot = new Map();
  let pollInFlight = false;
  let stopped = false;

  const recordUnavailableSample = (): void => {
    previousStats = new Map();
    resetConsumerFps();
    session.appendConsumerSample({
      timestamp: new Date().toISOString(),
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
      browserJsHeapUsedBytes: readBrowserJsHeapUsedBytes()
    });
    updateStatsDumpStatus(session);
  };

  const intervalId = window.setInterval(() => {
    if (!canApplyConsumerStatsResult(stopped, statsDumpSession === session) || pollInFlight) {
      return;
    }

    pollInFlight = true;
    void recvTransport.getStats()
      .then(report => {
        if (!canApplyConsumerStatsResult(stopped, statsDumpSession === session)) {
          return;
        }
        const inboundVideoStats = readVideoInboundStats(report.values());
        if (inboundVideoStats.length === 0) {
          recordUnavailableSample();
          return;
        }

        const calculated = calculateConsumerStats(inboundVideoStats, previousStats);
        previousStats = calculated.nextSnapshot;
        rtpReceived.textContent = formatCounter(calculated.packetsReceived);
        framesDecoded.textContent = formatCounter(calculated.framesDecoded);
        receivedFps.textContent = formatFps(calculated.receivedFps);
        decodedFps.textContent = formatFps(calculated.decodedFps);

        if (session.metadata.role === 'consumer') {
          const consumerSample: ConsumerStatsSample = {
            timestamp: new Date().toISOString(),
            elapsedMs: calculated.elapsedMs,
            packetsReceived: calculated.packetsReceived,
            packetsLost: calculated.packetsLost,
            maxJitterMs: calculated.maxJitterMs,
            framesReceived: calculated.framesReceived,
            framesDecoded: calculated.framesDecoded,
            framesDropped: calculated.framesDropped,
            bytesReceived: calculated.bytesReceived,
            receivedFps: calculated.receivedFps,
            decodedFps: calculated.decodedFps,
            receivedMediaBitrateKbps: calculated.receivedMediaBitrateKbps,
            browserJsHeapUsedBytes: readBrowserJsHeapUsedBytes()
          };
          session.appendConsumerSample(consumerSample);
          updateStatsDumpStatus(session);
        }

      })
      .catch(() => {
        if (canApplyConsumerStatsResult(stopped, statsDumpSession === session)) {
          recordUnavailableSample();
        }
      })
      .finally(() => {
        pollInFlight = false;
      });
  }, 1000);

  const stopPolling = (): void => {
    if (stopped) {
      return;
    }
    stopped = true;
    window.clearInterval(intervalId);
    resetConsumerFps();
    if (consumerStatsPollingStop === stopPolling) {
      consumerStatsPollingStop = undefined;
    }
  };
  consumerStatsPollingStop = stopPolling;
}

function formatCounter(value: number | null): string {
  return value === null || !Number.isFinite(value) ? 'n/a' : String(value);
}

function formatFps(value: number | null | undefined): string {
  return value === null || value === undefined || !Number.isFinite(value) || value < 0
    ? 'n/a'
    : value.toFixed(1);
}

function resetConsumerFps(): void {
  receivedFps.textContent = 'n/a';
  decodedFps.textContent = 'n/a';
}

function beginStatsDumpSession(sessionRole: PeerRole): StatsDumpSession {
  const session = new StatsDumpSession({
    peerId: roleAssignment?.peerId ?? peerIdLabel.textContent ?? 'unknown',
    role: sessionRole,
    measurementStartTime: new Date().toISOString(),
    sampleIntervalMs: 1000,
    userAgent: navigator.userAgent,
    simulcastEnabled: activeSimulcastEnabled,
    availableLayers: [...availableLayers]
  });
  statsDumpSession = session;
  updateStatsDumpStatus(session);
  return session;
}

function updateStatsDumpStatus(session: StatsDumpSession): void {
  if (statsDumpSession !== session) {
    return;
  }

  if (session.collectionStatus === 'limit-reached') {
    measurementStatus.textContent =
      'Recording stopped: sample/time limit reached. Download the collected statistics.';
  } else if (session.collectionStatus === 'stopped') {
    measurementStatus.textContent =
      `Recording stopped · ${session.sampleCount.toLocaleString()} samples retained. Download remains available.`;
  } else {
    measurementStatus.textContent =
      `Recording · Samples: ${session.sampleCount.toLocaleString()} / ${MAX_STATS_SAMPLES.toLocaleString()}`;
  }
}

function clearProducerStatsTimer(): void {
  if (producerStatsTimer !== undefined) {
    window.clearInterval(producerStatsTimer);
    producerStatsTimer = undefined;
  }
}

function stopActiveStatsCollection(): void {
  clearProducerStatsTimer();
  consumerStatsPollingStop?.();
  const session = statsDumpSession;
  if (!session) {
    return;
  }
  session.stop();
  updateStatsDumpStatus(session);
}

function startProducerStatsSampling(layers: readonly LayerEncoderState[]): void {
  const session = beginStatsDumpSession('producer');
  sourceFramesTotal = 0;
  producerLayerCounters.clear();
  for (const layer of layers) {
    producerLayerCounters.set(layer.id, {
      chunks: 0,
      bytes: 0,
      keyFrames: 0,
      deltaFrames: 0
    });
  }

  let previousSampleAt = performance.now();
  let previousSourceFrames = sourceFramesTotal;
  const previousLayers = new Map<SimulcastLayer, ProducerLayerCounters>();
  for (const layer of layers) {
    previousLayers.set(layer.id, {
      chunks: 0,
      bytes: 0,
      keyFrames: 0,
      deltaFrames: 0
    });
  }

  producerStatsTimer = window.setInterval(() => {
    if (statsDumpSession !== session || role !== 'producer') {
      return;
    }
    const sampledAt = performance.now();
    const elapsedMs = sampledAt - previousSampleAt;
    const sourceFramesDelta = sourceFramesTotal - previousSourceFrames;
    const layerSamples: Partial<Record<SimulcastLayer, ProducerLayerStatsSample>> = {};

    for (const layer of layers) {
      const current = producerLayerCounters.get(layer.id);
      const previous = previousLayers.get(layer.id);
      if (!current || !previous) {
        continue;
      }
      const encodedChunksDelta = current.chunks - previous.chunks;
      const encodedPayloadBytesDelta = current.bytes - previous.bytes;
      const keyFramesDelta = current.keyFrames - previous.keyFrames;
      const deltaFramesDelta = current.deltaFrames - previous.deltaFrames;
      const elapsedSeconds = elapsedMs > 0 ? elapsedMs / 1000 : undefined;
      layerSamples[layer.id] = {
        targetBitrateBps: layer.bitrate,
        encodedFps: elapsedSeconds === undefined
          ? null
          : encodedChunksDelta / elapsedSeconds,
        encodedChunksDelta,
        encodedChunksTotal: current.chunks,
        encodedPayloadBytesDelta,
        encodedPayloadBytesTotal: current.bytes,
        encodedPayloadBitrateKbps: elapsedSeconds === undefined
          ? null
          : encodedPayloadBytesDelta * 8 / 1000 / elapsedSeconds,
        keyFramesDelta,
        keyFramesTotal: current.keyFrames,
        deltaFramesDelta,
        deltaFramesTotal: current.deltaFrames,
        encodeQueueSize: Number.isFinite(layer.encoder.encodeQueueSize)
          ? layer.encoder.encodeQueueSize
          : null,
        width: layer.width,
        height: layer.height
      };
      previousLayers.set(layer.id, { ...current });
    }

    const elapsedSeconds = elapsedMs > 0 ? elapsedMs / 1000 : undefined;
    const sample: ProducerStatsSample = {
      timestamp: new Date().toISOString(),
      elapsedMs: elapsedSeconds === undefined ? null : elapsedMs,
      sourceFps: elapsedSeconds === undefined
        ? null
        : sourceFramesDelta / elapsedSeconds,
      sourceFramesDelta,
      sourceFramesTotal,
      layers: layerSamples,
      socketBufferedAmountBytes: typeof socket !== 'undefined'
        ? socket.bufferedAmount
        : null,
      browserJsHeapUsedBytes: readBrowserJsHeapUsedBytes()
    };
    session.appendProducerSample(sample);
    updateStatsDumpStatus(session);
    if (session.collectionStatus !== 'recording') {
      clearProducerStatsTimer();
    }
    previousSampleAt = sampledAt;
    previousSourceFrames = sourceFramesTotal;
  }, 1000);
}

async function startEncoder(
  stream: MediaStream,
  simulcastEnabled: boolean
): Promise<void> {
  const track = stream.getVideoTracks()[0];

  if (!track) {
    throw new Error('display stream has no video track');
  }

  // // MediaStreamTrackProcessor는 카메라 영상을 프레임 단위로 분해 (각 영상 프레임을 직접 꺼내야 화면을 수정하고 WebCodecs로 직접 인코딩할 수 있기 때문)
  const Processor = window.MediaStreamTrackProcessor;
  const processor = new Processor({ track });
  const reader = processor.readable.getReader();
  const settings = track.getSettings();
  let currentSourceWidth = normalizeVideoDimension(settings.width ?? 640);
  let currentSourceHeight = normalizeVideoDimension(settings.height ?? 360);
  const frameRate = Math.max(1, settings.frameRate ?? 30);
  const sourceCanvas = new OffscreenCanvas(
    currentSourceWidth,
    currentSourceHeight
  );
  const sourceContext = sourceCanvas.getContext('2d');
  let frameIndex = 0;

  if (!sourceContext) {
    throw new Error('OffscreenCanvas 2D context is unavailable');
  }

  const sourceCtx = sourceContext;
  const layerStates: LayerEncoderState[] = [];
  const requestedKeyframeEncodeFailures = new Map<SimulcastLayer, number>();
  const activeLayers = simulcastEnabled
    ? SIMULCAST_LAYERS
    : [SIMULCAST_LAYERS[2]];

  try {
    for (const layer of activeLayers) {
      const { width, height } = getLayerDimensions(
        currentSourceWidth,
        currentSourceHeight,
        layer.scaleDownBy
      );
      const config = createEncoderConfig(layer, width, height);
      const support = await VideoEncoder.isConfigSupported(config);

      if (!support.supported) {
        throw new Error(
          `VideoEncoder config is not supported for ${layer.id}: ${JSON.stringify(config)}`
        );
      }

      let encoder: VideoEncoder | undefined;
      const requestedKeyframesByTimestamp = new Map<
        number,
        RequestedKeyframeEncodingAudit
      >();

      try {
        encoder = new VideoEncoder({
          output: (chunk, metadata) => { // chunk는 더 이상 화면 전체 픽셀 한 장이 아니라, {코덱}으로 압축된 데이터 조각
            const requestedKeyframeAudit = requestedKeyframesByTimestamp.get(
              chunk.timestamp
            );
            if (requestedKeyframeAudit) {
              requestedKeyframesByTimestamp.delete(chunk.timestamp);
            }
            sendEncodedChunk(
              layer.id,
              chunk,
              metadata,
              requestedKeyframeAudit
                ? {
                  ...requestedKeyframeAudit,
                  outputAt: Date.now(),
                  chunkType: chunk.type
                }
                : undefined
            );
          },
          error: error => {
            logError(`${layer.id} encoder error: ${error.message}`);
          }
        });
        encoder.configure(support.config ?? config);
      } catch (error) {
        encoder?.close();
        throw error;
      }

      const canvas = new OffscreenCanvas(width, height);
      const context = canvas.getContext('2d');

      if (!context) {
        encoder.close();
        throw new Error(`${layer.id} OffscreenCanvas 2D context is unavailable`);
      }

      layerStates.push({
        id: layer.id,
        scaleDownBy: layer.scaleDownBy,
        bitrate: layer.bitrate,
        width,
        height,
        canvas,
        context,
        encoder,
        config: support.config ?? config,
        requestedKeyframesByTimestamp
      });

      debugLog(
        `${layer.id} encoder configured: ${width}x${height}@${Math.round(frameRate)}`
      );
    }
  } catch (error) {
    for (const layer of layerStates) {
      layer.encoder.close();
    }
    await reader.cancel().catch(() => undefined);
    throw error;
  }

  startProducerStatsSampling(layerStates);
  void pumpFrames(); // 펌프 호출. 반복문으로 영상 계속 읽어서 encodeFrame으로 넘김.

  async function pumpFrames(): Promise<void> {
    try {
      while (true) {
        const { done, value } = await reader.read();

        if (done || !value) {
          break;
        }

        try {
          await encodeFrame(value);
        } catch (error) {
          if (error instanceof RequestedKeyframeEncodeRetryError) {
            logError(error.message);
            continue;
          }
          throw error;
        }
      }
    } catch (error) {
      logError(
        `video pipeline failed: ${error instanceof Error ? error.message : String(error)}`
      );
      if (socket.readyState !== WebSocket.CLOSED) {
        socket.close();
      }
    } finally {
      pendingKeyframeRequests.clear();
      requestedKeyframeEncodeFailures.clear();
      for (const layer of layerStates) {
        layer.requestedKeyframesByTimestamp.clear();
      }
      stopActiveStatsCollection();
    }
  }

  async function encodeFrame(frame: VideoFrame): Promise<void> {
    frameIndex++;

    const processedFrames: VideoFrame[] = [];

    try {
      const frameWidth = normalizeVideoDimension(
        frame.displayWidth || frame.codedWidth || currentSourceWidth
      );
      const frameHeight = normalizeVideoDimension(
        frame.displayHeight || frame.codedHeight || currentSourceHeight
      );

      const periodicKeyframe =
        frameIndex % Math.max(1, Math.floor(frameRate * 2)) === 1;
      let resizeKeyframe = false;

      // 창 크기가 도중에 변경된 경우 활성 인코더를 함께 재설정하고 키프레임을 강제 발행
      if (
        currentSourceWidth !== frameWidth ||
        currentSourceHeight !== frameHeight
      ) {
        currentSourceWidth = frameWidth;
        currentSourceHeight = frameHeight;

        for (const layer of layerStates) {
          const dimensions = getLayerDimensions(
            currentSourceWidth,
            currentSourceHeight,
            layer.scaleDownBy
          );
          const nextConfig = {
            ...layer.config,
            width: dimensions.width,
            height: dimensions.height
          };
          const support = await VideoEncoder.isConfigSupported(nextConfig);

          if (!support.supported) {
            throw new Error(
              `VideoEncoder config is not supported for ${layer.id}: ${JSON.stringify(nextConfig)}`
            );
          }

          layer.width = dimensions.width;
          layer.height = dimensions.height;
          layer.config = support.config ?? nextConfig;
          layer.canvas.width = layer.width;
          layer.canvas.height = layer.height;
          layer.encoder.configure(layer.config);
        }

        sourceCanvas.width = currentSourceWidth;
        sourceCanvas.height = currentSourceHeight;
        resizeKeyframe = true;
        debugLog(
          `simulcast reconfigured: source ${currentSourceWidth}x${currentSourceHeight}`
        );
      }

      if (
        sourceCanvas.width !== currentSourceWidth ||
        sourceCanvas.height !== currentSourceHeight
      ) {
        sourceCanvas.width = currentSourceWidth;
        sourceCanvas.height = currentSourceHeight;
      }

      // // OffscreenCanvas에 프레임을 그리고 청록색 상자와 문구를 추가
      sourceCtx.drawImage(
        frame,
        0,
        0,
        currentSourceWidth,
        currentSourceHeight
      );
      sourceCtx.fillStyle = 'rgba(10, 92, 120, 0.78)';
      sourceCtx.fillRect(14, 14, 250, 72);
      sourceCtx.fillStyle = '#ffffff';
      sourceCtx.font = '20px sans-serif';
      sourceCtx.fillText('1:N Producer', 24, 46);
      sourceCtx.font = '14px sans-serif';
      sourceCtx.fillText(`encoded frame ${frameIndex}`, 24, 70);
      sourceFramesTotal++;

      for (const layer of layerStates) {
        layer.context.drawImage(
          sourceCanvas,
          0,
          0,
          layer.width,
          layer.height
        );

        const processedFrame = new VideoFrame(
          layer.canvas,
          frame.duration === null
            ? { timestamp: frame.timestamp }
            : { timestamp: frame.timestamp, duration: frame.duration }
        );
        processedFrames.push(processedFrame);
        const requestedKeyframe = pendingKeyframeRequests.get(layer.id);
        const keyFrame = Boolean(
          periodicKeyframe || resizeKeyframe || requestedKeyframe
        );
        if (requestedKeyframe) {
          layer.requestedKeyframesByTimestamp.set(processedFrame.timestamp, {
            requests: requestedKeyframe.requests.map(request => ({ ...request })),
            encodeRequestedAt: Date.now(),
            periodicKeyframe,
            resizeKeyframe,
            keyframeOption: keyFrame
          });
          if (IS_DEBUG) {
            debugLog(`forcing requested keyframe for ${layer.id}`);
          }
        }
        try {
          layer.encoder.encode(processedFrame, { keyFrame });
        } catch (error) {
          if (requestedKeyframe) {
            layer.requestedKeyframesByTimestamp.delete(processedFrame.timestamp);
            const previousFailures = requestedKeyframeEncodeFailures.get(layer.id) ?? 0;
            if (previousFailures === 0) {
              requestedKeyframeEncodeFailures.set(layer.id, 1);
              throw new RequestedKeyframeEncodeRetryError(layer.id, error);
            }
            requestedKeyframeEncodeFailures.delete(layer.id);
          }
          throw error;
        }
        if (requestedKeyframe) {
          requestedKeyframeEncodeFailures.delete(layer.id);
          if (pendingKeyframeRequests.get(layer.id) === requestedKeyframe) {
            pendingKeyframeRequests.delete(layer.id);
          }
        }
      }
    } finally {
      frame.close();
      for (const processedFrame of processedFrames) {
        processedFrame.close();
      }
    }
  }

  function createEncoderConfig(
    layer: (typeof SIMULCAST_LAYERS)[number],
    width: number,
    height: number
  ): VideoEncoderConfig {
    return {
      codec: 'avc1.42E01F',
      width,
      height,
      framerate: frameRate,
      bitrate: layer.bitrate,
      latencyMode: 'realtime',
      hardwareAcceleration: 'prefer-hardware', // 하드웨어 가속 설정?
      avc: { format: 'avc' }
    };
  }
}

function normalizeVideoDimension(value: number): number {
  const evenValue = Math.floor(Math.max(2, value));
  return evenValue - (evenValue % 2);
}

function isSimulcastLayer(value: unknown): value is SimulcastLayer {
  return SIMULCAST_LAYERS.some(layer => layer.id === value);
}

function getLayerDimensions(
  sourceWidth: number,
  sourceHeight: number,
  scaleDownBy: number
): { width: number; height: number } {
  return {
    width: normalizeVideoDimension(sourceWidth / scaleDownBy),
    height: normalizeVideoDimension(sourceHeight / scaleDownBy)
  };
}

function sendEncodedChunk(
  layer: SimulcastLayer,
  chunk: EncodedVideoChunk,
  metadata?: EncodedVideoChunkMetadata,
  requestedKeyframeAudit?: RequestedKeyframeEncodingAudit & {
    outputAt: number;
    chunkType: string;
  }
): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }

  // [추가] 소켓 버퍼가 1MB 이상 차있고 키프레임이 아닌 델타 프레임이면 송신 건너뛰기 [20260730]
  // drop 판단(1MiB threshold·delta만·key 유지)은 그대로, debug 문자열 생성만 차단
  if (socket.bufferedAmount > 1024 * 1024 && chunk.type !== 'key') {
    if (IS_DEBUG) {
      debugConsole(`Socket buffer > 1MB (${socket.bufferedAmount} bytes). Dropping delta frame.`);
    }
    return;
  }


  // // 스트림 데이터만 보내는게 아니라, 브라우저가 이해할 수 있는 메타데이터도 같이 보내야 함.
  const data = new Uint8Array(chunk.byteLength);
  const description = metadata?.decoderConfig?.description;
  const header = {
    event: 'encodedChunk',
    layer,
    timestamp: chunk.timestamp,
    type: chunk.type as BrowserChunkType,
    requestedKeyframeAudit,
    duration: chunk.duration,
    metadata: description
      ? {
        decoderConfig: {
          codec: metadata.decoderConfig?.codec,
          descriptionBase64: arrayBufferToBase64(description)
        }
      }
      : undefined
  };

  chunk.copyTo(data);

  const headerBytes = new TextEncoder().encode(JSON.stringify(header));
  const packet = new Uint8Array(4 + headerBytes.byteLength + data.byteLength);
  const view = new DataView(packet.buffer);

  view.setUint32(0, headerBytes.byteLength);
  packet.set(headerBytes, 4);
  packet.set(data, 4 + headerBytes.byteLength);
  socket.send(packet);

  const counters = producerLayerCounters.get(layer);
  if (counters) {
    counters.chunks++;
    counters.bytes += chunk.byteLength;
    if (chunk.type === 'key') {
      counters.keyFrames++;
    } else {
      counters.deltaFrames++;
    }
  }

  encodedChunks++;
  chunkCount.textContent = String(encodedChunks);
}

function request<T>(action: string, data?: unknown): Promise<T> {
  const id = String(++requestSeq);

  socket.send(JSON.stringify({ id, action, data }));

  return new Promise<T>((resolve, reject) => {
    pending.set(id, {
      resolve: value => resolve(value as T),
      reject
    });
  });
}

function assertWebCodecsSupport(): void {
  if (
    !('VideoEncoder' in window) ||
    !('VideoFrame' in window) ||
    !('MediaStreamTrackProcessor' in window)
  ) {
    throw new Error('This browser does not expose the required WebCodecs APIs');
  }
}

function arrayBufferToBase64(buffer: AllowSharedBufferSource): string {
  const bytes =
    buffer instanceof ArrayBuffer
      ? new Uint8Array(buffer)
      : new Uint8Array(buffer as SharedArrayBuffer);
  let binary = '';

  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }

  return btoa(binary);
}

/** 화면 log box에만 기록 (production 콘솔 미러링 없음) */
function writeLog(line: string): void {
  const time = new Date().toLocaleTimeString();

  logBox.textContent = `[${time}] ${line}\n${logBox.textContent}`;
}

/** debug 전용 상태 메시지: debug 모드에서만 log box + console.debug */
function debugLog(line: string): void {
  if (!IS_DEBUG) {
    return;
  }
  writeLog(line);
  console.debug('[sfu]', line);
}

/** 실제 오류: production에서도 console.error (log box는 debug에서만 보임) */
function logError(line: string): void {
  writeLog(line);
  console.error('[sfu]', line);
}

function summarizeServerStats(data: unknown): string {
  const stats = data as {
    producer?: Array<Record<string, unknown>>;
    consumer?: Array<Record<string, unknown>>;
    transport?: Array<Record<string, unknown>>;
  };
  const producer = stats.producer?.[0];
  const consumer = stats.consumer?.[0];
  const transport = stats.transport?.[0];
  const producerPackets =
    producer?.packetCount ?? producer?.packetsReceived ?? producer?.packetsSent;
  const consumerPackets =
    consumer?.packetCount ?? consumer?.packetsSent ?? consumer?.packetsReceived;
  const transportSent = transport?.rtpBytesSent ?? transport?.bytesSent;

  return `server stats: producerPackets=${String(
    producerPackets ?? 'n/a'
  )}, consumerPackets=${String(
    consumerPackets ?? 'n/a'
  )}, transportSent=${String(transportSent ?? 'n/a')}`;
}

declare global {
  interface Window {
    MediaStreamTrackProcessor: new (init: {
      track: MediaStreamTrack;
    }) => {
      readable: ReadableStream<VideoFrame>;
    };
  }
}
