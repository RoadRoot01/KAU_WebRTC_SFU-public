# WebCodecs mediasoup 1:N

## 한국어

이 프로젝트는 브라우저의 WebCodecs API로 화면/창/탭 공유(`getDisplayMedia()`) 영상을 H.264로 인코딩한 뒤, 로컬 mediasoup 파이프라인을 통해 1:N 구조로 전달하는 예제입니다. `Start`를 눌러 가장 먼저 WebSocket 연결에 성공한 peer가 producer가 되어 화면 공유 영상을 송신하고, 이후 연결한 peer들은 consumer가 되어 같은 스트림을 수신합니다.

## 주요 기능

- WebCodecs `VideoEncoder`를 사용한 실시간 H.264 인코딩
- WebSocket 기반 signaling
- mediasoup WebRTC transport를 통한 consumer 수신
- producer/consumer 역할 자동 할당
- 연결 상태, 처리된 Encoded Chunk, 서버 통계, 디코딩 프레임 수 등 기본 통계 표시

## Simulcast 모드와 Consumer 화질 우선

- Producer는 `Start` 전에 `Simulcast`를 선택할 수 있으며 기본값은 ON입니다. 한 세션이 시작되면 모드는 변경되지 않습니다.
- ON에서는 LOW/MID/HIGH WebCodecs encoder 3개와 mediasoup simulcast Producer를 사용합니다.
- OFF에서는 HIGH encoder 하나만 만들고 전송하며 mediasoup simple Producer를 사용합니다.
- Consumer는 Producer가 Simulcast ON일 때 LOW/MID/HIGH 화질 우선을 선택할 수 있습니다. 선택은 mediasoup `setPreferredLayers()`에 전달되는 preferred upper layer이며, 네트워크 상태(BWE)에 따라 실제 layer가 더 낮아질 수 있습니다.
- Simulcast OFF Consumer에는 layer 선택 UI가 표시되지 않으며, 서버도 `setConsumerLayer` 요청을 거부합니다.

## 요구 사항

- **Node.js 22.12 이상**
- npm
- Git
- WebCodecs와 `MediaStreamTrackProcessor`를 지원하는 브라우저
- 화면/창/탭 공유 권한
- 이 저장소와 같은 상위 폴더에 준비된 sibling `mediasoup` 소스/산출물

이 프로젝트는 npm의 `mediasoup` 패키지를 서버에서 직접 사용하는 구조가 아니라, sibling `mediasoup` checkout의 빌드 산출물을 직접 import합니다. 현재 검증된 기준 환경은 upstream `versatica/mediasoup`의 `v3` 계열, commit `539145a703ea745dc92531400d32505269ad87f9`(mediasoup 3.26.0)입니다.

권장 workspace 구조는 다음과 같습니다.

```text
workspace/
├─ mediasoup/
└─ WebRTC_SFU/
```

서버와 TypeScript build는 sibling `mediasoup`의 다음 파일을 사용합니다.

```text
../mediasoup/node/lib/index.js
../mediasoup/node/lib/types.js
../mediasoup/node/lib/webCodecsPipeline.js
../mediasoup/worker/out/Release/mediasoup-worker[.exe]
```

`index.js`와 `types.js`는 mediasoup 자체 TypeScript 빌드가 생성하는 산출물이며 수정하지 않습니다. WebRTC_SFU의 custom pipeline은 `docs/custom_code/webCodecsPipeline.js`를 `../mediasoup/node/lib/webCodecsPipeline.js`로 동기화해 사용하며, 서버는 이 파일을 직접 import합니다. 따라서 mediasoup의 `index.js`에 custom export를 추가하거나 별도의 custom `index.js`를 만들 필요가 없습니다.

Windows에서는 worker 파일명이 `mediasoup-worker.exe`이고, 다른 환경에서는 `mediasoup-worker`입니다.

## 범위 및 보안

이 프로젝트는 통제된 LAN 및 실험 환경을 위한 연구용 예제이며, 공용 인터넷에 직접 노출하도록 보안 강화된 서비스가 아닙니다. 현재 WebSocket signaling에는 사용자 인증과 Origin allowlist가 없고, 최초로 연결한 peer가 producer가 됩니다. `/health`는 공지 IP, WebRTC 포트 범위, peer 수와 runtime 통계를 반환합니다.

backend와 WebRTC transport는 `0.0.0.0`에 바인딩됩니다. 기본 backend port는 `3000`, 브라우저 HTTPS port는 `5173`, WebRTC UDP/TCP port range는 `40000-40100`입니다. 신뢰할 수 있는 격리 네트워크와 적절한 방화벽 안에서만 실행하세요.

## 실행 방법

### Fresh clone 준비

먼저 이 공개 저장소를 clone합니다.

```bash
git clone https://github.com/RoadRoot01/KAU_WebRTC_SFU-public.git
cd KAU_WebRTC_SFU-public
```


1. WebRTC_SFU와 sibling `mediasoup`가 같은 상위 폴더에 오도록 준비합니다. WebRTC_SFU를 clone한 뒤 그 상위 폴더에서 검증된 mediasoup commit을 준비할 수 있습니다.

```bash
cd ..
git clone https://github.com/versatica/mediasoup.git
cd mediasoup
git checkout 539145a703ea745dc92531400d32505269ad87f9
npm ci
```

`mediasoup`의 `npm ci`는 Node 산출물을 준비하고 worker prebuilt 다운로드를 시도하며, 사용할 수 없는 경우 로컬 worker build로 fallback합니다. 로컬 worker build가 필요한 환경에서는 Python 3.10+와 플랫폼별 C/C++ toolchain 등 추가 prerequisites가 필요할 수 있습니다. 자세한 요구 사항은 [mediasoup Installation](https://mediasoup.org/documentation/v3/mediasoup/installation/)을 참고하세요.

2. WebRTC_SFU 의존성을 설치합니다.

```bash
cd ../KAU_WebRTC_SFU-public
npm ci
```

3. 이 저장소의 canonical custom pipeline을 sibling mediasoup runtime에 동기화하고 검증합니다.

```bash
npm run sync-mediasoup-pipeline
npm run verify-mediasoup-pipeline
```

`sync-mediasoup-pipeline`은 `docs/custom_code/webCodecsPipeline.js`를 `../mediasoup/node/lib/webCodecsPipeline.js`로 복사합니다. `verify-mediasoup-pipeline`은 두 파일이 byte-for-byte 동일한지 확인합니다.

> **중요:** sibling mediasoup에서 `npm run typescript:build`를 실행하면 `node/lib`가 삭제 후 다시 생성됩니다. mediasoup Node 산출물을 다시 빌드한 경우에는 반드시 WebRTC_SFU에서 `npm run sync-mediasoup-pipeline`을 다시 실행하세요.

4. 필요하면 build와 stats 테스트를 먼저 확인합니다.

```bash
npm test
```

5. 개발 서버를 실행합니다.

```bash
npm run dev
```

이 명령은 signaling backend와 HTTPS Vite 개발 서버를 동시에 실행합니다.

| Mode | Command | Browser URL |
| --- | --- | --- |
| Dev | `npm run dev` | `https://<server-ip>:5173` |
| Prod | `npm run start:prod` | `https://<server-ip>:5173` |

`5173`은 개발과 운영에서 공통으로 쓰는 고정 HTTPS browser port입니다. 예를 들어 `https://<LAN_SERVER_IP>:5173`에 접속해 `Start`를 누릅니다. `3000`은 기본 backend/signaling port이며, 브라우저는 여기에 직접 접속하지 않습니다. `/ws`와 `/health` 요청은 5173 gateway에서 backend로 전달됩니다. 기본 self-signed certificate 때문에 브라우저에서 경고를 승인해야 할 수 있습니다.

6. `Start`를 누릅니다. WebSocket 연결에 성공한 첫 peer가 producer가 됩니다. 화면 공유 대상을 선택하면 WebCodecs로 인코딩된 영상이 서버로 전송됩니다.

7. 같은 주소를 다른 브라우저 탭 또는 창에서 다시 열고 `Start`를 누르면 consumer로 접속하여 producer의 영상을 수신합니다.

## Production 실행

`dist/`는 Vite가 생성하는 build artifact이며 Git에서 추적하지 않습니다. 새 clone에는 `dist/`가 없을 수 있고, `npm run build`가 TypeScript를 검사한 뒤 Vite production bundle을 생성합니다.

위의 sibling mediasoup 준비와 custom pipeline 동기화가 완료된 환경에서 사용하는 canonical production 명령은 다음과 같습니다.

```bash
npm run start:prod
```

이 명령은 production build를 완료한 뒤 backend와 Vite preview를 함께 실행합니다. npm lifecycle은 `server:prod` 전에 canonical mediasoup pipeline을 검증하고, Vite preview는 production bundle을 HTTPS 5173에서 제공합니다. `/ws`와 `/health`는 내부 backend로 proxy됩니다. 두 장기 실행 프로세스 중 하나가 종료되면 다른 하나도 종료됩니다. 브라우저에서는 `https://<server-ip>:5173`에 접속합니다.

`npm run server:prod`는 production 설정의 backend만 실행하는 low-level 명령이며 frontend build나 Vite preview를 시작하지 않습니다. `dist/`가 있으면 Express의 static serving이 fallback으로 동작합니다. 일반 개발/운영 실행에는 `npm run dev` 또는 `npm run start:prod`를 사용합니다.

## 로그 모드 (production / debug)

- Production: `npm run build`로 만든 클라이언트 번들은 기본적으로 debug가 꺼져 있습니다. 서버는 production 로깅(필수 startup info + warning/error만 출력, mediasoup worker `logLevel: warn`)으로 동작하며, 화면의 debug log box는 숨겨집니다.
- Debug: `npm run dev`는 서버 debug + Vite 클라이언트 debug를 함께 실행합니다. 화면 log box가 표시되고 room/simulcast/stats 상세 로그가 출력되며, mediasoup worker는 `logLevel: debug`로 동작합니다.
- 추가 debug 방법: 서버는 `npm run server:debug` 또는 `SFU_DEBUG=1`, 프로덕션 번들 클라이언트는 `?debug=1` 또는 `localStorage sfu_debug=1`로 강제 debug on이 가능합니다.
- client debug와 server debug는 서로 독립적입니다. prod 번들 URL에 `?debug=1`을 붙여도 서버가 자동으로 debug가 되지 않으며, 전체 상세 진단이 필요하면 서버도 `npm run server:debug` (또는 `SFU_DEBUG=1`)로 실행해야 합니다.

## signaling 포트 (PORT)

- 기본 backend/signaling 포트는 `3000`이며, Vite dev/preview의 `/ws`와 `/health` proxy가 이 backend를 사용합니다.
- custom 포트(예: `3100`)를 지정하면 `PORT`는 backend와 proxy target에만 적용됩니다. browser port는 계속 5173입니다. PowerShell 예: `$env:PORT="3100"; npm run dev` 또는 `$env:PORT="3100"; npm run start:prod`.

## 세션 통계 덤프

Producer 또는 Consumer 세션에 참여한 뒤 **Dump Stats**를 누르면 해당 브라우저가 수집한 `schemaVersion: 1` JSON 측정 파일을 다운로드합니다. 샘플은 해당 페이지 안에서 수집되고 파일도 그 브라우저에서 생성·다운로드됩니다. 페이지를 새로고침하면 새 측정 세션이 시작됩니다.

단일 파일 대시보드 `https://<server-ip>:5173/stats-visualizer.html`을 열고 Producer 또는 Consumer 덤프 하나를 선택합니다. Producer 덤프는 source-frame FPS와 LOW/MID/HIGH encoder 출력을 별도로 기록하며, Simulcast OFF에서는 HIGH만 기록합니다. Consumer 샘플은 기존의 초당 한 번 실행되는 브라우저 `getStats()` polling 결과를 사용합니다. 브라우저가 제공하지 않는 counter는 `n/a`로 표시하며, packet rate를 video FPS 추정치로 사용하지 않습니다.

Producer bitrate는 encoded chunk byte로 계산한 **인코딩 payload bitrate**이며 WebSocket header 등 transport/network overhead는 포함하지 않습니다. **Browser JS Heap**은 Chromium이 제공할 때 기록하는 JavaScript heap이며 시스템 RAM이 아닙니다.

Consumer의 **Received FPS**와 **Decoded FPS**는 각각 RTCStats timestamp 구간의 `framesReceived`와 `framesDecoded` counter 차이로 계산하며, 렌더링/display FPS가 아닙니다. **Received media bitrate**는 `bytesReceived` 차이로 계산하며 전체 네트워크 사용량이 아닙니다. `packetsLost`에는 유효한 음수도 포함해 브라우저의 부호 있는 counter 값이 유지됩니다. `getStats()`가 실패하거나 빈 결과를 반환한 샘플은 missing으로 기록되고 rate 기준값이 초기화됩니다. 복구 후 첫 샘플은 기준값을 설정하고 그 다음 샘플부터 rate를 계산합니다.

페이지 세션은 최대 **4,200개 샘플 또는 70분** 중 먼저 도달하는 한도까지 기록합니다. 한도에 도달하면 기존 샘플은 보존하고 추가 기록을 멈추며 **Dump Stats**는 계속 사용할 수 있습니다. Producer capture/socket이 닫히거나 Consumer 세션이 끝나면 수집 상태가 stopped로 바뀌지만 덤프 데이터는 유지됩니다. Visualizer는 기본적으로 로컬에 포함된 **Chart.js 4.5.1**을 사용하고 chart module이 실패하면 SVG로 대체합니다. collection-status metadata가 없는 Phase 1 v1 덤프를 포함해 schema v1 파일을 읽습니다.

## 기타 명령어

```bash
npm run server
```

signaling 서버만 실행합니다.

```bash
npm run client
```

Vite 클라이언트만 실행합니다.

```bash
npm run build
```

TypeScript 타입 검사와 Vite 프로덕션 빌드를 실행합니다.

```bash
npm run preview
```

빌드 결과물을 Vite preview 서버로 확인합니다.

독립 확인이 필요하면 `npm run preview`를 HTTPS 5173에서 실행할 수 있습니다. 이 경우 `/ws`와 `/health` proxy를 사용하려면 backend를 별도로 실행해야 합니다. 기본 사용자 실행 경로는 개발에서 `npm run dev`, 운영에서 `npm run start:prod`입니다.

```bash
npm run sync-mediasoup-pipeline
```

`docs/custom_code/webCodecsPipeline.js`를 `../mediasoup/node/lib/webCodecsPipeline.js`로 복사합니다.

```bash
npm run verify-mediasoup-pipeline
```

canonical 파일과 sibling runtime이 정확히 일치하는지 확인합니다. `npm run server`, `npm run server:debug`, `npm run server:prod`는 시작 전에 이 검증을 자동으로 실행하지만 파일을 자동으로 sync하지는 않습니다.

### Custom pipeline workflow

custom pipeline을 변경할 때는 `docs/custom_code/webCodecsPipeline.js`만 수정한 뒤 `npm run sync-mediasoup-pipeline`와 `npm run verify-mediasoup-pipeline`를 실행하고, build/runtime 검증 후 canonical 파일을 커밋합니다. `src/webCodecsPipeline.d.ts`의 API 선언과 `src/server.ts`의 사용 방식도 함께 일치하는지 확인합니다.

## Upstream 출처 및 라이선스

서버는 별도 sibling checkout으로 준비하는 upstream [versatica/mediasoup](https://github.com/versatica/mediasoup) 3.26.0을 commit [539145a703ea745dc92531400d32505269ad87f9](https://github.com/versatica/mediasoup/commit/539145a703ea745dc92531400d32505269ad87f9) 기준으로 사용하고, npm의 `mediasoup-client`도 사용합니다. mediasoup 소스와 build 산출물은 이 저장소에 포함하지 않습니다.

`docs/custom_code/webCodecsPipeline.js`는 WebCodecs 인코딩 청크를 mediasoup DirectTransport에 주입하기 위한 이 프로젝트의 custom integration source입니다. upstream 컴포넌트의 저작권 및 라이선스 고지는 각 upstream checkout/package에 포함된 상태로 유지하세요. 이 저장소 자체 코드에는 root ISC License가 적용됩니다.

## 참고 사항

- backend HTTP/WebSocket은 기본적으로 `0.0.0.0:3000`에 바인딩됩니다. mediasoup WebRTC transport도 UDP/TCP `0.0.0.0`에 listen하지만, ICE에 공지하는 주소(`SERVER_IP`)는 별도로 네트워크 인터페이스에서 선택합니다. `PORT`는 backend port와 Vite proxy target을 함께 바꾸며 browser-facing HTTPS port 5173은 고정입니다.
- 자동 주소 선택은 이름에 `veth`, `wsl`, `virtual`, `vmware`, `docker`가 들어간 adapter와 internal/non-IPv4 주소를 건너뜁니다. `192.168.56.x`도 제외하며, 나머지 사용 가능한 주소 중 `192.168.x.x`를 우선 선택하고 없으면 다른 non-internal IPv4를 선택합니다. 적합한 주소가 없으면 `127.0.0.1`을 사용합니다.
- `SFU_ANNOUNCED_IP`를 설정하면 mediasoup가 공지할 IP를 명시적으로 덮어쓸 수 있습니다. 값은 공백이 없는 유효한 IPv4 또는 IPv6 주소 literal이어야 합니다. 이 설정은 공지 주소만 바꾸고 backend 및 WebRTC transport의 listen address는 바꾸지 않습니다. 예: `$env:SFU_ANNOUNCED_IP="<LAN_SERVER_IP>"; npm run dev`.
- mediasoup worker는 RTP 포트 범위 `40000-40100`을 사용합니다.
- 다른 LAN 기기에서 접속할 경우 host firewall에서 HTTPS `5173`과 mediasoup WebRTC의 UDP/TCP `40000-40100` 통신이 허용되어야 합니다.
- producer가 종료되면 consumer는 producer 종료 이벤트를 받고, 새 producer로 다시 참여하려면 페이지를 새로고침해야 합니다.
- 브라우저에서 WebCodecs H.264 인코딩을 지원하지 않으면 producer 시작에 실패할 수 있습니다.
- `npm run start:prod`의 browser-facing endpoint는 `@vitejs/plugin-basic-ssl`을 사용하는 HTTPS 5173입니다. LAN 화면 공유는 `https://<server-ip>:5173`에서 수행하며, self-signed certificate 경고를 승인해야 할 수 있습니다. backend HTTP port는 내부 signaling 용도입니다.

---

## English

This project is a 1:N streaming example that encodes display, window, or tab capture from `getDisplayMedia()` with the browser WebCodecs API as H.264, then forwards it through a local mediasoup pipeline. The peer whose WebSocket connection succeeds first after clicking `Start` becomes the producer and sends the captured video. Later peers become consumers and receive the same stream.

## Key Features

- Real-time H.264 encoding with the WebCodecs `VideoEncoder`
- WebSocket-based signaling
- Consumer receiving through mediasoup WebRTC transports
- Automatic producer/consumer role assignment
- Basic status and stats display, including connection state, processed encoded chunks, server stats, and decoded frames

## Simulcast modes and Consumer quality preference

- The Producer chooses Simulcast before `Start`; the default is ON and the mode is immutable after the session starts.
- ON creates three LOW/MID/HIGH WebCodecs encoders and a mediasoup simulcast Producer.
- OFF creates and sends only the HIGH encoder and uses a mediasoup simple Producer.
- When the Producer is Simulcast ON, a Consumer can choose LOW, MID, or HIGH as its preferred quality. The choice is sent to mediasoup through `setPreferredLayers()` as a preferred upper layer; BWE may still select a lower current layer.
- Simulcast OFF hides Consumer layer controls, and the server rejects `setConsumerLayer` requests.

## Requirements

- **Node.js 22.12 or later**
- npm
- Git
- A browser that supports WebCodecs and `MediaStreamTrackProcessor`
- Display, window, or tab capture permission
- A prepared sibling `mediasoup` source/output tree next to this repository

This project does not use the npm `mediasoup` package as its server runtime. Instead, it imports generated files directly from a sibling `mediasoup` checkout. The currently tested baseline is upstream `versatica/mediasoup` on the `v3` line at commit `539145a703ea745dc92531400d32505269ad87f9` (mediasoup 3.26.0).

The recommended workspace layout is:

```text
workspace/
├─ mediasoup/
└─ WebRTC_SFU/
```

The server and TypeScript build use the following files from the sibling checkout:

```text
../mediasoup/node/lib/index.js
../mediasoup/node/lib/types.js
../mediasoup/node/lib/webCodecsPipeline.js
../mediasoup/worker/out/Release/mediasoup-worker[.exe]
```

`index.js` and `types.js` are generated by mediasoup's own TypeScript build and should not be edited. WebRTC_SFU's custom pipeline is synchronized from `docs/custom_code/webCodecsPipeline.js` to `../mediasoup/node/lib/webCodecsPipeline.js`, and the server imports that file directly. There is therefore no need to add a custom export to mediasoup's `index.js` or create a separate custom `index.js`.

On Windows the worker executable is `mediasoup-worker.exe`; on other platforms it is `mediasoup-worker`.

## Scope and security

This is a research example for controlled LAN and experimental use; it is not hardened for direct exposure to the public Internet. The current WebSocket signaling endpoint has no user authentication or Origin allowlist, and the first peer to connect becomes the producer. `/health` returns the announced IP, WebRTC port range, peer counts, and runtime counters.

The backend and WebRTC transport bind to `0.0.0.0`. The default backend port is `3000`, the browser-facing HTTPS port is `5173`, and the WebRTC UDP/TCP port range is `40000-40100`. Run it only inside a trusted, isolated network with an appropriate firewall.

## How to Run

### Fresh clone setup

First clone this public repository:

```bash
git clone https://github.com/RoadRoot01/KAU_WebRTC_SFU-public.git
cd KAU_WebRTC_SFU-public
```


1. Place WebRTC_SFU and the sibling `mediasoup` checkout under the same parent directory. After cloning WebRTC_SFU, prepare the tested mediasoup commit from its parent directory.

```bash
cd ..
git clone https://github.com/versatica/mediasoup.git
cd mediasoup
git checkout 539145a703ea745dc92531400d32505269ad87f9
npm ci
```

`npm ci` in mediasoup prepares the Node outputs and attempts to download a prebuilt worker, falling back to a local worker build when needed. A local worker build may require Python 3.10+ and a platform-specific C/C++ toolchain. See the [mediasoup Installation](https://mediasoup.org/documentation/v3/mediasoup/installation/) requirements for details.

2. Install WebRTC_SFU dependencies.

```bash
cd ../KAU_WebRTC_SFU-public
npm ci
```

3. Synchronize this repository's canonical custom pipeline into the sibling mediasoup runtime and verify it.

```bash
npm run sync-mediasoup-pipeline
npm run verify-mediasoup-pipeline
```

`sync-mediasoup-pipeline` copies `docs/custom_code/webCodecsPipeline.js` to `../mediasoup/node/lib/webCodecsPipeline.js`. `verify-mediasoup-pipeline` checks that the two files are byte-for-byte identical.

> **Important:** running `npm run typescript:build` in the sibling mediasoup checkout deletes and recreates `node/lib`. If you rebuild mediasoup's Node outputs, run `npm run sync-mediasoup-pipeline` again from WebRTC_SFU afterward.

4. Optionally verify the production build and focused stats tests first.

```bash
npm test
```

5. Start the development servers.

```bash
npm run dev
```

This command starts the signaling backend and the HTTPS Vite development server.

| Mode | Command | Browser URL |
| --- | --- | --- |
| Dev | `npm run dev` | `https://<server-ip>:5173` |
| Prod | `npm run start:prod` | `https://<server-ip>:5173` |

Port `5173` is the fixed HTTPS browser port in both modes. For example, open `https://<LAN_SERVER_IP>:5173` and click `Start`. Port `3000` is the default backend/signaling port; browsers do not connect to it directly. The `5173` gateway proxies `/ws` and `/health` to the backend. The browser may ask you to accept the self-signed certificate warning.

6. Click `Start`. The first peer whose WebSocket connection succeeds becomes the producer. After a display, window, or tab is selected, WebCodecs-encoded video is sent to the server.

7. Open the same address in another browser tab or window and click `Start` again. It joins as a consumer and receives the producer video.

## Production startup

`dist/` is a Vite-generated build artifact and is not tracked by Git. A fresh clone may not contain `dist/`; `npm run build` type-checks the project and creates the Vite production bundle.

After the sibling mediasoup runtime and custom pipeline have been prepared as described above, use this canonical production command:

```bash
npm run start:prod
```

It completes the production build, then starts the backend and Vite preview together. npm's lifecycle hook verifies the canonical mediasoup pipeline before `server:prod`; Vite preview serves the production bundle over HTTPS on port 5173. `/ws` and `/health` are proxied internally to the backend. If either long-running process exits, its sibling is stopped. Open `https://<server-ip>:5173` in the browser.

`npm run server:prod` is the low-level backend-only command for production settings; it does not build the frontend or start Vite preview. If `dist/` exists, Express static serving remains available as fallback behavior. Use `npm run dev` or `npm run start:prod` for the normal development or production flow.

## Logging modes (production / debug)

- Production: the client bundle created with `npm run build` has debug off by default. The server uses production logging (essential startup info plus warnings/errors only, mediasoup worker `logLevel: warn`), and the on-screen debug log box is hidden.
- Debug: `npm run dev` runs the server in debug mode together with the Vite debug client. The log box is shown with detailed room/simulcast/stats logs, and the mediasoup worker runs with `logLevel: debug`.
- Extra ways to enable debug: server via `npm run server:debug` or `SFU_DEBUG=1`; production-bundle client via `?debug=1` or `localStorage sfu_debug=1`.
- Client debug and server debug are independent. Adding `?debug=1` to a prod bundle URL does not enable server debug; for full diagnostics, also run the server with `npm run server:debug` (or `SFU_DEBUG=1`).

## Signaling port (PORT)

- The default backend/signaling port is `3000`; Vite dev/preview proxies `/ws` and `/health` to it.
- A custom `PORT` (e.g. `3100`) changes only the backend and proxy target. The browser port remains 5173. PowerShell examples: `$env:PORT="3100"; npm run dev` or `$env:PORT="3100"; npm run start:prod`.

## Session stats dumps

After joining a Producer or Consumer session, use **Dump Stats** to download that browser's `schemaVersion: 1` JSON measurement. Samples are collected in the local page and the file is generated and downloaded by that browser. Reloading the page starts a new measurement session.

Open the single-file dashboard at `https://<server-ip>:5173/stats-visualizer.html`, then choose one Producer or Consumer dump. Producer dumps report source-frame FPS separately from LOW/MID/HIGH encoder output; Simulcast OFF records only HIGH. Consumer samples reuse the existing one-second browser `getStats()` poll. Missing browser counters display as `n/a`; packet rate is never used as a video FPS estimate.

Producer bitrate is **encoded payload bitrate** calculated from encoded chunk bytes; it excludes WebSocket headers and other transport/network overhead. **Browser JS Heap** is the JavaScript heap reported by Chromium when available, not System RAM.

Consumer **Received FPS** and **Decoded FPS** are respectively based on `framesReceived` and `framesDecoded` counter deltas over RTCStats timestamps; they are not rendered/display FPS. **Received media bitrate** uses `bytesReceived` delta and is not total network usage. `packetsLost` preserves the browser's signed counter, including valid negative values. Failed or empty `getStats()` samples are recorded as missing and reset the rate baseline; the first recovered sample establishes a baseline before rates resume.

Each page session keeps up to **4,200 samples or 70 minutes**, whichever limit comes first. Reaching either limit stops adding samples, keeps every sample already collected, and leaves **Dump Stats** available. Closing the Producer capture/socket or ending the Consumer session marks collection stopped without deleting the dump data. The Visualizer uses locally bundled **Chart.js 4.5.1** by default and falls back to SVG if its chart module fails. It accepts schema v1 files, including Phase 1 v1 dumps without collection-status metadata; legacy custom-format files are not supported.

## Other Commands

```bash
npm run server
```

Starts only the signaling server.

```bash
npm run client
```

Starts only the Vite client.

```bash
npm run build
```

Runs TypeScript type checking and creates a Vite production build.

```bash
npm run preview
```

Serves the built output with the Vite preview server.

For a standalone check, `npm run preview` serves the built output over HTTPS on port 5173. Run the backend separately if you need `/ws` or `/health` proxying. The usual entrypoints are `npm run dev` for development and `npm run start:prod` for production.

```bash
npm run sync-mediasoup-pipeline
```

Copies `docs/custom_code/webCodecsPipeline.js` to `../mediasoup/node/lib/webCodecsPipeline.js`.

```bash
npm run verify-mediasoup-pipeline
```

Checks that the canonical file and sibling runtime are an exact byte match. `npm run server`, `npm run server:debug`, and `npm run server:prod` run this verification automatically before startup, but they do not synchronize the file automatically.

### Custom pipeline workflow

When changing the custom pipeline, edit only `docs/custom_code/webCodecsPipeline.js`, run `npm run sync-mediasoup-pipeline` and `npm run verify-mediasoup-pipeline`, then run build/runtime validation before committing the canonical file. Also confirm that `src/webCodecsPipeline.d.ts` and the usage in `src/server.ts` still match the exported API.

## Upstream attribution and licensing

The server uses upstream [versatica/mediasoup](https://github.com/versatica/mediasoup) 3.26.0 at commit [539145a703ea745dc92531400d32505269ad87f9](https://github.com/versatica/mediasoup/commit/539145a703ea745dc92531400d32505269ad87f9) from a separate sibling checkout, plus the `mediasoup-client` npm package. The mediasoup source and build outputs are not included in this repository.

`docs/custom_code/webCodecsPipeline.js` is this project's custom integration source for injecting WebCodecs encoded chunks into mediasoup DirectTransport. Keep upstream copyright and license notices with each upstream checkout/package. The root ISC License applies to this repository's own code.

## Notes

- The backend HTTP/WebSocket server binds to `0.0.0.0:3000` by default. The mediasoup WebRTC transport separately listens on UDP/TCP `0.0.0.0`, while the IP (`SERVER_IP`) announced in ICE is selected from network interfaces. `PORT` changes the backend and Vite proxy target; the browser-facing HTTPS port remains fixed at 5173.
- Automatic address selection skips adapters whose names contain `veth`, `wsl`, `virtual`, `vmware`, or `docker`, and skips internal/non-IPv4 addresses. It also excludes `192.168.56.x`, prefers an eligible `192.168.x.x`, then another non-internal IPv4, and falls back to `127.0.0.1` if none is available.
- Set `SFU_ANNOUNCED_IP` to explicitly override the IP mediasoup announces. The value must be an unpadded valid IPv4 or IPv6 address literal. This changes only the announced address; it does not change the backend or WebRTC transport listen addresses. Example: `$env:SFU_ANNOUNCED_IP="<LAN_SERVER_IP>"; npm run dev`.
- The mediasoup worker uses RTP ports `40000-40100`.
- For access from other LAN devices, the host firewall must allow HTTPS `5173` and mediasoup WebRTC traffic on UDP/TCP `40000-40100`.
- If the producer closes, consumers receive a producer-closed event. Refresh the page to join again with a new producer.
- Producer startup can fail if the browser does not support WebCodecs H.264 encoding.
- `npm run start:prod` exposes its browser-facing endpoint over HTTPS on port 5173 through `@vitejs/plugin-basic-ssl`. LAN screen capture uses `https://<server-ip>:5173`; the browser may require accepting the self-signed certificate warning. The backend HTTP port is for internal signaling.
