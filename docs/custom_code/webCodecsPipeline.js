"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.setupPipeline = setupPipeline;
exports.injectEncodedChunk = injectEncodedChunk;
exports.getPipelineStats = getPipelineStats;
const RTP_VERSION = 0x80;
const RTP_HEADER_SIZE = 12;
const H264_PAYLOAD_TYPE = 96;
const H264_CLOCK_RATE = 90000;
const H264_MAX_PAYLOAD_SIZE = 1200;
const DEFAULT_H264_PROFILE_LEVEL_ID = '42e01f';
const RTCP_SR_PACKET_SIZE = 28;
const RTCP_SR_PACKET_TYPE = 200;
const RTCP_SR_LENGTH = 6;
const NTP_EPOCH_OFFSET_SECONDS = 2208988800n;
const MICROSECONDS_PER_SECOND = 1000000n;
const NTP_FRACTION_SCALE = 0x100000000n;
const UINT32_MAX = 0xffffffffn;
let producer;
let directTransport;
let senderReportTimer;
let captureClockAnchor;
let pipelinePayloadType = H264_PAYLOAD_TYPE;
let sequenceBySsrc = new Map();
let parameterSetsBySsrc = new Map();
let senderReportStateBySsrc = new Map();
/**
 * Prepare only the DirectTransport Producer that injects RTP in the relay.
 * Router -> Producer -> Consumer behavior stays unchanged for receivers.
 */
async function setupPipeline(router, options) {
    if (!options?.encodings?.length) {
        throw new Error('WebCodecs pipeline requires at least one encoding');
    }
    const ssrcs = new Set(options.encodings.map(encoding => encoding.ssrc));
    if (ssrcs.size !== options.encodings.length) {
        throw new Error('WebCodecs pipeline encodings must use distinct SSRCs');
    }
    resetPipelineState();
    sequenceBySsrc = new Map(options.encodings.map(encoding => [
        encoding.ssrc,
        Math.floor(Math.random() * 0xffff),
    ]));
    parameterSetsBySsrc = new Map(options.encodings.map(encoding => [
        encoding.ssrc,
        [],
    ]));
    senderReportStateBySsrc = new Map(options.encodings.map(encoding => [
        encoding.ssrc,
        {
            packetCount: 0,
            octetCount: 0,
            lastRtpTimestamp: undefined,
            lastCaptureTimestampUs: undefined,
        },
    ]));
    pipelinePayloadType = options.payloadType ?? H264_PAYLOAD_TYPE;
    directTransport = await router.createDirectTransport();
    try {
        await directTransport.connect();
        producer = await directTransport.produce({
            kind: 'video',
            rtpParameters: {
                codecs: [
                    {
                        mimeType: 'video/H264',
                        payloadType: pipelinePayloadType,
                        clockRate: H264_CLOCK_RATE,
                        parameters: {
                            'packetization-mode': 1,
                            'profile-level-id': options.profileLevelId ?? DEFAULT_H264_PROFILE_LEVEL_ID,
                            'level-asymmetry-allowed': 1,
                        },
                        rtcpFeedback: [
                            { type: 'nack', parameter: 'pli' },
                            { type: 'ccm', parameter: 'fir' },
                        ],
                    },
                ],
                encodings: options.encodings.map(encoding => ({
                    ssrc: encoding.ssrc,
                    scalabilityMode: encoding.scalabilityMode ?? 'L1T1',
                })),
                headerExtensions: [],
                rtcp: { cname: 'webcodecs-pipeline' },
            },
        });
    } catch (error) {
        resetPipelineState();
        throw error;
    }
    const activeProducer = producer;
    const activeDirectTransport = directTransport;
    activeProducer.observer.once('close', () => {
        if (producer !== activeProducer) {
            return;
        }
        clearSenderReportState();
        producer = undefined;
        if (directTransport === activeDirectTransport) {
            directTransport = undefined;
        }
        if (!activeDirectTransport.closed) {
            activeDirectTransport.close();
        }
    });
    activeDirectTransport.observer.once('close', () => {
        if (directTransport !== activeDirectTransport) {
            return;
        }
        clearSenderReportState();
        directTransport = undefined;
        if (producer === activeProducer) {
            producer = undefined;
            if (!activeProducer.closed) {
                activeProducer.close();
            }
        }
    });
    senderReportTimer = setInterval(sendSenderReports, 1000);
    // 진단 로그는 server.ts의 logger.debug에서 출력 (production/debug 정책 일원화)
    return producer;
}
function injectEncodedChunk(chunk, ssrc, metadata) {
    if (!producer) {
        throw new Error('WebCodecs pipeline is not initialized');
    }
    if (!sequenceBySsrc.has(ssrc)) {
        throw new Error(`WebCodecs pipeline SSRC is not configured: ${ssrc}`);
    }
    updateCachedParameterSets(ssrc, metadata);
    const rawChunk = toUint8Array(chunk.data);
    const chunkNalus = extractH264Nalus(rawChunk);
    if (chunkNalus.length === 0) {
        return;
    }
    if (!Number.isSafeInteger(chunk.timestamp)) {
        throw new Error(`Invalid WebCodecs capture timestamp: ${chunk.timestamp}`);
    }
    ensureCaptureClockAnchor(chunk.timestamp);
    // WebCodecs H264 key chunks may omit SPS/PPS, so prepend cached parameter
    // sets from decoderConfig before key frames while keeping RTP/H264 output.
    const parameterSets = parameterSetsBySsrc.get(ssrc) ?? [];
    const nalus = chunk.type === 'key' && parameterSets.length > 0
        ? [...parameterSets, ...chunkNalus]
        : chunkNalus;
    const rtpTimestamp = webCodecsTimestampToRtpTimestamp(chunk.timestamp);
    for (let i = 0; i < nalus.length; ++i) {
        const isLastNalu = i === nalus.length - 1;
        const packets = packetizeH264Nalu(nalus[i], {
            ssrc,
            payloadType: pipelinePayloadType,
            timestamp: rtpTimestamp,
            marker: isLastNalu,
        });
        for (const packet of packets) {
            const rtpPacket = Buffer.from(packet);
            producer.send(rtpPacket);
            recordSentRtpPacket(ssrc, rtpTimestamp, chunk.timestamp, rtpPacket);
        }
    }
}

function getPipelineStats() {
    let rtpPackets = 0;
    let payloadOctets = 0;
    const bySsrc = {};

    for (const [ssrc, state] of senderReportStateBySsrc) {
        const ssrcStats = {
            rtpPackets: state.packetCount,
            payloadOctets: state.octetCount,
        };
        bySsrc[ssrc] = ssrcStats;
        rtpPackets += ssrcStats.rtpPackets;
        payloadOctets += ssrcStats.payloadOctets;
    }

    return { rtpPackets, payloadOctets, bySsrc };
}

function recordSentRtpPacket(ssrc, rtpTimestamp, captureTimestampUs, packet) {
    const state = senderReportStateBySsrc.get(ssrc);

    if (!state) {
        throw new Error(`WebCodecs pipeline SSRC is not configured: ${ssrc}`);
    }
    if (
        packet.byteLength < RTP_HEADER_SIZE ||
        (packet[0] & 0xc0) !== RTP_VERSION ||
        (packet[0] & 0x10) !== 0 ||
        (packet[0] & 0x0f) !== 0
    ) {
        throw new Error('WebCodecs pipeline expects RTP packets with a basic 12-byte header');
    }
    if (!Number.isSafeInteger(captureTimestampUs)) {
        throw new Error(`Invalid WebCodecs capture timestamp: ${captureTimestampUs}`);
    }

    state.packetCount += 1;
    state.octetCount += packet.byteLength - RTP_HEADER_SIZE;
    state.lastRtpTimestamp = rtpTimestamp;
    state.lastCaptureTimestampUs = captureTimestampUs;
}

function ensureCaptureClockAnchor(mediaTimestampUs) {
    if (!captureClockAnchor) {
        captureClockAnchor = {
            mediaTimestampUs,
            unixTimeMs: Date.now(),
        };
    }
}

function sendSenderReports() {
    if (!directTransport || directTransport.closed || !captureClockAnchor) {
        return;
    }

    for (const [ssrc, state] of senderReportStateBySsrc) {
        if (
            state.packetCount === 0 ||
            state.lastRtpTimestamp === undefined ||
            state.lastCaptureTimestampUs === undefined
        ) {
            continue;
        }

        const report = buildSenderReport(ssrc, state);
        directTransport.sendRtcp(report);
    }
}

function buildSenderReport(ssrc, state) {
    const ntp = captureTimestampUsToNtp(state.lastCaptureTimestampUs);
    const report = Buffer.alloc(RTCP_SR_PACKET_SIZE);

    report[0] = RTP_VERSION;
    report[1] = RTCP_SR_PACKET_TYPE;
    report.writeUInt16BE(RTCP_SR_LENGTH, 2);
    report.writeUInt32BE(ssrc >>> 0, 4);
    report.writeUInt32BE(ntp.seconds, 8);
    report.writeUInt32BE(ntp.fraction, 12);
    report.writeUInt32BE(state.lastRtpTimestamp >>> 0, 16);
    report.writeUInt32BE(state.packetCount >>> 0, 20);
    report.writeUInt32BE(state.octetCount >>> 0, 24);

    return report;
}

function captureTimestampUsToNtp(mediaTimestampUs) {
    if (!captureClockAnchor) {
        throw new Error('WebCodecs pipeline capture clock anchor is not initialized');
    }

    const unixTimeUs =
        BigInt(captureClockAnchor.unixTimeMs) * 1000n +
        (BigInt(mediaTimestampUs) - BigInt(captureClockAnchor.mediaTimestampUs));

    return unixTimeUsToNtp(unixTimeUs);
}

function unixTimeUsToNtp(unixTimeUs) {
    const ntpTimeUs =
        unixTimeUs + NTP_EPOCH_OFFSET_SECONDS * MICROSECONDS_PER_SECOND;
    let seconds = ntpTimeUs / MICROSECONDS_PER_SECOND;
    let remainderUs = ntpTimeUs % MICROSECONDS_PER_SECOND;

    if (remainderUs < 0n) {
        seconds -= 1n;
        remainderUs += MICROSECONDS_PER_SECOND;
    }

    let fraction =
        (remainderUs * NTP_FRACTION_SCALE + 500000n) /
        MICROSECONDS_PER_SECOND;

    if (fraction >= NTP_FRACTION_SCALE) {
        seconds += 1n;
        fraction = 0n;
    }
    if (seconds < 0n || seconds > UINT32_MAX) {
        throw new Error(`NTP seconds are outside the 32-bit range: ${seconds}`);
    }

    return {
        seconds: Number(seconds),
        fraction: Number(fraction),
    };
}

function clearSenderReportState() {
    if (senderReportTimer) {
        clearInterval(senderReportTimer);
        senderReportTimer = undefined;
    }
    captureClockAnchor = undefined;
    senderReportStateBySsrc = new Map();
    sequenceBySsrc = new Map();
    parameterSetsBySsrc = new Map();
}

function resetPipelineState() {
    const previousProducer = producer;
    const previousDirectTransport = directTransport;

    clearSenderReportState();
    producer = undefined;
    directTransport = undefined;

    if (previousProducer && !previousProducer.closed) {
        previousProducer.close();
    }
    if (previousDirectTransport && !previousDirectTransport.closed) {
        previousDirectTransport.close();
    }
}
function packetizeH264Nalu(nalu, opts) {
    if (nalu.length <= H264_MAX_PAYLOAD_SIZE) {
        return [buildRtpPacket(nalu, opts)];
    }
    const packets = [];
    const naluType = nalu[0] & 0x1f;
    const naluHeader = nalu[0] & 0xe0;
    let offset = 1;
    while (offset < nalu.length) {
        const isFirst = offset === 1;
        const end = Math.min(offset + H264_MAX_PAYLOAD_SIZE - 2, nalu.length);
        const isLast = end === nalu.length;
        const fuHeader = (isFirst ? 0x80 : 0) | (isLast ? 0x40 : 0) | naluType;
        const payload = new Uint8Array(2 + end - offset);
        payload[0] = naluHeader | 28;
        payload[1] = fuHeader;
        payload.set(nalu.subarray(offset, end), 2);
        packets.push(buildRtpPacket(payload, { ...opts, marker: isLast && opts.marker }));
        offset = end;
    }
    return packets;
}
function buildRtpPacket(payload, opts) {
    const packet = new Uint8Array(12 + payload.length);
    const view = new DataView(packet.buffer, packet.byteOffset, packet.byteLength);
    view.setUint8(0, RTP_VERSION);
    view.setUint8(1, (opts.marker ? 0x80 : 0) | (opts.payloadType & 0x7f));
    const sequence = sequenceBySsrc.get(opts.ssrc);
    if (sequence === undefined) {
        throw new Error(`WebCodecs pipeline SSRC is not configured: ${opts.ssrc}`);
    }
    view.setUint16(2, sequence);
    view.setUint32(4, opts.timestamp);
    view.setUint32(8, opts.ssrc);
    packet.set(payload, 12);
    sequenceBySsrc.set(opts.ssrc, (sequence + 1) & 0xffff);
    return packet;
}
function webCodecsTimestampToRtpTimestamp(timestampUs) {
    // WebCodecs timestamps are microseconds, while RTP/H264 uses a 90 kHz clock.
    return Math.floor((timestampUs * H264_CLOCK_RATE) / 1_000_000) >>> 0;
}
function updateCachedParameterSets(ssrc, metadata) {
    const description = metadata?.decoderConfig?.description;
    if (!description) {
        return;
    }
    const parameterSets = extractParameterSetsFromAvcc(toUint8Array(description));
    if (parameterSets.length > 0) {
        parameterSetsBySsrc.set(ssrc, parameterSets);
    }
}
function extractH264Nalus(data) {
    // WebCodecs avc output is length-prefixed. Parse it first so a length
    // prefix such as 00 00 01 xx is not mistaken for an Annex-B start code.
    const lengthPrefixedNalus = splitLengthPrefixedNalus(data);
    if (lengthPrefixedNalus.length > 0) {
        return lengthPrefixedNalus;
    }
    const annexBNalus = splitAnnexB(data);
    return annexBNalus.length > 0 ? annexBNalus : data.length > 0 ? [data] : [];
}
function splitAnnexB(data) {
    const starts = [];
    for (let i = 0; i < data.length - 2; ++i) {
        if (data[i] === 0 && data[i + 1] === 0 && data[i + 2] === 1) {
            starts.push({ index: i, size: 3 });
            i += 2;
        }
        else if (i < data.length - 3 &&
            data[i] === 0 &&
            data[i + 1] === 0 &&
            data[i + 2] === 0 &&
            data[i + 3] === 1) {
            starts.push({ index: i, size: 4 });
            i += 3;
        }
    }
    return starts
        .map((start, idx) => {
        const naluStart = start.index + start.size;
        const naluEnd = starts[idx + 1]?.index ?? data.length;
        return data.subarray(naluStart, naluEnd);
    })
        .filter(nalu => nalu.length > 0);
}
function splitLengthPrefixedNalus(data) {
    const nalus = [];
    let offset = 0;
    while (offset + 4 <= data.length) {
        const length = (data[offset] << 24) |
            (data[offset + 1] << 16) |
            (data[offset + 2] << 8) |
            data[offset + 3];
        const naluStart = offset + 4;
        const naluEnd = naluStart + length;
        if (length <= 0 || naluEnd > data.length) {
            return [];
        }
        nalus.push(data.subarray(naluStart, naluEnd));
        offset = naluEnd;
    }
    return offset === data.length ? nalus : [];
}
function extractParameterSetsFromAvcc(description) {
    if (description.length < 7) {
        return [];
    }
    const parameterSets = [];
    let offset = 5;
    const spsCount = description[offset++] & 0x1f;
    for (let i = 0; i < spsCount; ++i) {
        if (offset + 2 > description.length) {
            return parameterSets;
        }
        const length = (description[offset] << 8) | description[offset + 1];
        offset += 2;
        if (offset + length > description.length) {
            return parameterSets;
        }
        parameterSets.push(description.subarray(offset, offset + length));
        offset += length;
    }
    if (offset >= description.length) {
        return parameterSets;
    }
    const ppsCount = description[offset++];
    for (let i = 0; i < ppsCount; ++i) {
        if (offset + 2 > description.length) {
            return parameterSets;
        }
        const length = (description[offset] << 8) | description[offset + 1];
        offset += 2;
        if (offset + length > description.length) {
            return parameterSets;
        }
        parameterSets.push(description.subarray(offset, offset + length));
        offset += length;
    }
    return parameterSets;
}
function toUint8Array(data) {
    if (data instanceof Uint8Array) {
        return data;
    }
    return new Uint8Array(data);
}
