declare module '*webCodecsPipeline.js' {
  import type * as MediasoupTypes from '../../mediasoup/node/lib/types.js';

  type PipelineEncoding = {
    ssrc: number;
    scalabilityMode?: string;
  };

  type PipelineOptions = {
    encodings: readonly PipelineEncoding[];
    payloadType?: number;
    profileLevelId?: string;
  };

  type EncodedChunk = {
    data: Uint8Array;
    timestamp: number;
    type: 'key' | 'delta';
    duration?: number;
  };

  type ChunkMetadata = {
    decoderConfig?: {
      description?: Uint8Array;
    };
  };

  type PipelineSsrcStats = {
    rtpPackets: number;
    payloadOctets: number;
  };

  type PipelineStats = {
    rtpPackets: number;
    payloadOctets: number;
    bySsrc: Record<number, PipelineSsrcStats>;
  };

  export function setupPipeline(
    router: MediasoupTypes.Router,
    options: PipelineOptions
  ): Promise<MediasoupTypes.Producer>;

  export function injectEncodedChunk(
    chunk: EncodedChunk,
    ssrc: number,
    metadata?: ChunkMetadata
  ): void;

  export function getPipelineStats(): PipelineStats;
}
