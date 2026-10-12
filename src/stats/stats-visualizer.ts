import type {
  ConsumerStatsSample,
  ProducerStatsSample,
  StatsDump,
  StatsLayer,
  StatsRole
} from './statsDump.js';
import {
  createChartSeries,
  createElapsedSeconds,
  decimateChartSeries,
  formatElapsedTime,
  type ChartDefinition,
  type ChartSeries
} from './statsChartData.js';
import './stats-visualizer.css';

const SVG_NS = 'http://www.w3.org/2000/svg';
const LAYERS: readonly StatsLayer[] = ['low', 'mid', 'high'];
const COLORS = ['#087f9d', '#0b8f64', '#9a55b5', '#d27616'];
const LAYER_COLORS: Record<StatsLayer, string> = {
  low: '#087f9d',
  mid: '#0b8f64',
  high: '#9a55b5'
};
const fileInput = document.querySelector<HTMLInputElement>('#stats-file')!;
const loadError = document.querySelector<HTMLParagraphElement>('#load-error')!;
const loadStatus = document.querySelector<HTMLParagraphElement>('#load-status')!;
const dashboard = document.querySelector<HTMLElement>('#dashboard')!;
const dumpTitle = document.querySelector<HTMLElement>('#dump-title')!;
const metadataList = document.querySelector<HTMLDListElement>('#metadata')!;
const metrics = document.querySelector<HTMLElement>('#metrics')!;
const rendererStatus = document.querySelector<HTMLElement>('#renderer-status')!;
const charts = document.querySelector<HTMLElement>('#charts')!;

let loadGeneration = 0;
let chartInstances: Array<{ destroy: () => void }> = [];

fileInput.addEventListener('change', () => {
  const file = fileInput.files?.[0];
  if (file) {
    void loadDump(file);
  }
});
document.documentElement.dataset.statsVisualizerReady = 'true';

async function loadDump(file: File): Promise<void> {
  const generation = ++loadGeneration;
  destroyCurrentCharts();
  charts.replaceChildren();
  metadataList.replaceChildren();
  metrics.replaceChildren();
  rendererStatus.textContent = '';
  loadError.hidden = true;
  loadError.textContent = '';
  dashboard.hidden = true;
  loadStatus.textContent = `Loading ${file.name}…`;

  try {
    let parsed: unknown;
    try {
      parsed = JSON.parse(await file.text());
    } catch {
      throw new Error('The selected file is not valid JSON.');
    }
    if (generation !== loadGeneration) {
      return;
    }

    const dump = validateDump(parsed);
    const definitions = createChartDefinitions(dump);
    renderSummary(dump);
    dashboard.hidden = false;
    rendererStatus.textContent = 'Renderer: loading Chart.js…';
    await renderCharts(definitions, generation);
    if (generation === loadGeneration) {
      loadStatus.textContent = `Loaded ${dump.metadata.role} dump from ${file.name}.`;
    }
  } catch (error) {
    if (generation !== loadGeneration) {
      return;
    }
    loadError.textContent = error instanceof Error
      ? error.message
      : 'The selected file could not be read.';
    loadError.hidden = false;
    loadStatus.textContent = 'No valid file loaded.';
  }
}

function validateDump(value: unknown): StatsDump {
  if (!isRecord(value)) {
    throw new Error('The JSON root must be an object.');
  }
  if (value.schemaVersion !== 1) {
    throw new Error('Unsupported stats schema. This page supports schemaVersion 1.');
  }
  if (!isRecord(value.metadata)) {
    throw new Error('The stats metadata is missing or invalid.');
  }

  const metadata = value.metadata;
  if (metadata.role !== 'producer' && metadata.role !== 'consumer') {
    throw new Error('The stats role must be producer or consumer.');
  }
  if (
    typeof metadata.peerId !== 'string' ||
    typeof metadata.measurementStartTime !== 'string' ||
    typeof metadata.dumpTime !== 'string' ||
    !isFiniteNumber(metadata.sampleIntervalMs) ||
    typeof metadata.userAgent !== 'string' ||
    typeof metadata.simulcastEnabled !== 'boolean' ||
    !Array.isArray(metadata.availableLayers) ||
    !metadata.availableLayers.every(isStatsLayer) ||
    (metadata.collectionStatus !== undefined &&
      metadata.collectionStatus !== 'recording' &&
      metadata.collectionStatus !== 'stopped' &&
      metadata.collectionStatus !== 'limit-reached') ||
    (metadata.measurementEndTime !== undefined &&
      typeof metadata.measurementEndTime !== 'string')
  ) {
    throw new Error('The stats metadata does not match schemaVersion 1.');
  }
  if (!Array.isArray(value.producerStats) || !Array.isArray(value.consumerStats)) {
    throw new Error('Producer and Consumer stats must be arrays.');
  }

  const selectedSamples = metadata.role === 'producer'
    ? value.producerStats
    : value.consumerStats;
  if (!selectedSamples.every(sample => isRecord(sample) && typeof sample.timestamp === 'string')) {
    throw new Error(`The ${metadata.role} stats array contains an invalid sample timestamp.`);
  }

  return value as unknown as StatsDump;
}

function createChartDefinitions(dump: StatsDump): ChartDefinition[] {
  const samples = dump.metadata.role === 'producer'
    ? dump.producerStats
    : dump.consumerStats;
  const elapsed = createElapsedSeconds(
    dump.metadata.measurementStartTime,
    samples.map(sample => sample.timestamp)
  );

  if (dump.metadata.role === 'producer') {
    return buildProducerCharts(dump.producerStats, dump.metadata.availableLayers, elapsed);
  }
  return buildConsumerCharts(dump.consumerStats, elapsed);
}

function buildProducerCharts(
  samples: ProducerStatsSample[],
  availableLayers: StatsLayer[],
  elapsed: number[]
): ChartDefinition[] {
  const activeLayers = LAYERS.filter(layer => availableLayers.includes(layer));
  const make = (
    label: string,
    color: string,
    values: Array<number | null>
  ): ChartSeries => createChartSeries(label, color, elapsed, values);

  return [
    {
      title: 'Source and encoded FPS',
      unit: 'FPS',
      series: [
        make('Source FPS', COLORS[0]!, samples.map(sample => finiteOrNull(sample.sourceFps))),
        ...activeLayers.map(layer => make(
          `${layer.toUpperCase()} encoded FPS`,
          LAYER_COLORS[layer],
          samples.map(sample => finiteOrNull(sample.layers?.[layer]?.encodedFps))
        ))
      ]
    },
    {
      title: 'Encoded payload bitrate',
      unit: 'Kbps',
      series: activeLayers.map(layer => make(
        `${layer.toUpperCase()} payload`,
        LAYER_COLORS[layer],
        samples.map(sample => finiteOrNull(sample.layers?.[layer]?.encodedPayloadBitrateKbps))
      ))
    },
    {
      title: 'Cumulative encoded payload',
      unit: 'Bytes',
      series: activeLayers.map(layer => make(
        `${layer.toUpperCase()} payload`,
        LAYER_COLORS[layer],
        samples.map(sample => finiteOrNull(sample.layers?.[layer]?.encodedPayloadBytesTotal))
      ))
    },
    {
      title: 'Key and delta frames per sample',
      unit: 'Frames / sample',
      series: activeLayers.flatMap(layer => ([
        make(
          `${layer.toUpperCase()} key frames`,
          LAYER_COLORS[layer],
          samples.map(sample => finiteOrNull(sample.layers?.[layer]?.keyFramesDelta))
        ),
        make(
          `${layer.toUpperCase()} delta frames`,
          COLORS[(LAYERS.indexOf(layer) + 1) % COLORS.length]!,
          samples.map(sample => finiteOrNull(sample.layers?.[layer]?.deltaFramesDelta))
        )
      ]))
    },
    {
      title: 'Encoder queue size',
      unit: 'Frames',
      series: activeLayers.map(layer => make(
        `${layer.toUpperCase()} queue`,
        LAYER_COLORS[layer],
        samples.map(sample => finiteOrNull(sample.layers?.[layer]?.encodeQueueSize))
      ))
    }
  ];
}

function buildConsumerCharts(
  samples: ConsumerStatsSample[],
  elapsed: number[]
): ChartDefinition[] {
  const make = (
    label: string,
    color: string,
    values: Array<number | null>
  ): ChartSeries => createChartSeries(label, color, elapsed, values);
  return [
    {
      title: 'Received and decoded FPS',
      unit: 'FPS',
      series: [
        make('Received FPS', COLORS[0]!, samples.map(sample => finiteOrNull(sample.receivedFps))),
        make('Decoded FPS', COLORS[1]!, samples.map(sample => finiteOrNull(sample.decodedFps)))
      ]
    },
    {
      title: 'Received media bitrate',
      unit: 'Kbps',
      series: [make(
        'Received media',
        COLORS[2]!,
        samples.map(sample => finiteOrNull(sample.receivedMediaBitrateKbps))
      )]
    },
    {
      title: 'Packets received and lost',
      unit: 'Packets',
      series: [
        make('Packets received', COLORS[0]!, samples.map(sample => finiteOrNull(sample.packetsReceived))),
        make('Packets lost', COLORS[3]!, samples.map(sample => finiteOrNull(sample.packetsLost)))
      ]
    },
    {
      title: 'Cumulative bytes received',
      unit: 'Bytes',
      series: [make(
        'Bytes received',
        COLORS[0]!,
        samples.map(sample => finiteOrNull(sample.bytesReceived))
      )]
    },
    {
      title: 'Decoded and dropped frames',
      unit: 'Frames',
      series: [
        make('Frames decoded', COLORS[1]!, samples.map(sample => finiteOrNull(sample.framesDecoded))),
        make('Frames dropped', COLORS[3]!, samples.map(sample => finiteOrNull(sample.framesDropped)))
      ]
    },
    {
      title: 'Maximum inbound jitter',
      unit: 'Milliseconds',
      series: [make(
        'Jitter',
        COLORS[2]!,
        samples.map(sample => finiteOrNull(sample.maxJitterMs))
      )]
    }
  ];
}

function renderSummary(dump: StatsDump): void {
  const { metadata } = dump;
  dumpTitle.textContent = `${capitalize(metadata.role)} · peer ${metadata.peerId}`;
  addMetadata('Role', metadata.role);
  addMetadata('Peer ID', metadata.peerId);
  addMetadata('Measurement start', formatDate(metadata.measurementStartTime));
  addMetadata('Dump time', formatDate(metadata.dumpTime));
  addMetadata('Sample target', `${formatNumber(metadata.sampleIntervalMs, 0)} ms`);
  addMetadata('Simulcast', metadata.simulcastEnabled ? 'On' : 'Off');
  addMetadata('Available layers', metadata.availableLayers.length
    ? metadata.availableLayers.map(layer => layer.toUpperCase()).join(', ')
    : 'n/a');
  addMetadata('Browser', metadata.userAgent);
  if (metadata.collectionStatus !== undefined) {
    addMetadata('Collection status', metadata.collectionStatus);
  }
  if (metadata.measurementEndTime !== undefined) {
    addMetadata('Measurement end', formatDate(metadata.measurementEndTime));
  }

  if (metadata.role === 'producer') {
    renderProducerSummary(dump.producerStats, metadata.availableLayers);
  } else {
    renderConsumerSummary(dump.consumerStats);
  }
}

function renderProducerSummary(
  samples: ProducerStatsSample[],
  availableLayers: StatsLayer[]
): void {
  const last = samples.at(-1);
  const activeLayers = LAYERS.filter(layer => availableLayers.includes(layer));
  addMetric('Samples', formatNumber(samples.length, 0));
  addMetric('Source FPS', formatNumber(last?.sourceFps, 1));
  addMetric('Source frames', formatNumber(last?.sourceFramesTotal, 0));
  addMetric('Socket buffered amount', formatBytes(last?.socketBufferedAmountBytes));
  addMetric('Browser JS Heap', formatMegabytes(last?.browserJsHeapUsedBytes));
  for (const layer of activeLayers) {
    const sample = last?.layers?.[layer];
    const prefix = layer.toUpperCase();
    addMetric(`${prefix} encoded FPS`, formatNumber(sample?.encodedFps, 1));
    addMetric(`${prefix} encoded payload bitrate`, formatKbps(sample?.encodedPayloadBitrateKbps));
    addMetric(`${prefix} cumulative payload`, formatBytes(sample?.encodedPayloadBytesTotal));
    addMetric(`${prefix} key / delta frames`, sample
      ? `${formatNumber(sample.keyFramesTotal, 0)} / ${formatNumber(sample.deltaFramesTotal, 0)}`
      : 'n/a');
    addMetric(`${prefix} encode queue`, formatNumber(sample?.encodeQueueSize, 0));
    addMetric(`${prefix} dimensions`, sample?.width && sample.height
      ? `${formatNumber(sample.width, 0)} × ${formatNumber(sample.height, 0)}`
      : 'n/a');
  }
}

function renderConsumerSummary(samples: ConsumerStatsSample[]): void {
  const last = samples.at(-1);
  addMetric('Samples', formatNumber(samples.length, 0));
  addMetric('Received media bitrate', formatKbps(last?.receivedMediaBitrateKbps));
  addMetric('Received FPS', formatNumber(last?.receivedFps, 1));
  addMetric('Decoded FPS', formatNumber(last?.decodedFps, 1));
  addMetric('Packets received', formatNumber(last?.packetsReceived, 0));
  addMetric('Packets lost', formatNumber(last?.packetsLost, 0));
  addMetric('Maximum inbound jitter', formatMilliseconds(last?.maxJitterMs));
  addMetric('Cumulative bytes received', formatBytes(last?.bytesReceived));
  addMetric('Frames received', formatNumber(last?.framesReceived, 0));
  addMetric('Frames decoded', formatNumber(last?.framesDecoded, 0));
  addMetric('Frames dropped', formatNumber(last?.framesDropped, 0));
  addMetric('Browser JS Heap', formatMegabytes(last?.browserJsHeapUsedBytes));
}

async function renderCharts(
  definitions: ChartDefinition[],
  generation: number
): Promise<void> {
  const displayDefinitions = definitions.map(definition => ({
    ...definition,
    series: definition.series.map(series => decimateChartSeries(series))
  }));
  try {
    const chartModule = await import('./statsChartJs.js');
    if (generation !== loadGeneration) {
      return;
    }
    for (const definition of displayDefinitions) {
      chartInstances.push(chartModule.renderChartJsChart(charts, definition));
    }
    if (generation === loadGeneration) {
      rendererStatus.textContent = 'Renderer: Chart.js';
    }
  } catch {
    if (generation !== loadGeneration) {
      return;
    }
    destroyCurrentCharts();
    charts.replaceChildren();
    for (const definition of displayDefinitions) {
      renderSvgChart(charts, definition);
    }
    rendererStatus.textContent = 'Renderer: SVG (Chart.js unavailable)';
  }
}

function destroyCurrentCharts(): void {
  for (const chart of chartInstances) {
    try {
      chart.destroy();
    } catch {
      // A failed Chart.js initialization may leave a partially created instance.
    }
  }
  chartInstances = [];
}

function renderSvgChart(container: HTMLElement, definition: ChartDefinition): void {
  const card = document.createElement('article');
  card.className = 'chart-card';
  const heading = document.createElement('h4');
  heading.textContent = `${definition.title} (${definition.unit})`;
  card.append(heading);

  const allValues: number[] = [];
  for (const item of definition.series) {
    for (const point of item.data) {
      if (point.y !== null && Number.isFinite(point.y)) {
        allValues.push(point.y);
      }
    }
  }
  if (allValues.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'chart-empty';
    empty.textContent = 'No numeric samples for this chart.';
    card.append(empty);
    container.append(card);
    return;
  }

  let minimum = 0;
  let maximum = 0;
  for (const value of allValues) {
    if (value < minimum) {
      minimum = value;
    }
    if (value > maximum) {
      maximum = value;
    }
  }
  if (minimum === maximum) {
    maximum = minimum + 1;
  }
  const valueRange = maximum - minimum;
  const svg = document.createElementNS(SVG_NS, 'svg');
  svg.setAttribute('viewBox', '0 0 760 250');
  svg.setAttribute('role', 'img');
  svg.setAttribute('aria-label', `${definition.title} · ${definition.unit}`);
  const left = 58;
  const right = 742;
  const top = 18;
  const bottom = 194;
  const xMaximum = definition.series.reduce((currentMaximum, item) => {
    const last = item.data.at(-1)?.x ?? 0;
    return Math.max(currentMaximum, last);
  }, 1);

  for (let tick = 0; tick <= 4; tick += 1) {
    const ratio = tick / 4;
    const y = bottom - (bottom - top) * ratio;
    const grid = document.createElementNS(SVG_NS, 'line');
    grid.setAttribute('x1', String(left));
    grid.setAttribute('x2', String(right));
    grid.setAttribute('y1', String(y));
    grid.setAttribute('y2', String(y));
    grid.setAttribute('class', 'chart-grid-line');
    svg.append(grid);

    const yLabel = document.createElementNS(SVG_NS, 'text');
    yLabel.setAttribute('x', String(left - 8));
    yLabel.setAttribute('y', String(y + 3));
    yLabel.setAttribute('text-anchor', 'end');
    yLabel.setAttribute('class', 'chart-axis-label');
    yLabel.textContent = formatNumber(minimum + valueRange * ratio, 1);
    svg.append(yLabel);

    const x = left + (right - left) * ratio;
    const xLabel = document.createElementNS(SVG_NS, 'text');
    xLabel.setAttribute('x', String(x));
    xLabel.setAttribute('y', String(bottom + 20));
    xLabel.setAttribute('text-anchor', tick === 0 ? 'start' : tick === 4 ? 'end' : 'middle');
    xLabel.setAttribute('class', 'chart-axis-label');
    xLabel.textContent = formatElapsedTime(xMaximum * ratio);
    svg.append(xLabel);
  }

  const xAxisLabel = document.createElementNS(SVG_NS, 'text');
  xAxisLabel.setAttribute('x', String((left + right) / 2));
  xAxisLabel.setAttribute('y', '244');
  xAxisLabel.setAttribute('text-anchor', 'middle');
  xAxisLabel.setAttribute('class', 'chart-axis-title');
  xAxisLabel.textContent = 'Elapsed time (MM:SS)';
  svg.append(xAxisLabel);

  const legend = document.createElement('div');
  legend.className = 'chart-legend';
  definition.series.forEach(series => {
    const key = document.createElement('span');
    const swatch = document.createElement('i');
    swatch.className = 'legend-swatch';
    swatch.style.backgroundColor = series.color;
    key.append(swatch, document.createTextNode(`${series.label} (${definition.unit})`));
    legend.append(key);

    let pathPoints: string[] = [];
    const appendSegment = (): void => {
      if (pathPoints.length === 1) {
        const [xText, yText] = pathPoints[0]!.split(',');
        const dot = document.createElementNS(SVG_NS, 'circle');
        dot.setAttribute('cx', xText!);
        dot.setAttribute('cy', yText!);
        dot.setAttribute('r', '2.5');
        dot.setAttribute('fill', series.color);
        dot.setAttribute('data-series-label', series.label);
        svg.append(dot);
      } else if (pathPoints.length > 1) {
        const line = document.createElementNS(SVG_NS, 'polyline');
        line.setAttribute('points', pathPoints.join(' '));
        line.setAttribute('fill', 'none');
        line.setAttribute('stroke', series.color);
        line.setAttribute('stroke-width', '2.5');
        line.setAttribute('stroke-linejoin', 'round');
        line.setAttribute('stroke-linecap', 'round');
        line.setAttribute('data-series-label', series.label);
        const title = document.createElementNS(SVG_NS, 'title');
        title.textContent = `${series.label} · ${definition.unit}`;
        line.append(title);
        svg.append(line);
      }
      pathPoints = [];
    };

    for (const point of series.data) {
      if (point.y === null || !Number.isFinite(point.y)) {
        appendSegment();
        continue;
      }
      const x = left + (right - left) * point.x / xMaximum;
      const y = bottom - (bottom - top) * (point.y - minimum) / valueRange;
      pathPoints.push(`${x.toFixed(2)},${y.toFixed(2)}`);
    }
    appendSegment();
  });

  card.append(svg, legend);
  container.append(card);
}

function addMetric(label: string, value: string): void {
  const card = document.createElement('div');
  card.className = 'metric-card';
  const name = document.createElement('span');
  name.className = 'metric-label';
  name.textContent = label;
  const content = document.createElement('strong');
  content.className = 'metric-value';
  content.textContent = value;
  card.append(name, content);
  metrics.append(card);
}

function addMetadata(label: string, value: string): void {
  const item = document.createElement('div');
  const term = document.createElement('dt');
  term.textContent = label;
  const detail = document.createElement('dd');
  detail.textContent = value;
  item.append(term, detail);
  metadataList.append(item);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function isStatsLayer(value: unknown): value is StatsLayer {
  return value === 'low' || value === 'mid' || value === 'high';
}

function finiteOrNull(value: unknown): number | null {
  return isFiniteNumber(value) ? value : null;
}

function formatNumber(value: unknown, digits: number): string {
  return isFiniteNumber(value)
    ? new Intl.NumberFormat(undefined, { maximumFractionDigits: digits }).format(value)
    : 'n/a';
}

function formatKbps(value: unknown): string {
  return isFiniteNumber(value) ? `${formatNumber(value, 1)} Kbps` : 'n/a';
}

function formatMilliseconds(value: unknown): string {
  return isFiniteNumber(value) ? `${formatNumber(value, 2)} ms` : 'n/a';
}

function formatBytes(value: unknown): string {
  return isFiniteNumber(value) ? `${formatNumber(value, 0)} B` : 'n/a';
}

function formatMegabytes(value: unknown): string {
  return isFiniteNumber(value)
    ? `${formatNumber(value / (1024 * 1024), 2)} MB`
    : 'n/a';
}

function formatDate(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? 'n/a' : date.toLocaleString();
}

function capitalize(value: StatsRole): string {
  return value === 'producer' ? 'Producer' : 'Consumer';
}
