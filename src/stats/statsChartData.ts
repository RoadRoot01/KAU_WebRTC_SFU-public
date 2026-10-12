export const MAX_RENDER_POINTS = 1600;

export type ChartPoint = { x: number; y: number | null };

export type ChartSeries = {
  label: string;
  color: string;
  data: ChartPoint[];
};

export type ChartDefinition = {
  title: string;
  unit: string;
  series: ChartSeries[];
};

export function createElapsedSeconds(
  measurementStartTime: string,
  timestamps: readonly string[]
): number[] {
  const startTime = Date.parse(measurementStartTime);
  if (!Number.isFinite(startTime)) {
    throw new Error('The measurement start time is invalid, so elapsed-time charts cannot be created.');
  }

  let previousTime = startTime - 1;
  return timestamps.map(timestamp => {
    const sampleTime = Date.parse(timestamp);
    if (
      !Number.isFinite(sampleTime) ||
      sampleTime < startTime ||
      sampleTime <= previousTime
    ) {
      throw new Error(
        'Sample timestamps must be valid, unique, and increasing to plot real elapsed time.'
      );
    }
    previousTime = sampleTime;
    return (sampleTime - startTime) / 1000;
  });
}

export function createChartSeries(
  label: string,
  color: string,
  elapsedSeconds: readonly number[],
  values: readonly (number | null)[]
): ChartSeries {
  return {
    label,
    color,
    data: elapsedSeconds.map((x, index) => ({
      x,
      y: typeof values[index] === 'number' && Number.isFinite(values[index])
        ? values[index]!
        : null
    }))
  };
}

export function decimateChartSeries(
  series: ChartSeries,
  maximumPoints = MAX_RENDER_POINTS
): ChartSeries {
  if (series.data.length <= maximumPoints) {
    return { ...series, data: [...series.data] };
  }

  const segments: ChartPoint[][] = [];
  const separators: ChartPoint[] = [];
  let currentSegment: ChartPoint[] = [];
  let pendingGap: ChartPoint | undefined;

  for (const point of series.data) {
    if (point.y === null || !Number.isFinite(point.y)) {
      if (currentSegment.length > 0) {
        segments.push(currentSegment);
        currentSegment = [];
      }
      if (segments.length > 0 && pendingGap === undefined) {
        pendingGap = { x: point.x, y: null };
      }
      continue;
    }

    if (currentSegment.length === 0 && segments.length > 0) {
      separators.push(pendingGap ?? { x: point.x, y: null });
      pendingGap = undefined;
    }
    currentSegment.push(point);
  }
  if (currentSegment.length > 0) {
    segments.push(currentSegment);
  }

  if (segments.length === 0) {
    return {
      ...series,
      data: series.data.length > 1
        ? [series.data[0]!, series.data.at(-1)!]
        : [...series.data]
    };
  }

  const pointBudgetPerSegment = Math.max(
    4,
    Math.floor((maximumPoints - separators.length) / segments.length)
  );
  const output: ChartPoint[] = [];
  for (let segmentIndex = 0; segmentIndex < segments.length; segmentIndex += 1) {
    const segment = segments[segmentIndex]!;
    if (segmentIndex > 0) {
      output.push(separators[segmentIndex - 1] ?? { x: segment[0]!.x, y: null });
    }
    if (segment.length <= pointBudgetPerSegment) {
      output.push(...segment);
      continue;
    }

    const bucketCount = Math.max(1, Math.floor(pointBudgetPerSegment / 4));
    const selected = new Set<number>([0, segment.length - 1]);
    for (let bucketIndex = 0; bucketIndex < bucketCount; bucketIndex += 1) {
      const bucketStart = Math.floor(bucketIndex * segment.length / bucketCount);
      const bucketEnd = Math.max(
        bucketStart + 1,
        Math.floor((bucketIndex + 1) * segment.length / bucketCount)
      );
      let minimumIndex = bucketStart;
      let maximumIndex = bucketStart;
      for (let index = bucketStart + 1; index < bucketEnd; index += 1) {
        if (segment[index]!.y! < segment[minimumIndex]!.y!) {
          minimumIndex = index;
        }
        if (segment[index]!.y! > segment[maximumIndex]!.y!) {
          maximumIndex = index;
        }
      }
      selected.add(minimumIndex);
      selected.add(maximumIndex);
    }
    output.push(...[...selected].sort((a, b) => a - b).map(index => segment[index]!));
  }

  return { ...series, data: output };
}

export function formatElapsedTime(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) {
    return 'n/a';
  }
  const wholeSeconds = Math.floor(seconds);
  const hours = Math.floor(wholeSeconds / 3600);
  const minutes = Math.floor(wholeSeconds % 3600 / 60);
  const remainder = wholeSeconds % 60;
  const twoDigits = (value: number): string => String(value).padStart(2, '0');
  return hours > 0
    ? `${twoDigits(hours)}:${twoDigits(minutes)}:${twoDigits(remainder)}`
    : `${twoDigits(minutes)}:${twoDigits(remainder)}`;
}
