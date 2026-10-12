import Chart from 'chart.js/auto';
import type { Chart as ChartInstanceType, ChartConfiguration } from 'chart.js';
import { formatElapsedTime, type ChartDefinition, type ChartPoint } from './statsChartData.js';

export type StatsChartInstance = ChartInstanceType<'line', ChartPoint[], number>;

export function renderChartJsChart(
  container: HTMLElement,
  definition: ChartDefinition
): StatsChartInstance {
  const card = document.createElement('article');
  card.className = 'chart-card';
  const heading = document.createElement('h4');
  heading.textContent = definition.title;
  const canvas = document.createElement('canvas');
  canvas.className = 'chart-canvas';
  canvas.setAttribute('role', 'img');
  canvas.setAttribute('aria-label', `${definition.title} · ${definition.unit}`);
  card.append(heading, canvas);
  container.append(card);

  const configuration: ChartConfiguration<'line', ChartPoint[], number> = {
    type: 'line',
    data: {
      datasets: definition.series.map(series => ({
        label: series.label,
        data: series.data,
        borderColor: series.color,
        backgroundColor: series.color,
        borderWidth: 2,
        pointRadius: 0,
        pointHoverRadius: 3,
        pointHitRadius: 8,
        spanGaps: false,
        fill: false,
        tension: 0
      }))
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      parsing: false,
      // Each dataset is peak-decimated independently, so datasets can have
      // different x values. Keep parsing disabled but do not claim shared indexes.
      normalized: false,
      interaction: { mode: 'nearest', axis: 'x', intersect: false },
      plugins: {
        legend: { display: true, position: 'bottom' },
        tooltip: {
          callbacks: {
            title(items) {
              const elapsed = items[0]?.parsed.x;
              return typeof elapsed !== 'number' || !Number.isFinite(elapsed)
                ? 'Elapsed time'
                : `Elapsed ${formatElapsedTime(elapsed)}`;
            },
            label(item) {
              const value = item.parsed.y;
              if (typeof value !== 'number' || !Number.isFinite(value)) {
                return `${item.dataset.label}: n/a`;
              }
              return `${item.dataset.label}: ${formatNumber(value)} ${definition.unit}`;
            }
          }
        }
      },
      scales: {
        x: {
          type: 'linear',
          title: { display: true, text: 'Elapsed time (MM:SS)' },
          ticks: {
            maxTicksLimit: 8,
            callback(value) {
              return formatElapsedTime(Number(value));
            }
          }
        },
        y: {
          beginAtZero: true,
          title: { display: true, text: definition.unit }
        }
      }
    }
  };

  return new Chart(canvas, configuration);
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value);
}
