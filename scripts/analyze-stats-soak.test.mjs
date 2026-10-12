import assert from 'node:assert/strict';
import test from 'node:test';
import { reportMarkdown } from './analyze-stats-soak.mjs';

test('report.md lists actual Chart.js and SVG results for each participant', () => {
  const markdown = reportMarkdown({
    runId: 'fixture-run',
    durationMinutes: 3,
    visualizer: {
      chartJs: {
        status: 'passed',
        participants: {
          producer: { status: 'passed', canvasCount: 5, renderMs: 253 },
          consumer1: { status: 'passed', canvasCount: 6, renderMs: 68 },
          consumer2: { status: 'passed', canvasCount: 6, renderMs: 78 }
        }
      },
      svgFallback: {
        status: 'passed',
        participants: {
          producer: { status: 'passed', svgCount: 5, renderMs: 139 },
          consumer1: { status: 'passed', svgCount: 6, renderMs: 42 },
          consumer2: { status: 'passed', svgCount: 6, renderMs: 38 }
        }
      }
    }
  });

  assert.match(markdown, /Final chart rendering results:\n- producer: Chart\.js passed \(5 canvases, 253 ms\); SVG fallback passed \(5 charts, 139 ms\)/);
  assert.match(markdown, /- consumer1: Chart\.js passed \(6 canvases, 68 ms\); SVG fallback passed \(6 charts, 42 ms\)/);
  assert.match(markdown, /- consumer2: Chart\.js passed \(6 canvases, 78 ms\); SVG fallback passed \(6 charts, 38 ms\)/);
  assert.doesNotMatch(markdown, /Final chart rendering results: `\{\}`/);
});

test('report.md describes missing participant results without emitting an empty object', () => {
  const markdown = reportMarkdown({ runId: 'fixture-run', durationMinutes: 3 });
  assert.match(markdown, /- No per-participant renderer results recorded\./);
  assert.doesNotMatch(markdown, /Final chart rendering results: `\{\}`/);
});
