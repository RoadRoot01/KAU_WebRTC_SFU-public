import * as os from 'node:os';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as process from 'node:process';
import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import * as mediasoup from '../../mediasoup/node/lib/index.js';
import pidusage from 'pidusage';

import { logger } from './serverLogger.js';

const moduleDir = path.dirname(fileURLToPath(import.meta.url));

interface CpuSnapshot {
	idle: number;
	total: number;
}

interface HardwareQueryResult {
	gpuPct: number | null;
	cpuTempC: number | null;
	batteryPct: number | null;
	chromeCpuSec: number | null;
	chromeRamMb: number;
	chromeProcsCount: number;
}

function getCpuSnapshot(): CpuSnapshot {
	const cpus = os.cpus();
	let idle = 0;
	let total = 0;

	for (const cpu of cpus) {
		for (const [type, time] of Object.entries(cpu.times)) {
			total += time;

			if (type === 'idle') {
				idle += time;
			}
		}
	}

	return { idle, total };
}

function calculateCpuPercent(prev: CpuSnapshot, curr: CpuSnapshot): number {
	const idleDelta = curr.idle - prev.idle;
	const totalDelta = curr.total - prev.total;

	if (totalDelta <= 0) {
		return 0;
	}

	const usage = 100 - (idleDelta / totalDelta) * 100;

	return Math.max(0, Math.min(100, Math.round(usage * 10) / 10));
}

async function queryHardware(): Promise<HardwareQueryResult> {
	if (process.platform === 'win32') {
		return queryWindowsHardware();
	}

	let cpuTempC: number | null = null;
	let batteryPct: number | null = null;

	try {
		if (fs.existsSync('/sys/class/thermal/thermal_zone0/temp')) {
			const raw = fs.readFileSync(
				'/sys/class/thermal/thermal_zone0/temp',
				'utf8'
			);

			cpuTempC = Math.round((parseInt(raw, 10) / 1000) * 10) / 10;
		}
	} catch {}

	try {
		if (fs.existsSync('/sys/class/power_supply/BAT0/capacity')) {
			const raw = fs.readFileSync(
				'/sys/class/power_supply/BAT0/capacity',
				'utf8'
			);

			batteryPct = parseInt(raw, 10);
		}
	} catch {}

	return {
		gpuPct: null,
		cpuTempC,
		batteryPct,
		chromeCpuSec: null,
		chromeRamMb: 0,
		chromeProcsCount: 0,
	};
}

async function queryWindowsHardware(): Promise<HardwareQueryResult> {
	return new Promise(resolve => {
		const script = [
			'$cp = Get-Process chrome -ErrorAction SilentlyContinue',
			'$b = (Get-CimInstance Win32_Battery -ErrorAction SilentlyContinue).EstimatedChargeRemaining',
			'$t = (Get-CimInstance Win32_PerfFormattedData_Counters_ThermalZoneInformation -ErrorAction SilentlyContinue | Select-Object -First 1).HighPrecisionTemperature',
			'$g = (Get-CimInstance Win32_PerfFormattedData_GPUPerformanceCounters_GPUEngine -ErrorAction SilentlyContinue | Measure-Object -Property UtilizationPercentage -Sum).Sum',
			'$cWs = if ($cp) { [math]::Round(($cp | Measure-Object -Property WorkingSet64 -Sum).Sum / 1MB, 1) } else { 0 }',
			'$cCpu = if ($cp) { [math]::Round(($cp | Measure-Object -Property CPU -Sum).Sum, 2) } else { 0 }',
			'$cCount = if ($cp) { $cp.Count } else { 0 }',
			'@{ Battery=$b; HighPrecisionTemp=$t; Gpu=$g; ChromeCpuSec=$cCpu; ChromeRamMb=$cWs; ChromeCount=$cCount } | ConvertTo-Json -Compress',
		].join('; ');

		execFile(
			'powershell.exe',
			['-NoProfile', '-NonInteractive', '-Command', script],
			{ timeout: 4500, windowsHide: true },
			(error, stdout) => {
				if (error || !stdout.trim()) {
					resolve({
						gpuPct: null,
						cpuTempC: null,
						batteryPct: null,
						chromeCpuSec: null,
						chromeRamMb: 0,
						chromeProcsCount: 0,
					});

					return;
				}

				try {
					const data = JSON.parse(stdout.trim());
					let cpuTempC: number | null = null;

					if (
						typeof data.HighPrecisionTemp === 'number' &&
						data.HighPrecisionTemp > 0
					) {
						// HighPrecisionTemperature is in deciKelvin (0.1 K)
						const celsius = data.HighPrecisionTemp / 10 - 273.15;

						if (celsius > 0 && celsius < 130) {
							cpuTempC = Math.round(celsius * 10) / 10;
						}
					}

					let gpuPct: number | null = null;

					if (typeof data.Gpu === 'number') {
						gpuPct = Math.round(data.Gpu * 10) / 10;
					}

					let batteryPct: number | null = null;

					if (typeof data.Battery === 'number') {
						batteryPct = data.Battery;
					}

					const chromeCpuSec =
						typeof data.ChromeCpuSec === 'number' ? data.ChromeCpuSec : null;
					const chromeRamMb =
						typeof data.ChromeRamMb === 'number' ? data.ChromeRamMb : 0;
					const chromeProcsCount =
						typeof data.ChromeCount === 'number' ? data.ChromeCount : 0;

					resolve({
						gpuPct,
						cpuTempC,
						batteryPct,
						chromeCpuSec,
						chromeRamMb,
						chromeProcsCount,
					});
				} catch {
					resolve({
						gpuPct: null,
						cpuTempC: null,
						batteryPct: null,
						chromeCpuSec: null,
						chromeRamMb: 0,
						chromeProcsCount: 0,
					});
				}
			}
		);
	});
}

const INTERVAL_MS = 5000; // 5 seconds
const MAX_DURATION_MS = 50 * 60 * 1000; // 50 minutes (3,000,000 ms)
const MAX_SAMPLES = 600; // 50 * 60 / 5 = 600 samples

export class SystemMetricsCollector {
	static #timer: NodeJS.Timeout | undefined;
	static #startTime: number = 0;
	static #sampleCount: number = 0;
	static #logFilePath: string | undefined;
	static #lastCpuSnapshot: CpuSnapshot | undefined;
	static #lastChromeCpuSec: number | undefined;
	static #lastChromeTime: number = 0;
	static #isRunning: boolean = false;
	static readonly #workerPids: Set<number> = new Set();
	static #observerRegistered: boolean = false;

	public static start(): void {
		if (this.#isRunning) {
			return;
		}

		try {
			this.#isRunning = true;
			this.#startTime = Date.now();
			this.#sampleCount = 0;
			this.#lastCpuSnapshot = getCpuSnapshot();
			this.#lastChromeCpuSec = undefined;
			this.#lastChromeTime = Date.now();

			this.#registerWorkerObserver();

			const logsDir = path.resolve(moduleDir, '..', 'logs');

			if (!fs.existsSync(logsDir)) {
				fs.mkdirSync(logsDir, { recursive: true });
			}

			const now = new Date();
			const pad = (n: number): string => String(n).padStart(2, '0');
			const dateStr = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;

			this.#logFilePath = path.join(logsDir, `metrics_${dateStr}.csv`);

			const header = [
				'timestamp',
				'elapsed_sec',
				'cpu_system_pct',
				'cpu_sfu_total_pct',
				'cpu_chrome_pct',
				'ram_used_mb',
				'ram_total_mb',
				'ram_pct',
				'ram_sfu_rss_mb',
				'ram_chrome_mb',
				'chrome_procs',
				'gpu_pct',
				'cpu_temp_c',
				'battery_pct',
			].join(',');

			fs.writeFileSync(this.#logFilePath, `${header}\n`, 'utf8');

			logger.info(
				`[system-metrics] Host metrics collector started. Interval: ${INTERVAL_MS / 1000}s, Max duration: ${MAX_DURATION_MS / 60000}m. Saving to: ${this.#logFilePath}`
			);

			process.stdout.write(
				`\n[system-metrics] Host metrics recording started (5s interval, 50m max).\n[system-metrics] Output CSV: ${this.#logFilePath}\n\n`
			);

			this.#timer = setInterval(() => {
				void this.#collectSample();
			}, INTERVAL_MS);
		} catch (error) {
			if (this.#timer) {
				clearInterval(this.#timer);
				this.#timer = undefined;
			}
			this.#isRunning = false;
			logger.warn(
				'[system-metrics] Collector could not start; server will continue without system metrics.',
				error
			);
		}
	}

	public static stop(): void {
		if (!this.#isRunning) {
			return;
		}

		if (this.#timer) {
			clearInterval(this.#timer);
			this.#timer = undefined;
		}

		this.#isRunning = false;

		const elapsedTotalSec = Math.round((Date.now() - this.#startTime) / 1000);

		logger.info(
			`[system-metrics] Monitoring stopped after ${elapsedTotalSec}s (${this.#sampleCount} samples). File: ${this.#logFilePath}`
		);

		try {
			process.stdout.write(
				`\n[system-metrics] Host metrics monitoring stopped after ${elapsedTotalSec}s (${this.#sampleCount} samples). File: ${this.#logFilePath}\n\n`
			);
		} catch {
			// A closed stdout pipe must not interrupt server shutdown.
		}
	}

	static #registerWorkerObserver(): void {
		if (this.#observerRegistered) {
			return;
		}

		this.#observerRegistered = true;

		mediasoup.observer.on('newworker', worker => {
			this.#workerPids.add(worker.pid);

			worker.observer.on('close', () => {
				this.#workerPids.delete(worker.pid);
			});
		});
	}

	static async #collectSample(): Promise<void> {
		if (!this.#isRunning) {
			return;
		}

		try {
			const elapsedSec = Math.round((Date.now() - this.#startTime) / 1000);

			if (
				elapsedSec >= MAX_DURATION_MS / 1000 ||
				this.#sampleCount >= MAX_SAMPLES
			) {
				process.stdout.write(
					`\n[system-metrics] 50-minute maximum measurement window reached (${this.#sampleCount} samples). Stopping collector.\n`
				);
				this.stop();

				return;
			}

			// 1. CPU metrics
			const currentCpuSnapshot = getCpuSnapshot();
			const cpuSystemPct = this.#lastCpuSnapshot
				? calculateCpuPercent(this.#lastCpuSnapshot, currentCpuSnapshot)
				: 0;

			this.#lastCpuSnapshot = currentCpuSnapshot;

			// Total SFU process CPU (Node.js + mediasoup-worker processes)
			let cpuSfuTotalPct = 0;
			let ramSfuRssMb = 0;

			try {
				const pids = [process.pid, ...this.#workerPids];
				const statsMap = await pidusage(pids);

				for (const stats of Object.values(statsMap)) {
					if (stats) {
						cpuSfuTotalPct += stats.cpu;
						ramSfuRssMb += stats.memory;
					}
				}

				const coreCount = os.cpus().length || 1;
				cpuSfuTotalPct = Math.round((cpuSfuTotalPct / coreCount) * 10) / 10;
				ramSfuRssMb = Math.round((ramSfuRssMb / (1024 * 1024)) * 10) / 10;
			} catch {
				ramSfuRssMb =
					Math.round((process.memoryUsage().rss / (1024 * 1024)) * 10) / 10;
			}

			// 2. RAM metrics (Physical host memory)
			const totalRamBytes = os.totalmem();
			const freeRamBytes = os.freemem();
			const usedRamBytes = totalRamBytes - freeRamBytes;
			const ramUsedMb = Math.round(usedRamBytes / (1024 * 1024));
			const ramTotalMb = Math.round(totalRamBytes / (1024 * 1024));
			const ramPct = Math.round((usedRamBytes / totalRamBytes) * 1000) / 10;

			// 3. Hardware & Chrome metrics (GPU, Temp, Battery, Chrome CPU/RAM)
			const {
				gpuPct,
				cpuTempC,
				batteryPct,
				chromeCpuSec,
				chromeRamMb,
				chromeProcsCount,
			} = await queryHardware();
			if (!this.#isRunning) {
				return;
			}

			// Calculate Chrome CPU % normalized across system cores
			let chromeCpuPct = 0;

			if (
				typeof chromeCpuSec === 'number' &&
				this.#lastChromeCpuSec !== undefined
			) {
				const cpuSecDelta = chromeCpuSec - this.#lastChromeCpuSec;
				const timeDeltaSec = (Date.now() - this.#lastChromeTime) / 1000;
				const coreCount = os.cpus().length || 1;

				if (cpuSecDelta > 0 && timeDeltaSec > 0) {
					chromeCpuPct =
						Math.round((cpuSecDelta / (timeDeltaSec * coreCount)) * 1000) / 10;
					chromeCpuPct = Math.max(0, Math.min(100, chromeCpuPct));
				}
			}

			if (typeof chromeCpuSec === 'number') {
				this.#lastChromeCpuSec = chromeCpuSec;
				this.#lastChromeTime = Date.now();
			}

			// 4. Record sample in CSV
			const timestamp = new Date(Date.now() + 9 * 60 * 60 * 1000)
				.toISOString()
				.replace('Z', '+09:00');
			const row = [
				timestamp,
				elapsedSec,
				cpuSystemPct,
				cpuSfuTotalPct,
				chromeCpuPct,
				ramUsedMb,
				ramTotalMb,
				ramPct,
				ramSfuRssMb,
				chromeRamMb,
				chromeProcsCount,
				gpuPct !== null ? gpuPct : '',
				cpuTempC !== null ? cpuTempC : '',
				batteryPct !== null ? batteryPct : '',
			].join(',');

			if (this.#logFilePath) {
				fs.appendFileSync(this.#logFilePath, `${row}\n`, 'utf8');
			}

			this.#sampleCount++;

			// 5. Console summary
			const tempStr = cpuTempC !== null ? `${cpuTempC}°C` : 'N/A';
			const gpuStr = gpuPct !== null ? `${gpuPct}%` : 'N/A';
			const battStr = batteryPct !== null ? `${batteryPct}%` : 'N/A';

			process.stdout.write(
				`[system-metrics] [${elapsedSec}s/3000s] CPU: ${cpuSystemPct}% (SFU: ${cpuSfuTotalPct}%, Chrome: ${chromeCpuPct}%) | RAM: ${ramPct}% (SFU: ${ramSfuRssMb}MB, Chrome: ${chromeRamMb}MB) | GPU: ${gpuStr} | Temp: ${tempStr} | Batt: ${battStr}\n`
			);
		} catch (error) {
			logger.error('#collectSample() failed:', error);
		}
	}
}
