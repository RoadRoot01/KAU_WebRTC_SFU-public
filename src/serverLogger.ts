/**
 * 서버(Node)용 단순 on/off 로거 (A안: boolean).
 *
 * - 프로덕션 모드 (기본): info/warn/error 만 출력
 * - 개발 디버깅 모드: debug 까지 전부 출력
 *
 * 디버깅 모드 켜는 방법 (하나라도 만족하면 on):
 *  1. `npm run server:debug` (내부적으로 `--debug` 플래그 전달)
 *  2. `SFU_DEBUG=1` 환경변수 (예: `$env:SFU_DEBUG="1"; npm run server`)
 *  3. `NODE_ENV=development` 또는 `NODE_ENV=debug`
 */
const argv = process.argv.slice(2);
const envFlag = process.env.SFU_DEBUG?.toLowerCase();
const nodeEnv = process.env.NODE_ENV?.toLowerCase();

export const IS_DEBUG: boolean =
  argv.includes('--debug') ||
  envFlag === '1' ||
  envFlag === 'true' ||
  nodeEnv === 'development' ||
  nodeEnv === 'debug';

export const logger = {
  /** 디버깅 모드에서만 출력. 프로덕션에서는 no-op. */
  debug(...args: unknown[]): void {
    if (IS_DEBUG) {
      console.debug('[debug]', ...args);
    }
  },
  /** 기동 메시지 등 프로덕션에서도 보여야 할 정보 */
  info(...args: unknown[]): void {
    console.log(...args);
  },
  /** 경고 (항상 출력) */
  warn(...args: unknown[]): void {
    console.warn(...args);
  },
  /** 에러 (항상 출력) */
  error(...args: unknown[]): void {
    console.error(...args);
  }
};
