/**
 * 클라이언트(브라우저)용 단순 on/off 로거 (boolean).
 *
 * - 프로덕션 모드 (기본, `vite build` 결과물): debug 호출은 무시,
 *   화면 log box 숨김, console.debug 없음
 * - 개발 디버깅 모드: 화면 log box 표시 + debug 콘솔 출력
 *
 * 디버깅 모드 켜지는 조건 (하나라도 만족하면 on):
 *  1. `npm run client` (Vite dev 서버, import.meta.env.DEV === true)
 *  2. URL에 `?debug=1` 포함 (의도적인 운영 정책: 프로덕션 빌드에서도
 *     현장 진단용으로 강제 on 가능)
 *  3. `localStorage.setItem('sfu_debug','1')`
 *
 * 우선순위: Vite DEV는 항상 debug / production build는 기본 off /
 * 필요 시 URL·localStorage로 강제 debug on 가능.
 * 끄기: `?debug=0` 또는 `localStorage.removeItem('sfu_debug')`
 *  (단, Vite dev 서버에서는 import.meta.env.DEV 때문에 항상 on)
 */

function detectDebug(): boolean {
  if (import.meta.env.DEV) {
    return true;
  }

  try {
    const params = new URLSearchParams(window.location.search);
    if (params.has('debug')) {
      const value = params.get('debug');
      // `?debug` / `?debug=1` / `?debug=true` -> on, `?debug=0` -> off
      if (value === null || value === '' || value === '1' || value.toLowerCase() === 'true') {
        return true;
      }
      if (value === '0' || value.toLowerCase() === 'false') {
        return false;
      }
    }
    if (window.localStorage.getItem('sfu_debug') === '1') {
      return true;
    }
  } catch {
    // localStorage 접근 실패 시 조용히 프로덕션 모드로 동작
  }

  return false;
}

export const IS_DEBUG: boolean = detectDebug();

/** 디버그 콘솔: 디버깅 모드에서만 console.debug (production no-op) */
export function debugConsole(...args: unknown[]): void {
  if (IS_DEBUG) {
    console.debug('[debug]', ...args);
  }
}
