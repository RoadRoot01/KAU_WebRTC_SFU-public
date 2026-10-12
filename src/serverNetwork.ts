import { isIP } from 'node:net';

/**
 * Resolve the address advertised in ICE candidates. The override accepts only
 * an IP literal so a misspelled host name cannot silently produce unusable
 * candidates. An absent variable preserves the historical interface choice.
 */
export function resolveAnnouncedIp(
  configuredValue: string | undefined,
  detectedValue: string
): string {
  if (configuredValue === undefined) {
    return detectedValue;
  }

  if (
    configuredValue.length === 0 ||
    configuredValue.trim() !== configuredValue ||
    isIP(configuredValue) === 0
  ) {
    throw new Error(
      'SFU_ANNOUNCED_IP must be a valid IPv4 or IPv6 address literal.'
    );
  }

  return configuredValue;
}
