export function platformVersion(): string {
  const configured = process.env.SCS_PLATFORM_VERSION?.trim();
  return configured === undefined || configured.length === 0 ? '0.0.0' : configured;
}
