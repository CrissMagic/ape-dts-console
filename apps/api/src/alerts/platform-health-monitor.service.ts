import { Injectable, Logger, type OnModuleInit, type OnApplicationShutdown } from '@nestjs/common';
import { MetadataAvailabilityService } from '../metadata-availability/metadata-availability.service.js';
import { PlatformReadinessService } from '../health/platform-readiness.service.js';
import { AlertsService } from './alerts.service.js';

@Injectable()
export class PlatformHealthMonitorService implements OnModuleInit, OnApplicationShutdown {
  private timer: ReturnType<typeof setInterval> | undefined;
  private running = false;
  private stopped = false;
  private incident:
    { rule: string; startedAt: string; lastAttempt: number; notified: boolean } | undefined;
  private readonly logger = new Logger(PlatformHealthMonitorService.name);
  constructor(
    private readonly availability: MetadataAvailabilityService,
    private readonly readiness: PlatformReadinessService,
    private readonly alerts: AlertsService,
  ) {}
  onModuleInit() {
    void this.tick();
    this.timer = setInterval(() => void this.tick(), 5000);
    this.timer.unref();
  }
  onApplicationShutdown() {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
  }
  async tick() {
    if (
      this.running ||
      this.stopped ||
      ['SEALED', 'BOOTSTRAPPING'].includes(this.readiness.getPhase())
    )
      return;
    this.running = true;
    try {
      const phase = this.readiness.getPhase();
      if (phase !== 'READY' && !this.incident)
        await this.report(
          phase === 'BLOCKED_CONCURRENT_INSTANCE'
            ? 'PLATFORM_CONTROL_UNAVAILABLE'
            : 'PLATFORM_METADATA_UNAVAILABLE',
        );
      // Not tied to HTTP requests: even an unattended platform probes and reconciles itself.
      await this.availability.ensureManagementAvailable();
      if (this.readiness.getPhase() !== 'READY' || this.stopped) return;
      await this.alerts.get();
      if (this.incident) {
        if (this.incident.notified)
          await this.alerts.notifyPlatform(
            this.incident.rule,
            'RECOVERY',
            `异常开始时间：${this.incident.startedAt}\n元数据库和控制面已恢复。`,
          );
        this.logger.log({ code: 'PLATFORM_HEALTH_RECOVERED', startedAt: this.incident.startedAt });
        this.incident = undefined;
      }
      await this.alerts.flushPlatformDeliveries();
    } catch (error) {
      const connectivity = this.availability.reportFailure(error, 'platform_health_monitor');
      if (this.stopped) return;
      if (!connectivity && this.readiness.getPhase() === 'READY') {
        this.logger.error({ code: 'PLATFORM_HEALTH_MONITOR_FAILED' });
        return;
      }
      try {
        await this.report(
          this.readiness.getPhase() === 'BLOCKED_CONCURRENT_INSTANCE'
            ? 'PLATFORM_CONTROL_UNAVAILABLE'
            : 'PLATFORM_METADATA_UNAVAILABLE',
        );
      } catch {
        this.logger.error({ code: 'PLATFORM_HEALTH_NOTIFICATION_FAILED' });
      }
    } finally {
      this.running = false;
    }
  }
  private async report(rule: string) {
    const now = Date.now();
    if (!this.incident) {
      this.incident = {
        rule,
        startedAt: new Intl.DateTimeFormat('zh-CN', {
          timeZone: 'Asia/Shanghai',
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hour12: false,
        })
          .format(new Date())
          .replace(/^(\d{4})\/(\d{2})\/(\d{2})/u, '$1年$2月$3日'),
        lastAttempt: 0,
        notified: false,
      };
      this.logger.error({ code: rule, phase: this.readiness.getPhase() });
    }
    if (this.incident.lastAttempt && now - this.incident.lastAttempt < 60_000) return;
    this.incident.lastAttempt = now;
    this.incident.notified =
      (await this.alerts.notifyPlatform(
        rule,
        'ALARM',
        `异常开始时间：${this.incident.startedAt}\n请检查平台元数据库连接和控制面状态；后台将继续探测。`,
      )) || this.incident.notified;
  }
}
