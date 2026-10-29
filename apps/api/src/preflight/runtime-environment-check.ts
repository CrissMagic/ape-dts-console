import type { TaskPreflightCheck } from '@scs/contracts';

import { RuntimeMetricsPortAllocator } from '../runtime/runtime-metrics-port-allocator.js';

export const RUNTIME_ENVIRONMENT_CHECK = Symbol('RUNTIME_ENVIRONMENT_CHECK');
export interface RuntimeEnvironmentCheck {
  inspect(): Promise<TaskPreflightCheck>;
}

/** Inspects prerequisites only. Does not allocate ports or start task processes. */
export class RuntimeEnvironmentInspector implements RuntimeEnvironmentCheck {
  constructor(private readonly environment: NodeJS.ProcessEnv = process.env) {}

  async inspect(): Promise<TaskPreflightCheck> {
    const issues: Array<{ code: string; summary: string }> = [];
    try {
      new RuntimeMetricsPortAllocator({
        minPort: Number(this.environment.SCS_RUNTIME_METRICS_PORT_MIN),
        maxPort: Number(this.environment.SCS_RUNTIME_METRICS_PORT_MAX),
      });
    } catch {
      issues.push({
        code: 'RUNTIME_METRICS_PORT_ALLOCATION_UNCONFIGURED',
        summary:
          'Metrics 端口范围未配置或无效，请部署管理员配置 SCS_RUNTIME_METRICS_PORT_MIN/MAX。',
      });
    }
    return {
      key: 'RUNTIME_ENVIRONMENT',
      state: issues.length === 0 ? 'PASSED' : 'FAILED',
      code: issues[0]?.code ?? 'RUNTIME_ENVIRONMENT_READY',
      summary:
        issues.length === 0
          ? '运行环境基础检查通过；任务以独立子进程运行，共享容器 CPU 与内存，启动时仍会校验实际端口分配。'
          : issues.map((issue) => `${issue.summary}（${issue.code}）`).join(' '),
      details: { issues },
    };
  }
}
