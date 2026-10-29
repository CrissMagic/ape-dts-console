import { useEffect, useState } from 'react';
import type { BootstrapStatusResponse } from '@scs/contracts';
import { Alert, Button, Tooltip } from 'antd';
import { QuestionCircleOutlined } from '@ant-design/icons';
import { bootstrapApi, BootstrapApiError } from '../../api/bootstrap-api';

export function BootstrapPage({
  onReady,
  onRecovery,
}: {
  onReady?: () => void;
  onRecovery?: () => void;
}) {
  const [status, setStatus] = useState<BootstrapStatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);
  const issues: Record<string, string> = {
    DATABASE_CONFIGURATION_REQUIRED: '请设置部署环境变量 SCS_PLATFORM_DATABASE_CONFIG。',
    LEGACY_SECURITY_MIGRATION_REQUIRED: '现有平台需由部署人员通过容器命令完成一次密钥迁移。',
    DATABASE_UPGRADE_REQUIRED: '数据库需要升级，请由部署人员备份后执行升级命令。',
    LOCAL_STARTUP_FAILED: '自动启动失败，请由部署人员检查容器日志。',
  };
  useEffect(() => {
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    const check = async () => {
      try {
        const result = await bootstrapApi.getStatus();
        if (!active) return;
        setStatus(result);
        setError(null);
        if (result.startupIssue === undefined && result.phase === 'READY') {
          onReady?.();
          return;
        }
        if (result.startupIssue === undefined && result.phase === 'RECOVERING') {
          onRecovery?.();
          return;
        }
      } catch (cause) {
        if (active)
          setError(
            cause instanceof BootstrapApiError && cause.code === 'NETWORK_ERROR'
              ? '服务连接已中断'
              : '暂时无法读取平台状态',
          );
      }
      if (active)
        timer = setTimeout(() => {
          void check();
        }, 3000);
    };
    void check();
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [onReady, onRecovery, refresh]);
  return (
    <main className="platform-shell">
      <section className="bootstrap-card auth-card">
        <header className="platform-header">
          <div>
            <p className="eyebrow">ape-dts · 数据同步管理平台</p>
            <h1>ape-dts 统一管理平台</h1>
          </div>
          <Tooltip title="页面每 3 秒检查服务状态。访问隧道断开不代表容器或数据库故障；仅在服务明确返回启动问题时显示配置指引。">
            <QuestionCircleOutlined aria-label="启动说明" />
          </Tooltip>
        </header>
        <section className="bootstrap-panel">
          <p role="status">
            {error
              ? error
              : status === null
                ? '正在读取启动状态'
                : status.startupIssue !== undefined
                  ? '平台启动受阻'
                  : status.phase === 'READY'
                    ? '平台已就绪'
                    : '等待平台启动'}
          </p>
          {error ? (
            <Alert
              type="warning"
              showIcon
              title="请检查访问隧道、代理或服务连接，页面将自动重试。"
            />
          ) : (
            status &&
            (status.phase !== 'READY' || status.startupIssue !== undefined) && (
              <Alert
                type={status.startupIssue !== undefined ? 'error' : 'info'}
                title={status.startupIssue ? '平台报告启动问题' : '正在等待平台就绪'}
                description={
                  status.startupIssue
                    ? (issues[status.startupIssue] ??
                      `请检查容器启动日志（${status.startupIssue}）。`)
                    : `服务返回状态：${status.phase}。页面将自动检查，请等待服务就绪。`
                }
              />
            )
          )}
          <Button onClick={() => setRefresh((value) => value + 1)}>刷新状态</Button>
        </section>
      </section>
    </main>
  );
}
