import type { AlertDelivery } from '@scs/contracts';

export function alertErrorCategory(item: AlertDelivery) {
  if (item.status === 'FAILED') return '通知发送失败';
  const code = item.faultCode ?? '';
  if (code === 'RUNTIME_PG_REPLICATION_DISCONNECTED') return '复制连接失联';
  if (/TIMEOUT|ETIMEDOUT/u.test(code)) return '连接或读取超时';
  if (/CONTINUITY|IDENTITY/u.test(code)) return '连续性校验失败';
  if (/CAPACITY/u.test(code)) return '运行容量校验失败';
  if (code === 'PREFLIGHT_EXTERNAL_CHECK_FAILED') return '外部依赖检查失败';
  if (code === 'RUNTIME_AUTO_RECOVERY_DISABLED') return '自动恢复已关闭';
  return (
    (
      {
        TEST: '无异常',
        UNEXPECTED_EXIT: '进程异常退出',
        RECOVERY_FAILED: '任务恢复失败',
        START_FAILED: '任务启动失败',
        PG_SLOT_INACTIVE: '复制槽未连接',
        PG_SLOT_MISSING: '复制槽不存在',
        PG_RETAINED_WAL: '保留 WAL 超限',
        PG_UNCONFIRMED_WAL: '未确认 WAL 超限',
        KAFKA_OFFSET_LAG: '消费积压超限',
        SYNC_DELAY: '同步延迟超限',
        PLATFORM_METADATA_UNAVAILABLE: '元数据库不可用',
        PLATFORM_CONTROL_UNAVAILABLE: '控制面不可用',
      } as Record<string, string>
    )[item.rule] ?? '其他异常'
  );
}
