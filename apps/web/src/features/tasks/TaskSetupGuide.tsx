import { Button, Space, Steps, Typography } from 'antd';
import { InfoTip } from '../../components/InfoTip';

export function TaskSetupHelp() {
  return (
    <InfoTip
      label="同步配置说明"
      text="配置流程：登记并测试数据源，创建任务草稿，运行预检，再由管理员确认风险并显式启动。保存草稿和预检都不会自动启动同步；任务列表仅显示当前账号可访问的任务。"
    />
  );
}

export function TaskSetupGuide({
  onOpenDataSources,
  onOpenTasks,
}: {
  onOpenDataSources?: (() => void) | undefined;
  onOpenTasks?: (() => void) | undefined;
}) {
  return (
    <Space orientation="vertical" size={16} style={{ width: '100%', textAlign: 'left' }}>
      <Steps
        size="small"
        items={[
          { title: '配置数据源', status: 'wait' },
          { title: '验证连接', status: 'wait' },
          { title: '创建任务', status: 'wait' },
          { title: '首次运行', status: 'wait' },
        ]}
      />
      <Typography.Text type="secondary">
        保存任务后先运行预检；预检结果会明确列出连接、元数据、制品、资源和容量阻断项。
      </Typography.Text>
      <Space wrap>
        {onOpenDataSources && <Button onClick={onOpenDataSources}>前往数据源</Button>}
        {onOpenTasks && (
          <Button type="primary" onClick={onOpenTasks}>
            创建任务
          </Button>
        )}
      </Space>
    </Space>
  );
}
