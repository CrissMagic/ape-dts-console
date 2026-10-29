import { Button, Tooltip } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';

export function InfoTip({ label, text }: { label: string; text: string }) {
  return (
    <Tooltip title={text} trigger={['hover', 'focus']} placement="topRight">
      <Button
        type="text"
        size="small"
        aria-label={label}
        icon={<InfoCircleOutlined aria-hidden />}
      />
    </Tooltip>
  );
}
