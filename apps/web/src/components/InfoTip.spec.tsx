import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { ConfigProvider } from 'antd';
import { afterEach, describe, expect, it } from 'vitest';
import { InfoTip } from './InfoTip';

afterEach(cleanup);

describe('InfoTip', () => {
  it('shows details on hover without displaying them initially', async () => {
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <InfoTip label="配置说明" text="保存配置后执行连接测试。" />
      </ConfigProvider>,
    );
    expect(screen.queryByRole('tooltip')).not.toBeInTheDocument();
    fireEvent.mouseEnter(screen.getByRole('button', { name: '配置说明' }));
    expect(await screen.findByRole('tooltip')).toHaveTextContent('保存配置后执行连接测试。');
  });

  it('also exposes details to keyboard users', async () => {
    render(<InfoTip label="配置说明" text="连接地址须可从平台容器访问。" />);
    fireEvent.focus(screen.getByRole('button', { name: '配置说明' }));
    expect(await screen.findByRole('tooltip')).toHaveTextContent('连接地址须可从平台容器访问。');
  });
});
