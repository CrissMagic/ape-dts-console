import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { useOperationNotification } from './useOperationNotification';
import { ConfigProvider } from 'antd';

function Example() {
  const { holder, notify } = useOperationNotification();
  return (
    <>
      {holder}
      {(['success', 'error', 'warning', 'info'] as const).map((type) => (
        <button key={type} onClick={() => notify(type, `${type}结果`)}>
          {type}
        </button>
      ))}
    </>
  );
}

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe('operation notifications', () => {
  it.each(['success', 'error', 'warning', 'info'] as const)(
    'renders %s feedback with its own color and icon',
    async (type) => {
      render(<Example />);
      expect(screen.queryByText(`${type}结果`)).not.toBeInTheDocument();
      fireEvent.click(screen.getByRole('button', { name: type }));
      const message = await screen.findByText(`${type}结果`);
      const notice = message.closest('.ant-notification-notice');
      expect(notice).toHaveClass(`operation-notification-${type}`);
      expect(notice?.querySelector(`.ant-notification-notice-icon-${type}`)).not.toBeNull();
    },
  );

  it('closes automatically after five seconds and can show the same result again', async () => {
    vi.useFakeTimers();
    render(
      <ConfigProvider theme={{ token: { motion: false } }}>
        <Example />
      </ConfigProvider>,
    );
    fireEvent.click(screen.getByRole('button', { name: 'success' }));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(100);
    });
    expect(screen.getByText('success结果')).toBeInTheDocument();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5500);
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(screen.queryByText('success结果')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'success' }));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(100);
    });
    expect(screen.getByText('success结果')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(screen.queryByText('success结果')).not.toBeInTheDocument();
  });
});
