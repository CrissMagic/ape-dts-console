import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { emergencyApi } from '../../api/platform-api';
import { EmergencyPage } from './EmergencyPage';

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe('EmergencyPage', () => {
  it('shows and hides the stop code without submitting the form or clearing its value', () => {
    const list = vi.spyOn(emergencyApi, 'list').mockResolvedValue([]);
    render(<EmergencyPage />);
    const input = screen.getByLabelText('当前进程紧急停止码');
    fireEvent.change(input, { target: { value: 'secret-code' } });
    expect(input).toHaveAttribute('type', 'password');
    fireEvent.click(screen.getByRole('img', { name: 'eye-invisible' }));
    expect(input).toHaveAttribute('type', 'text');
    expect(input).toHaveValue('secret-code');
    fireEvent.click(screen.getByRole('img', { name: 'eye' }));
    expect(input).toHaveAttribute('type', 'password');
    expect(list).not.toHaveBeenCalled();
  });
  it('requires a reason and a second confirmation before force-kill', async () => {
    const runtime = { runId: 'run-1', taskId: 'task-1', unitCount: 2 };
    vi.spyOn(emergencyApi, 'list').mockResolvedValue([runtime]);
    const forceKill = vi.spyOn(emergencyApi, 'forceKill').mockResolvedValue(undefined);
    render(<EmergencyPage />);
    fireEvent.change(screen.getByLabelText('当前进程紧急停止码'), {
      target: { value: 'secret-code' },
    });
    fireEvent.click(screen.getByRole('button', { name: '读取内存运行清单' }));
    fireEvent.click(await screen.findByRole('button', { name: '立即强杀…' }));
    expect(screen.getByRole('button', { name: '确认立即强杀' })).toBeDisabled();
    fireEvent.change(screen.getByLabelText('强杀原因'), { target: { value: '进程失去响应' } });
    fireEvent.click(screen.getByRole('button', { name: '确认立即强杀' }));
    await waitFor(() =>
      expect(forceKill).toHaveBeenCalledWith('secret-code', 'task-1', '进程失去响应'),
    );
  });
});
