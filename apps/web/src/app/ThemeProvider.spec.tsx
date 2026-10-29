import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { theme } from 'antd';
import { ThemeProvider } from './ThemeProvider';
import { THEME_STORAGE_KEY } from './theme-context';
import { ThemeToggle } from '../components/ThemeToggle';

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  localStorage.clear();
  document.documentElement.classList.remove('dark');
  document.documentElement.style.colorScheme = '';
});

function SampleForm() {
  const { token } = theme.useToken();
  return (
    <>
      <ThemeToggle />
      <input aria-label="任务名称" defaultValue="" />
      <output aria-label="组件背景">{token.colorBgContainer}</output>
    </>
  );
}

it('defaults to the current light appearance and switches both the document and Ant components', () => {
  render(
    <ThemeProvider>
      <SampleForm />
    </ThemeProvider>,
  );
  expect(document.documentElement).not.toHaveClass('dark');
  expect(screen.getByLabelText('组件背景')).toHaveTextContent('#ffffff');
  fireEvent.change(screen.getByRole('textbox', { name: '任务名称' }), {
    target: { value: '未保存的任务' },
  });
  fireEvent.click(screen.getByRole('button', { name: '切换为深色主题' }));
  expect(document.documentElement).toHaveClass('dark');
  expect(document.documentElement.style.colorScheme).toBe('dark');
  expect(screen.getByLabelText('组件背景')).toHaveTextContent('#18181b');
  expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark');
  expect(screen.getByRole('textbox', { name: '任务名称' })).toHaveValue('未保存的任务');
  fireEvent.click(screen.getByRole('button', { name: '切换为浅色主题' }));
  expect(document.documentElement).not.toHaveClass('dark');
  expect(document.documentElement.style.colorScheme).toBe('light');
  expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('light');
  expect(screen.getByRole('textbox', { name: '任务名称' })).toHaveValue('未保存的任务');
});

it('restores the chosen theme when the application is mounted again', () => {
  const view = render(
    <ThemeProvider>
      <ThemeToggle />
    </ThemeProvider>,
  );
  fireEvent.click(screen.getByRole('button', { name: '切换为深色主题' }));
  view.unmount();
  render(
    <ThemeProvider>
      <SampleForm />
    </ThemeProvider>,
  );
  expect(document.documentElement).toHaveClass('dark');
  expect(screen.getByRole('button', { name: '切换为浅色主题' })).toBeInTheDocument();
  expect(screen.getByLabelText('组件背景')).toHaveTextContent('#18181b');
});

it('keeps switching usable when browser storage is disabled', () => {
  vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
    throw new Error('storage blocked');
  });
  vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
    throw new Error('storage blocked');
  });
  render(
    <ThemeProvider>
      <SampleForm />
    </ThemeProvider>,
  );
  fireEvent.click(screen.getByRole('button', { name: '切换为深色主题' }));
  expect(document.documentElement).toHaveClass('dark');
  expect(screen.getByRole('button', { name: '切换为浅色主题' })).toBeInTheDocument();
  fireEvent.click(screen.getByRole('button', { name: '切换为浅色主题' }));
  expect(document.documentElement).not.toHaveClass('dark');
});

it('falls back to light for an unrecognized saved preference', () => {
  localStorage.setItem(THEME_STORAGE_KEY, 'invalid');
  render(
    <ThemeProvider>
      <ThemeToggle floating />
    </ThemeProvider>,
  );
  expect(screen.getByRole('button', { name: '切换为深色主题' })).toBeInTheDocument();
  expect(document.documentElement).not.toHaveClass('dark');
});
