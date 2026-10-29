import { useCallback, useLayoutEffect, useMemo, useState, type ReactNode } from 'react';
import { ConfigProvider, theme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { ThemeContext, THEME_STORAGE_KEY, type ColorMode } from './theme-context';

function savedMode(): ColorMode {
  try {
    return localStorage.getItem(THEME_STORAGE_KEY) === 'dark' ? 'dark' : 'light';
  } catch {
    return 'light';
  }
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [mode, setCurrentMode] = useState<ColorMode>(savedMode);
  const setMode = useCallback((next: ColorMode) => {
    setCurrentMode(next);
    try {
      localStorage.setItem(THEME_STORAGE_KEY, next);
    } catch {
      // Storage can be disabled; switching must still work for this page.
    }
  }, []);
  useLayoutEffect(() => {
    document.documentElement.classList.toggle('dark', mode === 'dark');
    document.documentElement.style.colorScheme = mode;
    document
      .querySelector('meta[name="theme-color"]')
      ?.setAttribute('content', mode === 'dark' ? '#09090b' : '#ffffff');
  }, [mode]);
  const value = useMemo(() => ({ mode, setMode }), [mode, setMode]);
  const antTheme = useMemo(
    () => ({
      algorithm: mode === 'dark' ? theme.darkAlgorithm : theme.defaultAlgorithm,
      components: {
        Tooltip: {
          colorTextLightSolid: '#fafafa',
          ...(mode === 'dark' ? { colorBgSpotlight: '#27272a' } : {}),
        },
      },
      token: {
        colorPrimary: mode === 'dark' ? '#60a5fa' : '#1e293b',
        colorBgBase: mode === 'dark' ? '#09090b' : '#ffffff',
        colorTextBase: mode === 'dark' ? '#fafafa' : '#1e293b',
        colorTextLightSolid: mode === 'dark' ? '#09090b' : '#ffffff',
        colorBgLayout: mode === 'dark' ? '#09090b' : '#f5f6f8',
        ...(mode === 'dark'
          ? {
              colorBgContainer: '#18181b',
              colorBgElevated: '#202023',
              colorBorder: '#3f3f46',
              colorSplit: '#333338',
              colorLink: '#60a5fa',
              colorLinkHover: '#93c5fd',
              colorLinkActive: '#3b82f6',
            }
          : {}),
        borderRadius: 6,
        fontSize: 14,
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", sans-serif',
      },
    }),
    [mode],
  );
  return (
    <ThemeContext.Provider value={value}>
      <ConfigProvider locale={zhCN} theme={antTheme}>
        {children}
      </ConfigProvider>
    </ThemeContext.Provider>
  );
}
