import { createContext, useContext } from 'react';

export type ColorMode = 'light' | 'dark';
export const THEME_STORAGE_KEY = 'scs.theme';
export const ThemeContext = createContext<{
  mode: ColorMode;
  setMode: (mode: ColorMode) => void;
}>({ mode: 'light', setMode: () => undefined });

export function useTheme() {
  return useContext(ThemeContext);
}
