import { Moon, Sun } from 'lucide-react';
import { useTheme } from '../app/theme-context';
import { Button } from './ui/button';

export function ThemeToggle({ floating = false }: { floating?: boolean }) {
  const { mode, setMode } = useTheme();
  const label = mode === 'dark' ? '切换为浅色主题' : '切换为深色主题';
  return (
    <Button
      type="button"
      variant="ghost"
      size="icon"
      aria-label={label}
      title={label}
      className={floating ? 'fixed right-4 top-3 z-50 border bg-background' : undefined}
      onClick={() => setMode(mode === 'dark' ? 'light' : 'dark')}
    >
      {mode === 'dark' ? <Sun className="size-4" /> : <Moon className="size-4" />}
    </Button>
  );
}
