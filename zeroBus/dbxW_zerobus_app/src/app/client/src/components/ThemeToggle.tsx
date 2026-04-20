import { Sun, Moon, Monitor } from 'lucide-react';
import { useTheme } from '@/ThemeProvider';
import type { ThemePreference } from '@/ThemeProvider';

const options: { value: ThemePreference; icon: typeof Sun; label: string }[] = [
  { value: 'light', icon: Sun, label: 'Light' },
  { value: 'dark', icon: Moon, label: 'Dark' },
  { value: 'system', icon: Monitor, label: 'System' },
];

export function ThemeToggle() {
  const { preference, setPreference } = useTheme();

  // Cycle through: system → light → dark → system
  const cycle = () => {
    const order: ThemePreference[] = ['system', 'light', 'dark'];
    const next = order[(order.indexOf(preference) + 1) % order.length];
    setPreference(next);
  };

  const current = options.find((o) => o.value === preference) ?? options[2];
  const Icon = current.icon;

  return (
    <button
      onClick={cycle}
      className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium text-gray-300 hover:text-white hover:bg-white/10 transition-all duration-200"
      title={`Theme: ${current.label} — click to cycle`}
      aria-label={`Current theme: ${current.label}`}
    >
      <Icon className="h-3.5 w-3.5" />
      <span className="hidden sm:inline">{current.label}</span>
    </button>
  );
}
