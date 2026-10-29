import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { App } from './app/App';
import { ThemeProvider } from './app/ThemeProvider';
import './app/shadcn.css';

const rootElement = document.getElementById('root');
if (rootElement === null) {
  throw new Error('Application root element is missing');
}

createRoot(rootElement).render(
  <StrictMode>
    <ThemeProvider>
      <App />
    </ThemeProvider>
  </StrictMode>,
);
