import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';
import ErrorBoundary from './ErrorBoundary';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);

const content = window.ALTO_DEPLOYMENT === undefined ? (
  <div className="error-container" role="alert">
    <h2>Explorer configuration failed to load.</h2>
    <p>Refresh the page to retry.</p>
  </div>
) : (
  <ErrorBoundary>
    <App />
  </ErrorBoundary>
);

// Use StrictMode in development.
root.render(
  process.env.NODE_ENV === 'development' ? <React.StrictMode>{content}</React.StrictMode> : content
);
