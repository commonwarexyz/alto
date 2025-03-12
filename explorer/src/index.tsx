import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';
import ErrorBoundary from './ErrorBoundary';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);

// Only use StrictMode in development, not in production
if (process.env.NODE_ENV === 'development') {
  root.render(
    <React.StrictMode>
      <ErrorBoundary>
        <App />
      </ErrorBoundary>
    </React.StrictMode>
  );
} else {
  root.render(
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  );
}