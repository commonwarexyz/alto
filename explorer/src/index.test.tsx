import { afterEach, beforeEach, expect, jest, test } from '@jest/globals';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import { runInNewContext } from 'vm';
import type { Root } from 'react-dom/client';

let mockRoot: Root | undefined;
const mockApp = jest.fn();

jest.mock('./App', () => ({ __esModule: true, default: () => mockApp() }));
jest.mock('react-dom/client', () => {
  const { jest } = require('@jest/globals');
  const client = jest.requireActual('react-dom/client') as typeof import('react-dom/client');
  return {
    ...client,
    createRoot: (...args: Parameters<typeof client.createRoot>) => {
      mockRoot = client.createRoot(...args);
      return mockRoot;
    },
  };
});

const originalDeployment = window.ALTO_DEPLOYMENT;
const originalMode = process.env.REACT_APP_MODE;
const originalUrl = window.location.href;
let unmount = () => {};
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

beforeEach(() => {
  delete window.ALTO_DEPLOYMENT;
  delete process.env.REACT_APP_MODE;
  window.history.replaceState(null, '', '/');
  document.body.innerHTML = '<div id="root"></div>';
  mockApp.mockImplementation(() => {
    const React = require('react');
    const { getClusterConfig, getInitialCluster } = require('./config');
    const config = getClusterConfig(getInitialCluster());
    return React.createElement('p', null, [config.name, config.BACKEND_URL, config.CERTIFICATE_MODE].join(' | '));
  });
});

afterEach(() => {
  unmount();
  mockRoot = undefined;
  document.body.innerHTML = '';
  window.ALTO_DEPLOYMENT = originalDeployment;
  if (originalMode === undefined) delete process.env.REACT_APP_MODE;
  else process.env.REACT_APP_MODE = originalMode;
  window.history.replaceState(null, '', originalUrl);
});

function start() {
  jest.isolateModules(() => {
    const { act } = require('react');
    unmount = () => act(() => mockRoot?.unmount());
    act(() => { require('./index'); });
  });
}

test('a failed configuration load stops startup before the explorer mounts', () => {
  start();
  expect(mockApp).not.toHaveBeenCalled();
  expect(document.body.textContent).toMatch(/configuration.*load/i);
});

test.each(['public', 'local'])('a loaded standalone placeholder selects %s configuration', mode => {
  process.env.REACT_APP_MODE = mode;
  runInNewContext(readFileSync(resolve(__dirname, '../public/runtime-config.js'), 'utf8'), { window });
  start();
  expect(mockApp).toHaveBeenCalled();
  expect(document.body.textContent).toContain(mode === 'public' ? 'Global Cluster' : 'Local Cluster');
});

test('loaded deployment settings take precedence over standalone settings', () => {
  process.env.REACT_APP_MODE = 'local';
  window.history.replaceState(null, '', '/?cluster=usa');
  window.ALTO_DEPLOYMENT = {
    mode: 'public', name: 'Fixture deployment', description: '',
    BACKEND_URL: 'fixture.invalid', PUBLIC_KEY_HEX: '00', CERTIFICATE_MODE: 'standard',
    PARTICIPANTS: [], LOCATIONS: [],
  };
  start();
  expect(mockApp).toHaveBeenCalled();
  expect(document.body.textContent).toContain('Fixture deployment | fixture.invalid | standard');
});
