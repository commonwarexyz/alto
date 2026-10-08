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
let unmount = () => {};
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

beforeEach(() => {
  delete window.ALTO_DEPLOYMENT;
  document.body.innerHTML = '<div id="root"></div>';
  mockApp.mockImplementation(() => {
    const React = require('react');
    const { getClusterConfig } = require('./config');
    const config = getClusterConfig();
    return React.createElement('p', null, [config.name, config.BACKEND_URL, config.PUBLIC_KEY_HEX].join(' | '));
  });
});

afterEach(() => {
  unmount();
  mockRoot = undefined;
  document.body.innerHTML = '';
  window.ALTO_DEPLOYMENT = originalDeployment;
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

test('a loaded standalone placeholder selects the local configuration', () => {
  runInNewContext(readFileSync(resolve(__dirname, '../public/runtime-config.js'), 'utf8'), { window });
  start();
  expect(mockApp).toHaveBeenCalled();
  expect(document.body.textContent).toBe('Local Cluster | localhost:8080 | 00');
});

test('loaded deployment settings take precedence over standalone settings', () => {
  window.ALTO_DEPLOYMENT = {
    name: 'Fixture deployment', description: '',
    BACKEND_URL: 'fixture.invalid', PUBLIC_KEY_HEX: 'ab', PARTICIPANTS: [], LOCATIONS: [],
  };
  start();
  expect(mockApp).toHaveBeenCalled();
  expect(document.body.textContent).toContain('Fixture deployment | fixture.invalid | ab');
});
