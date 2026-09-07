import { expect, jest, test } from "@jest/globals";
import { hexToUint8Array } from "./utils";

test.each(["public", "local"])("URL selection uses configured clusters in %s mode", (mode) => {
  const originalMode = process.env.REACT_APP_MODE;
  const originalUrl = window.location.href;
  process.env.REACT_APP_MODE = mode;
  try {
    jest.isolateModules(() => {
      const { DEFAULT_CLUSTER, getClusters, getClusterConfig, getInitialCluster } = require("./config");
      for (const cluster of ["", "missing", "constructor", "toString", "__proto__"]) {
        window.history.replaceState(null, "", `/?cluster=${cluster}`);
        const selected = getInitialCluster();
        expect(() => hexToUint8Array(getClusterConfig(selected).PUBLIC_KEY_HEX)).not.toThrow();
        expect(selected).toBe(DEFAULT_CLUSTER);
      }
      for (const cluster of Object.keys(getClusters())) {
        window.history.replaceState(null, "", `/?cluster=${cluster}`);
        expect(getInitialCluster()).toBe(cluster);
      }
    });
  } finally {
    if (originalMode === undefined) {
      delete process.env.REACT_APP_MODE;
    } else {
      process.env.REACT_APP_MODE = originalMode;
    }
    window.history.replaceState(null, "", originalUrl);
  }
});
