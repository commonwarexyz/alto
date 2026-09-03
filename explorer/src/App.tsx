import React, { useEffect, useState, useRef, useCallback, useMemo } from "react";
import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
import { DivIcon, LatLng } from "leaflet";
import "leaflet/dist/leaflet.css";
import init, { leader_index } from "./alto_types/alto_types.js";
import {
  getClusterConfig,
  getClusters,
  getHttpBackendUrl,
  getWebSocketBackendUrl,
  Cluster,
  DEFAULT_CLUSTER,
  MODE,
} from "./config";
import { SeedJs, NotarizedJs, FinalizedJs, ViewData } from "./types";
import { hexToUint8Array, hexUint8Array } from "./utils";
import { resolveLeaderLocation } from "./leaderLocation";
import {
  ConsensusWorkerPool,
  consensusWorkerCount,
  VerifiedConsensusArtifact,
} from "./consensusWorkerPool";
import { scaleTimelineWidth } from "./timeline";
import { getLeaderIndicator, getTimelineIdentifier } from "./timelineIdentifier";
import "./App.css";
import AboutModal from './AboutModal';
import './AboutModal.css';
import StatsSection from "./StatsSection";
import './StatsSection.css';
import KeyInfoModal from './KeyModal';
import MapOverlay from './MapOverlay';
import './MapOverlay.css';
import { useClockSkew } from './useClockSkew';
import ErrorNotification from './ErrorNotification';
import './ErrorNotification.css';
import MaintenancePage from './MaintenancePage';
import SearchModal from './SearchModal';
import './SearchModal.css';

const getInitialCluster = (): Cluster => {
  // Get the cluster from the URL
  const params = new URLSearchParams(window.location.search);
  const clusterFromUrl = params.get('cluster');
  const allClusters = getClusters();

  // If the cluster exists, use it
  if (clusterFromUrl && (clusterFromUrl in allClusters)) {
    return clusterFromUrl as Cluster;
  }

  // Otherwise, use the default cluster
  return DEFAULT_CLUSTER;
};

const TIMEOUT_DURATION = 5000; // 5s
const HEALTH_CHECK_INTERVAL = 60000; // Check health every minute
const VIEW_RENDER_INTERVAL = 100;
const MAX_TRACKED_VIEWS = 128;
const MAX_RENDERED_VIEWS = 50;

const retainNewestViews = (views: ViewData[]): ViewData[] => {
  const sorted = [...views].sort((a, b) => b.view - a.view);
  for (const discarded of sorted.slice(MAX_TRACKED_VIEWS)) {
    if (discarded.timeoutId) {
      clearTimeout(discarded.timeoutId);
    }
  }
  return sorted.slice(0, MAX_TRACKED_VIEWS);
};

const center = new LatLng(0, 0);
// ASCII Logo animation logic
const initializeLogoAnimations = () => {
  const horizontalSymbols = [" ", "*", "+", "-", "~"];
  const verticalSymbols = [" ", "*", "+", "|"];
  const edgeSymbols = [" ", "*", "+"];

  function getRandomItem(arr: string[]) {
    return arr[Math.floor(Math.random() * arr.length)];
  }

  function getRandomDuration(min: number) {
    return Math.random() * (10000 - min) + min;
  }

  function updateSymbol(symbol: Element, choices: string[]) {
    symbol.textContent = getRandomItem(choices);
    setTimeout(() => updateSymbol(symbol, choices), getRandomDuration(500));
  }

  document.querySelectorAll('.horizontal-logo-symbol').forEach(symbol => {
    setTimeout(() => updateSymbol(symbol, horizontalSymbols), getRandomDuration(1500));
  });

  document.querySelectorAll('.vertical-logo-symbol').forEach(symbol => {
    setTimeout(() => updateSymbol(symbol, verticalSymbols), getRandomDuration(1500));
  });

  document.querySelectorAll('.edge-logo-symbol').forEach(symbol => {
    setTimeout(() => updateSymbol(symbol, edgeSymbols), getRandomDuration(1500));
  });
};

const App: React.FC = () => {
  const [selectedCluster, setSelectedCluster] = useState<Cluster>(getInitialCluster());
  const clusterConfig = useMemo(() => getClusterConfig(selectedCluster), [selectedCluster]);
  const allConfigs = useMemo(() => getClusters(), []);
  const { BACKEND_URL, PUBLIC_KEY_HEX, LOCATIONS, PARTICIPANTS } = clusterConfig;
  const standardCertificates = clusterConfig.CERTIFICATE_MODE === 'standard';
  const leaderIndicator = getLeaderIndicator(standardCertificates);
  const timelineIdentifier = getTimelineIdentifier(standardCertificates);
  const markerIcon = useMemo(() => new DivIcon({
    className: "custom-div-icon",
    html: `<div style="
          background-color: ${leaderIndicator.color};
          width: 16px;
          height: 16px;
          border-radius: 50%;
          "></div>`,
    iconSize: [12, 12],
    iconAnchor: [6, 6],
  }), [leaderIndicator.color]);
  const PUBLIC_KEY = useMemo(() => hexToUint8Array(PUBLIC_KEY_HEX), [PUBLIC_KEY_HEX]);

  const [views, setViews] = useState<ViewData[]>([]);
  const mappedView = useMemo(
    () => views.reduce<ViewData | undefined>((latest, view) => {
      if (view.location === undefined || (latest && latest.view >= view.view)) {
        return latest;
      }
      return view;
    }, undefined),
    [views],
  );
  const lastObservedViewRef = useRef<number | null>(null);
  const [isAboutModalOpen, setIsAboutModalOpen] = useState<boolean>(false);
  const [isKeyInfoModalOpen, setIsKeyInfoModalOpen] = useState<boolean>(false);
  const [isMobile, setIsMobile] = useState<boolean>(false);
  const [errorMessage, setErrorMessage] = useState<string>("");
  const [showError, setShowError] = useState<boolean>(false);
  const [isInMaintenance, setIsInMaintenance] = useState<boolean>(false);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [isSearchModalOpen, setIsSearchModalOpen] = useState<boolean>(false);
  const healthCheckIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const adjustTime = useClockSkew();
  const currentTimeRef = useRef(adjustTime(Date.now()));
  const wsRef = useRef<WebSocket | null>(null);
  const verifierPoolRef = useRef<ConsensusWorkerPool | null>(null);
  const verifiedQueueRef = useRef<VerifiedConsensusArtifact[]>([]);

  // Manage WebSocket lifecycle
  const handleSeedRef = useRef<typeof handleSeed>(null!);
  const handleNotarizedRef = useRef<typeof handleNotarization>(null!);
  const handleFinalizedRef = useRef<typeof handleFinalization>(null!);
  const isInitializedRef = useRef(false);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  const performClusterSwitch = useCallback((cluster: Cluster) => {
    console.log(`Switching to ${cluster} cluster`);

    // When switching, we close the old socket. The `onclose` handler for that socket
    // should not trigger a reconnect or error message.
    isInitializedRef.current = false;
    if (wsRef.current) {
      // Temporarily disable the onclose handler to prevent side-effects.
      wsRef.current.onclose = null;
      wsRef.current.close();
    }

    // Update the selected cluster
    setSelectedCluster(cluster);
  }, []);


  const handleClusterChange = (cluster: Cluster) => {
    if (cluster !== selectedCluster) {
      // Update URL and push to history
      const url = new URL(window.location.href);
      url.searchParams.set('cluster', cluster);
      window.history.pushState({ cluster }, '', url.toString());

      // Perform the cluster switch
      performClusterSwitch(cluster);
    }
  };

  // Effect to handle browser navigation (back/forward)
  useEffect(() => {
    const handlePopState = () => {
      const newCluster = getInitialCluster();
      if (newCluster !== selectedCluster) {
        performClusterSwitch(newCluster);
      }
    };

    window.addEventListener('popstate', handlePopState);
    return () => {
      window.removeEventListener('popstate', handlePopState);
    };
  }, [selectedCluster, performClusterSwitch]);

  // Reset state when the cluster changes
  useEffect(() => {
    setViews([]);
    lastObservedViewRef.current = null;
    verifiedQueueRef.current = [];
    setErrorMessage("");
    setShowError(false);
  }, [selectedCluster]);

  // Health check function
  const checkHealth = useCallback(async () => {
    try {
      const response = await fetch(`${getHttpBackendUrl(BACKEND_URL)}/health`, {
        method: "GET",
        headers: {
          "Cache-Control": "no-cache, no-store, must-revalidate",
          "Pragma": "no-cache",
          "Expires": "0"
        }
      });

      if (response.status !== 200) {
        console.error(`Health check failed with status: ${response.status}`);
        setIsInMaintenance(true);
        return false;
      } else {
        setIsInMaintenance(false);
        return true;
      }
    } catch (error) {
      console.error("Health check failed:", error);
      setIsInMaintenance(true);
      return false;
    } finally {
      // Mark loading as complete regardless of result
      setIsLoading(false);
    }
  }, [BACKEND_URL]);

  // Run health check on initial load and periodically
  useEffect(() => {
    // Run initial health check immediately - this will set isLoading to false when complete
    checkHealth();

    // Only set up periodic checks after the initial check
    const setupInterval = () => {
      healthCheckIntervalRef.current = setInterval(checkHealth, HEALTH_CHECK_INTERVAL);
    };

    // Wait for the initial check before setting up the interval
    const initialCheckTimeout = setTimeout(setupInterval, 1000);

    return () => {
      // Clean up interval and timeout on unmount
      clearTimeout(initialCheckTimeout);
      if (healthCheckIntervalRef.current) {
        clearInterval(healthCheckIntervalRef.current);
      }
    };
  }, [checkHealth]);

  // Initialize logo animations
  useEffect(() => {
    initializeLogoAnimations();
  }, []);

  // Check for mobile viewport
  useEffect(() => {
    const checkIfMobile = () => {
      setIsMobile(window.innerWidth < 768);
    };

    // Initial check
    checkIfMobile();

    // Add resize listener
    window.addEventListener('resize', checkIfMobile);

    return () => {
      window.removeEventListener('resize', checkIfMobile);
    };
  }, []);

  const resolveSeedLocation = useCallback((seed: SeedJs) => {
    if (MODE !== 'public' || PARTICIPANTS?.length || LOCATIONS.length === 0) {
      return { location: undefined, locationName: undefined };
    }

    const locationIndex = leader_index(seed, LOCATIONS.length);
    return {
      location: LOCATIONS[locationIndex][0],
      locationName: LOCATIONS[locationIndex][1],
    };
  }, [LOCATIONS, PARTICIPANTS]);

  const handleSeed = useCallback((seed: SeedJs, receivedAt: number) => {
    const view = seed.view + 1; // Next view is determined by seed - 1
    const observedAt = adjustTime(receivedAt);

    setViews((prevViews) => {
      // Create a copy of the current views that we'll modify
      let newViews = [...prevViews];
      const lastObservedView = lastObservedViewRef.current;

      // If we haven't observed any views yet, or if the new view is greater than the last observed view + 1,
      // handle potentially missed views
      if (lastObservedView === null || view > lastObservedView + 1) {
        const startViewIndex = lastObservedView !== null ? lastObservedView + 1 : view;

        // Add any missed views as skipped/timed out
        for (let missedView = startViewIndex; missedView < view; missedView++) {
          // Check if this view already exists
          const existingIndex = newViews.findIndex(v => v.view === missedView);

          if (existingIndex === -1) {
            // Set a timeout for unknown views
            const timeoutId = setTimeout(() => {
              setViews((currentViews) => {
                return currentViews.map((v) => {
                  // Only time out this specific view if it's still in unknown state
                  if (v.view === missedView && v.status === "unknown") {
                    return { ...v, status: "timed_out", timeoutId: undefined };
                  }
                  return v;
                });
              });
            }, TIMEOUT_DURATION);


            // Only add if it doesn't already exist
            newViews.unshift({
              view: missedView,
              location: undefined,
              locationName: undefined,
              status: "unknown",
              startTime: observedAt,
              timeoutId: timeoutId
            });
          }
        }
      }

      // Check if this view already exists
      const existingIndex = newViews.findIndex(v => v.view === view);

      if (existingIndex !== -1) {
        // If it exists and is already finalized or notarized, just update
        // the location and signature information without changing timing
        const existingStatus = newViews[existingIndex].status;
        if (existingStatus === "finalized" || existingStatus === "notarized") {
          const { location, locationName } = resolveSeedLocation(seed);

          // Only update location and signature info, preserve all timing and status
          newViews[existingIndex] = {
            ...newViews[existingIndex],
            location: location ?? newViews[existingIndex].location,
            locationName: locationName ?? newViews[existingIndex].locationName,
            signature: seed.signature,
          };

          return newViews;
        }

        // Skip processing for views with "unknown" status
        if (existingStatus === "unknown") {
          return newViews;
        }

        // If it exists but is in another state, clear its timeout but preserve everything else
        if (newViews[existingIndex].timeoutId) {
          clearTimeout(newViews[existingIndex].timeoutId);
        }
      }

      // Create the new view data
      const { location, locationName } = resolveSeedLocation(seed);
      const newView: ViewData = {
        view,
        location,
        locationName,
        status: "growing",
        startTime: observedAt,
        signature: seed.signature,
      };

      // Set a timeout for this specific view
      const timeoutId = setTimeout(() => {
        setViews((currentViews) => {
          return currentViews.map((v) => {
            // Only time out this specific view if it's still in growing state
            if (v.view === view && v.status === "growing") {
              return { ...v, status: "timed_out", timeoutId: undefined };
            }
            return v;
          });
        });
      }, TIMEOUT_DURATION);

      // Add timeoutId to the new view
      const viewWithTimeout = { ...newView, timeoutId };

      // Update or add the view
      if (existingIndex !== -1) {
        // Only update if necessary - preserve existing data that shouldn't change
        newViews[existingIndex] = {
          ...newViews[existingIndex],
          status: "growing",
          signature: seed.signature,
          timeoutId: timeoutId,
          location,
          locationName,
        };
      } else {
        // Add as new
        newViews.unshift(viewWithTimeout);
      }

      // Update the last observed view if this is a new maximum
      if (lastObservedView === null || view > lastObservedView) {
        lastObservedViewRef.current = view;
      }

      return retainNewestViews(newViews);
    });
  }, [adjustTime, resolveSeedLocation]);

  const handleNotarization = useCallback((notarized: NotarizedJs, receivedAt: number) => {
    const view = notarized.proof.view;
    const leaderLocation = resolveLeaderLocation(notarized.block?.leader, PARTICIPANTS, LOCATIONS);
    setViews((prevViews) => {
      const lastObservedView = lastObservedViewRef.current;
      if (lastObservedView === null || view > lastObservedView) {
        lastObservedViewRef.current = view;
      }
      const index = prevViews.findIndex((v) => v.view === view);

      // If the view exists and is already finalized, ignore this notarization completely
      if (index !== -1 && prevViews[index].status === "finalized") {
        return prevViews; // No changes needed, preserve finalized state
      }
      let newViews = [...prevViews];
      const currentTime = adjustTime(receivedAt);

      // Calculate a reasonable start time using the block timestamp if available
      let calculatedStartTime = currentTime;
      if (notarized.block && notarized.block.timestamp) {
        // The block timestamp is in milliseconds since epoch
        const blockTime = Number(notarized.block.timestamp);
        calculatedStartTime = blockTime;
      }

      if (index !== -1) {
        const viewData = prevViews[index];
        // Clear timeout if it exists
        if (viewData.timeoutId) {
          clearTimeout(viewData.timeoutId);
        }

        // Calculate actual notarization latency when we receive the notarization message
        let actualNotarizationLatency: number | undefined = undefined;
        if (notarized.block && notarized.block.timestamp) {
          const blockTime = Number(notarized.block.timestamp);
          if (blockTime > 0 && blockTime < currentTime) {
            actualNotarizationLatency = currentTime - blockTime;
          }
        }

        // Update the view with notarization data
        const updatedView: ViewData = {
          ...viewData,
          status: "notarized", // We already checked it's not finalized
          notarizationTime: currentTime,
          // If no start time exists, use the block timestamp
          startTime: viewData.startTime || calculatedStartTime,
          block: viewData.block || notarized.block, // Don't overwrite existing block data
          timeoutId: undefined,
          actualNotarizationLatency,
          ...leaderLocation,
        };

        newViews = [
          ...prevViews.slice(0, index),
          updatedView,
          ...prevViews.slice(index + 1),
        ];
      } else {
        // If view doesn't exist, create it with block timestamp as start time
        let actualNotarizationLatency: number | undefined = undefined;
        if (notarized.block && notarized.block.timestamp) {
          const blockTime = Number(notarized.block.timestamp);
          if (blockTime > 0 && blockTime < currentTime) {
            actualNotarizationLatency = currentTime - blockTime;
          }
        }
        newViews = [{
          view,
          location: leaderLocation?.location,
          locationName: leaderLocation?.locationName,
          status: "notarized",
          startTime: calculatedStartTime,
          notarizationTime: currentTime,
          block: notarized.block,
          actualNotarizationLatency,
        }, ...prevViews];
      }

      return retainNewestViews(newViews);
    });
  }, [adjustTime, LOCATIONS, PARTICIPANTS]);

  const handleFinalization = useCallback((finalized: FinalizedJs, receivedAt: number) => {
    const view = finalized.proof.view;
    const leaderLocation = resolveLeaderLocation(finalized.block?.leader, PARTICIPANTS, LOCATIONS);
    setViews((prevViews) => {
      const lastObservedView = lastObservedViewRef.current;
      if (lastObservedView === null || view > lastObservedView) {
        lastObservedViewRef.current = view;
      }
      const index = prevViews.findIndex((v) => v.view === view);
      let newViews = [...prevViews];
      const currentTime = adjustTime(receivedAt);

      // Calculate a reasonable start time using the block timestamp if available
      let calculatedStartTime = currentTime;
      if (finalized.block && finalized.block.timestamp) {
        // The block timestamp is in milliseconds since epoch
        const blockTime = Number(finalized.block.timestamp);
        calculatedStartTime = blockTime;
      }

      if (index !== -1) {
        const viewData = prevViews[index];
        // Clear timeout if it exists
        if (viewData.timeoutId) {
          clearTimeout(viewData.timeoutId);
        }

        // Calculate actual finalization latency when we receive the finalization message
        let actualFinalizationLatency: number | undefined = undefined;
        if (finalized.block && finalized.block.timestamp) {
          const blockTime = Number(finalized.block.timestamp);
          if (blockTime > 0 && blockTime < currentTime) {
            actualFinalizationLatency = currentTime - blockTime;
          }
        }

        // If already finalized, don't update
        if (viewData.status === "finalized") {
          return prevViews;
        }

        // Use existing data if available, without fabricating missing data
        const updatedView: ViewData = {
          ...viewData,
          status: "finalized",
          finalizationTime: currentTime,
          // Keep existing notarization time if available, but don't create one if missing
          // Keep existing start time or use block timestamp if none
          startTime: viewData.startTime || calculatedStartTime,
          block: finalized.block,
          timeoutId: undefined,
          actualNotarizationLatency: viewData.actualNotarizationLatency,
          actualFinalizationLatency,
          ...leaderLocation,
        };

        newViews = [
          ...prevViews.slice(0, index),
          updatedView,
          ...prevViews.slice(index + 1),
        ];
      } else {
        // If view doesn't exist, create it with just the data we have
        let actualFinalizationLatency: number | undefined = undefined;
        if (finalized.block && finalized.block.timestamp) {
          const blockTime = Number(finalized.block.timestamp);
          if (blockTime > 0 && blockTime < currentTime) {
            actualFinalizationLatency = currentTime - blockTime;
          }
        }
        newViews = [{
          view,
          location: leaderLocation?.location,
          locationName: leaderLocation?.locationName,
          status: "finalized",
          startTime: calculatedStartTime,
          // No notarization time observed yet
          finalizationTime: currentTime,
          block: finalized.block,
          actualFinalizationLatency,
        }, ...prevViews];
      }

      return retainNewestViews(newViews);
    });
  }, [adjustTime, LOCATIONS, PARTICIPANTS]);

  // Merge every verified artifact while limiting React rendering work.
  useEffect(() => {
    const interval = setInterval(() => {
      currentTimeRef.current = adjustTime(Date.now());
      const verified = verifiedQueueRef.current.splice(0);
      for (const { kind, artifact, receivedAt } of verified) {
        if (!artifact) {
          continue;
        }
        switch (kind) {
          case 0:
            handleSeedRef.current(artifact as SeedJs, receivedAt);
            break;
          case 1:
            handleNotarizedRef.current(artifact as NotarizedJs, receivedAt);
            break;
          case 2:
            handleFinalizedRef.current(artifact as FinalizedJs, receivedAt);
            break;
        }
      }
      if (verified.length === 0) {
        setViews(views => [...views]);
      }
    }, VIEW_RENDER_INTERVAL);
    return () => clearInterval(interval);
  }, [adjustTime]);

  // Update handler refs when the handlers change
  useEffect(() => {
    handleSeedRef.current = handleSeed;
  }, [handleSeed]);

  useEffect(() => {
    handleNotarizedRef.current = handleNotarization;
  }, [handleNotarization]);

  useEffect(() => {
    handleFinalizedRef.current = handleFinalization;
  }, [handleFinalization]);

  // WebSocket connection management with fixed single-connection approach
  useEffect(() => {
    // If loading, don't start
    if (isLoading) return;

    // Skip if in maintenance mode
    if (isInMaintenance) {
      // If there's an existing WebSocket connection, close it
      if (wsRef.current) {
        try {
          const ws = wsRef.current;
          wsRef.current = null;
          ws.close();
        } catch (err) {
          console.error("Error closing WebSocket during maintenance:", err);
        }
      }
      return;
    }

    // Skip if already initialized to prevent duplicate connections during development mode's double-invocation
    if (isInitializedRef.current) return;
    isInitializedRef.current = true;
    let cancelled = false;

    const connectWebSocket = () => {
      if (cancelled) {
        return;
      }
      // Clear any existing reconnection timers
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }

      // Close existing connection (if any)
      if (wsRef.current) {
        try {
          const ws = wsRef.current;
          wsRef.current = null;
          ws.close();
        } catch (err) {
          console.error("Error closing existing WebSocket:", err);
        }
      }

      // Create new WebSocket connection
      const wsCreationTime = Date.now();
      const ws = new WebSocket(`${getWebSocketBackendUrl(BACKEND_URL)}/consensus/ws`);
      wsRef.current = ws;
      ws.binaryType = "arraybuffer";

      ws.onopen = () => {
        if (cancelled) {
          ws.close(1000, "Component unmounted");
          return;
        }
        console.log(`WebSocket connected: ${BACKEND_URL}`);
        setErrorMessage("");
        setShowError(false);
      };

      ws.onmessage = (event) => {
        const data = new Uint8Array(event.data);
        const kind = data[0];
        if (kind <= 2) {
          verifierPoolRef.current?.verify(kind, data.slice(1), Date.now());
        }
      };

      ws.onerror = (error) => {
        console.error("WebSocket error:", error);
      };

      ws.onclose = (event) => {
        if (cancelled) {
          return;
        }
        console.error(`WebSocket closed with code: ${event.code}`);

        // Check for potential rate limiting (code 1006 is "Abnormal Closure")
        if (event.code === 1006) {
          // If connection closed very quickly, likely rate-limited
          const timeSinceStarted = Date.now() - wsCreationTime;
          if (timeSinceStarted < 1000) {
            setErrorMessage("Too many connection attempts from your IP. Try connecting again in a few minutes.");
            setShowError(true);

            // Clear reference to prevent reconnection
            wsRef.current = null;
          } else {
            setErrorMessage("Disconnected from server. Reconnecting...");
            setShowError(true);
          }
        }

        // Only attempt to reconnect if we still have a reference to this websocket (and we didn't detect a rate limit error)
        if (wsRef.current === ws) {
          reconnectTimeoutRef.current = setTimeout(() => {
            reconnectTimeoutRef.current = null;
            if (!cancelled) {
              connectWebSocket();
            }
          }, 11000);
        }
      };
    };

    const setup = async () => {
      await init();
      if (cancelled) {
        return;
      }
      const createWorker = () => new Worker(new URL("./consensusWorker.ts", import.meta.url));
      const workers: Worker[] = [];
      try {
        for (let index = 0; index < consensusWorkerCount(navigator.hardwareConcurrency || 4); index++) {
          workers.push(createWorker());
        }
      } catch (error) {
        workers.forEach((worker) => worker.terminate());
        throw error;
      }
      if (cancelled) {
        workers.forEach((worker) => worker.terminate());
        return;
      }
      verifierPoolRef.current = new ConsensusWorkerPool(
        workers,
        PUBLIC_KEY,
        (verified) => verifiedQueueRef.current.push(verified),
        () => {
          setErrorMessage("A consensus verifier stopped unexpectedly. Refresh to reconnect.");
          setShowError(true);
        },
        createWorker,
        standardCertificates,
      );
      connectWebSocket();
    };

    setup().catch((error) => {
      if (cancelled) {
        return;
      }
      console.error("Unable to initialize consensus verifiers:", error);
      isInitializedRef.current = false;
      setErrorMessage("Consensus verification could not start. Refresh to retry.");
      setShowError(true);
    });

    // Cleanup function when component unmounts
    return () => {
      cancelled = true;
      isInitializedRef.current = false;
      // Clear any reconnection timers
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }

      verifierPoolRef.current?.terminate();
      verifierPoolRef.current = null;
      verifiedQueueRef.current = [];

      // Close and clean up the websocket
      if (wsRef.current) {
        const ws = wsRef.current;
        wsRef.current = null; // Clear reference first to prevent reconnection attempts
        try {
          ws.close(1000, "Component unmounting");
        } catch (err) {
          console.error("Error closing WebSocket during cleanup:", err);
        }
      }
    };
  }, [isLoading, isInMaintenance, BACKEND_URL, PUBLIC_KEY, standardCertificates]);

  // Loading state - show nothing until we get the result of the health check
  if (isLoading) {
    return null;
  }

  // If we're in maintenance mode, show the maintenance page
  if (isInMaintenance) {
    return <MaintenancePage />;
  }

  return (
    <div className="app-container">
      <ErrorNotification
        message={errorMessage}
        isVisible={showError}
        onDismiss={() => setShowError(false)}
        autoHideDuration={15000}
      />
      <header className="app-header">
        <div className="logo-container">
          <div className="logo-line">
            <span className="edge-logo-symbol">+</span>
            <span className="horizontal-logo-symbol">~</span>
            <span className="horizontal-logo-symbol"> </span>
            <span className="horizontal-logo-symbol">-</span>
            <span className="horizontal-logo-symbol">+</span>
            <span className="horizontal-logo-symbol">-</span>
            <span className="horizontal-logo-symbol">+</span>
            <span className="horizontal-logo-symbol"> </span>
            <span className="horizontal-logo-symbol">-</span>
            <span className="horizontal-logo-symbol">+</span>
            <span className="horizontal-logo-symbol">-</span>
            <span className="horizontal-logo-symbol">~</span>
            <span className="horizontal-logo-symbol">~</span>
            <span className="edge-logo-symbol">*</span>
          </div>
          <div className="logo-line">
            <span className="vertical-logo-symbol">|</span>
            <span className="logo-text"> commonware </span>
            <span className="vertical-logo-symbol"> </span>
          </div>
          <div className="logo-line">
            <span className="edge-logo-symbol">*</span>
            <span className="horizontal-logo-symbol">~</span>
            <span className="horizontal-logo-symbol">+</span>
            <span className="horizontal-logo-symbol">+</span>
            <span className="horizontal-logo-symbol">-</span>
            <span className="horizontal-logo-symbol"> </span>
            <span className="horizontal-logo-symbol">~</span>
            <span className="horizontal-logo-symbol">-</span>
            <span className="horizontal-logo-symbol">+</span>
            <span className="horizontal-logo-symbol"> </span>
            <span className="horizontal-logo-symbol">-</span>
            <span className="horizontal-logo-symbol">*</span>
            <span className="horizontal-logo-symbol">-</span>
            <span className="edge-logo-symbol">+</span>
          </div>
        </div>
        <div className="about-button-container">
          <button
            className="search-header-button"
            onClick={() => setIsSearchModalOpen(true)}
          >
            ⚲
          </button>
          <button
            className="key-header-button"
            onClick={() => setIsKeyInfoModalOpen(true)}
          >
            ⚷︎
          </button>
          <button
            className={`about-header-button ${selectedCluster === 'usa' ? 'usa-cluster' : ''}`}
            onClick={() => setIsAboutModalOpen(true)}
          >
            🌐︎
          </button>
        </div>
      </header>

      <main className="app-main">
        {/* Map - only show in public mode */}
        {MODE === 'public' && (
          <div className="map-container">
            <MapContainer key={selectedCluster} center={center} zoom={1} style={{ height: "100%", width: "100%" }} zoomControl={false} scrollWheelZoom={false} doubleClickZoom={false} touchZoom={false} dragging={false}>
              <TileLayer
                url="https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png"
                attribution='&copy; OSM | &copy; CARTO</a>'
              />
              {mappedView?.location !== undefined && (
                <Marker
                  key={mappedView.view}
                  position={mappedView.location}
                  icon={markerIcon}
                >
                  <Popup>
                    <div>
                      <strong>View: {mappedView.view}</strong><br />
                      Location: {mappedView.locationName}<br />
                      Status: {mappedView.status}<br />
                      {mappedView.block && (
                        <>Block Height: {mappedView.block.height}<br /></>
                      )}
                      {mappedView.startTime && (
                        <>Start Time: {new Date(mappedView.startTime).toLocaleTimeString()}<br /></>
                      )}
                    </div>
                  </Popup>
                </Marker>
              )}
              <MapOverlay numValidators={LOCATIONS.length} />
            </MapContainer>
          </div>
        )}

        {/* Stats Section */}
        <StatsSection
          views={views}
          selectedCluster={selectedCluster}
          onClusterChange={handleClusterChange}
          configs={allConfigs}
        />

        {/* Bars with integrated legend */}
        <div className="bars-container">
          <div className="bars-header">
            <h2 className="bars-title">Timeline</h2>
            <div className="legend-container">
              <LegendItem color={leaderIndicator.color} label={leaderIndicator.label} />
              <LegendItem color={"#000"} label="Locked" />
              <LegendItem color={"#228B22ff"} label="Finalized" />
              <LegendItem color={timelineIdentifier.color} label={timelineIdentifier.label} textMarker />
            </div>
          </div>

          <div className="bars-list">
            {views.slice(0, MAX_RENDERED_VIEWS).map((viewData) => (
              <Bar
                key={viewData.view}
                viewData={viewData}
                currentTime={currentTimeRef.current}
                isMobile={isMobile}
                standardCertificates={standardCertificates}
              />
            ))}
          </div>
        </div>
      </main >

      <footer className="footer">
        <div className="socials">
          <a href="https://commonware.xyz/hiring.html">Hiring</a>
          <a href="https://github.com/commonwarexyz/alto">GitHub</a>
          <a href="https://x.com/commonwarexyz">X</a>
        </div>
        &copy; {new Date().getFullYear()} Commonware, Inc. All rights reserved.
      </footer>

      <AboutModal
        isOpen={isAboutModalOpen}
        onClose={() => setIsAboutModalOpen(false)}
        standardCertificates={standardCertificates}
      />
      <KeyInfoModal
        isOpen={isKeyInfoModalOpen}
        onClose={() => setIsKeyInfoModalOpen(false)}
        publicKeyHex={clusterConfig.PUBLIC_KEY_HEX}
      />
      <SearchModal
        isOpen={isSearchModalOpen}
        onClose={() => setIsSearchModalOpen(false)}
        clusterConfig={clusterConfig}
      />
    </div >
  );
};

interface LegendItemProps {
  color: string;
  label: string;
  textMarker?: boolean;
}

const LegendItem: React.FC<LegendItemProps> = ({ color, label, textMarker = false }) => {
  return (
    <div className="legend-item">
      {textMarker ? (
        <span className="legend-text-marker" style={{ color }}>abc</span>
      ) : (
        <div className="legend-color" style={{ backgroundColor: color }}></div>
      )}
      <span className="legend-label">{label}</span>
    </div>
  );
};

interface BarProps {
  viewData: ViewData;
  currentTime: number;
  isMobile: boolean;
  standardCertificates: boolean;
  maxContainerWidth?: number;
}

// Replace the existing Bar component with this updated version

const Bar: React.FC<BarProps> = ({ viewData, currentTime, isMobile, standardCertificates }) => {
  const { view, status, startTime, notarizationTime, finalizationTime, signature, block, actualNotarizationLatency, actualFinalizationLatency } = viewData;
  const timelineIdentifier = getTimelineIdentifier(standardCertificates, signature, block?.digest);
  const [measuredWidth, setMeasuredWidth] = useState(isMobile ? 200 : 500); // Reasonable default
  const barContainerRef = useRef<HTMLDivElement>(null);

  // Measure width after component mounts and on resize
  useEffect(() => {
    const updateWidth = () => {
      if (barContainerRef.current) {
        const width = barContainerRef.current.clientWidth - (isMobile ? 4 : 8);
        setMeasuredWidth(width);
      }
    };

    // Initial measurement
    updateWidth();

    // Add resize listener
    window.addEventListener('resize', updateWidth);

    return () => {
      window.removeEventListener('resize', updateWidth);
    };
  }, [isMobile]);

  const viewInfoWidth = isMobile ? 50 : 80;
  const minBarWidth = isMobile ? 30 : 60; // Minimum width for completed bars
  const minSegmentWidth = isMobile ? 15 : 30; // Minimum segment width

  // Calculate widths for different stages
  let totalWidth = 0;
  let notarizedWidth = 0;
  let finalizedWidth = 0;

  // Get actual latency values for calculations
  let growingLatency = 0;
  let notarizedLatency = 0;
  let finalizedLatency = 0;

  // Format timing texts with improved clarity
  let inBarText = ""; // Text to display inside the bar (block info only)
  let notarizedLatencyText = ""; // Text to display below the notarized point
  let finalizedLatencyText = ""; // Text to display below the finalized point
  let growingLatencyText = ""; // Text to display below the growing bar tip

  // Calculate latencies and set text
  if (status === "growing" || status === "unknown") {
    growingLatency = currentTime - startTime;
    if (growingLatency > 1) {
      growingLatencyText = `${Math.round(growingLatency)}ms`;
    }
  } else if (status === "notarized") {
    if (actualNotarizationLatency) {
      notarizedLatency = actualNotarizationLatency;
      notarizedLatencyText = `${Math.round(notarizedLatency)}ms`;
    } else if (notarizationTime) {
      notarizedLatency = notarizationTime - startTime;
      if (notarizedLatency > 0) {
        notarizedLatencyText = `${Math.round(notarizedLatency)}ms`;
      }
    }
  } else if (status === "finalized") {
    // Calculate notarization latency if available
    if (notarizationTime) {
      if (actualNotarizationLatency) {
        notarizedLatency = actualNotarizationLatency;
        notarizedLatencyText = `${Math.round(notarizedLatency)}ms`;
      } else {
        notarizedLatency = notarizationTime - startTime;
        if (notarizedLatency > 0) {
          notarizedLatencyText = `${Math.round(notarizedLatency)}ms`;
        }
      }
    }

    // Calculate finalization latency
    if (actualFinalizationLatency) {
      finalizedLatency = actualFinalizationLatency;
      finalizedLatencyText = `${Math.round(finalizedLatency)}ms`;
    } else if (finalizationTime) {
      finalizedLatency = finalizationTime - startTime;
      if (finalizedLatency > 0) {
        finalizedLatencyText = `${Math.round(finalizedLatency)}ms`;
      }
    }
  }

  // Calculate the widths for different bar segments
  if (status === "growing" || status === "unknown") {
    totalWidth = scaleTimelineWidth(growingLatency, measuredWidth);
    // Ensure growing bars are visible but don't exceed available width
    totalWidth = Math.min(Math.max(totalWidth, growingLatency > 50 ? minSegmentWidth : 0), measuredWidth);
  } else if (status === "notarized") {
    totalWidth = scaleTimelineWidth(notarizedLatency, measuredWidth);
    // Ensure notarized bars meet minimum width
    totalWidth = Math.max(totalWidth, minBarWidth);
  } else if (status === "finalized") {
    if (notarizationTime) {
      // Calculate notarized segment width
      notarizedWidth = scaleTimelineWidth(notarizedLatency, measuredWidth);
      notarizedWidth = Math.max(notarizedWidth, minSegmentWidth);

      // Calculate finalized segment width (difference between finalization and notarization)
      const finalizationDelta = finalizedLatency - notarizedLatency;
      if (finalizationDelta > 0) {
        finalizedWidth = scaleTimelineWidth(finalizationDelta, measuredWidth);
        finalizedWidth = Math.max(finalizedWidth, minSegmentWidth / 2);
      }

      totalWidth = notarizedWidth + finalizedWidth;
    } else {
      // Without notarization time, use the entire bar for finalization
      totalWidth = scaleTimelineWidth(finalizedLatency, measuredWidth);
      totalWidth = Math.max(totalWidth, minBarWidth);
    }
  } else if (status === "timed_out") {
    // Timed out - always full width
    totalWidth = measuredWidth;
  }

  // Ensure total width doesn't exceed available space
  totalWidth = Math.min(totalWidth, measuredWidth);

  // Set block info text
  if (status === "timed_out") {
    inBarText = "MISSING";
  } else if (status === "unknown") {
    inBarText = "PENDING";
  } else if (block) {
    inBarText = `#${block.height} | ${hexUint8Array(block.digest)}`;
  }

  // Calculate positions for timing labels to prevent overlap
  const labelWidth = isMobile ? 30 : 45; // Estimated width of a timing label
  const minLabelSpacing = labelWidth + 5; // Increased minimum space needed between labels

  // Calculate ideal positions for notarization and finalization labels (centered on their respective points)
  let growingLabelPosition = Math.max(0, totalWidth - (labelWidth / 2));
  let notarizedLabelPosition = notarizedWidth > 0 ? Math.max(0, notarizedWidth - (labelWidth / 2)) : 0;
  let finalizedLabelPosition = totalWidth > 0 ? Math.max(0, totalWidth - (labelWidth / 2)) : 0;

  // Constraint to ensure labels don't overflow right edge
  const maxLabelPosition = measuredWidth - labelWidth;
  growingLabelPosition = Math.min(growingLabelPosition, maxLabelPosition);
  notarizedLabelPosition = Math.min(notarizedLabelPosition, maxLabelPosition);
  finalizedLabelPosition = Math.min(finalizedLabelPosition, maxLabelPosition);

  // Check if labels would overlap
  const wouldOverlap = status === "finalized" &&
    notarizationTime &&
    (finalizedLabelPosition - notarizedLabelPosition < minLabelSpacing);

  // Adjust positions if overlap detected
  if (wouldOverlap) {
    // Prioritize the finalization label position since it's usually more important
    // Then push the notarization label to the left to ensure minimum spacing
    notarizedLabelPosition = finalizedLabelPosition - minLabelSpacing;

    // If this would push the notarization label off the left edge, adjust both
    if (notarizedLabelPosition < 0) {
      notarizedLabelPosition = 0;

      // Only move the finalization label if there's enough room for both
      if (minLabelSpacing < totalWidth) {
        finalizedLabelPosition = minLabelSpacing;
      } else {
        // Not enough room for both, keep finalization at the far right
        finalizedLabelPosition = totalWidth - (labelWidth / 2);
      }
    }
  }

  // Determine what content to render in bar - for finalized without notarization
  const renderFinalizedWithoutNotarization = status === "finalized" && !notarizationTime;

  return (
    <div className="bar-row">
      <div className="view-info" style={{ width: `${viewInfoWidth}px` }}>
        <div className="view-number">{view}</div>
        <div
          className="view-identifier"
          style={{ color: timelineIdentifier.color }}
          title={timelineIdentifier.fullValue ? `${timelineIdentifier.label}: ${timelineIdentifier.fullValue}` : undefined}
        >
          {timelineIdentifier.value}
        </div>
      </div>

      <div className="bar-container" ref={barContainerRef}>
        {/* Main bar container */}
        <div
          className="bar-main"
          style={{
            width: `${totalWidth}px`,
          }}
        >
          {/* Timed out or Growing state */}
          {(status === "timed_out" || status === "growing" || status === "unknown") && (
            <div
              className={`bar-segment ${status === "timed_out" ? "timed-out" :
                status === "unknown" ? "unknown" : "growing"
                }`}
              style={{ width: "100%" }}
            >
              {inBarText}
            </div>
          )}

          {/* Notarized state */}
          {status === "notarized" && (
            <>
              <div
                className="bar-segment growing"
                style={{ width: "100%" }}
              >
                {inBarText}
              </div>
              <div
                className="marker notarization-marker"
                style={{
                  right: 0,
                }}
              />
            </>
          )}

          {/* Finalized state with notarization */}
          {status === "finalized" && !renderFinalizedWithoutNotarization && (
            <>
              {/* Base segment (proposal or seed to notarization) */}
              <div
                className="bar-segment growing"
                style={{ width: `${notarizedWidth}px` }}
              >
                {inBarText}
              </div>

              {/* Add notarization marker at the junction point between segments */}
              <div
                className="marker notarization-marker"
                style={{
                  left: `${notarizedWidth}px`,
                  right: 'auto',
                }}
                title="Notarization point"
              />

              {/* Notarized to finalized segment */}
              <div
                className="bar-segment finalized"
                style={{
                  left: `${notarizedWidth}px`,
                  width: `${finalizedWidth}px`,
                }}
              />
            </>
          )}

          {/* Finalized state without notarization - just a single finalized bar */}
          {renderFinalizedWithoutNotarization && (
            <div
              className="bar-segment finalized"
              style={{ width: "100%" }}
            >
              {inBarText}
            </div>
          )}

          {/* Marker for finalization point */}
          {status === "finalized" && (
            <div
              className="marker finalization-marker"
              style={{
                right: 0,
              }}
            />
          )}
        </div>

        {/* Timing information underneath */}
        <div className="timing-info">
          {/* Show timing for all states that need it */}
          {(signature || status === "unknown") && (
            <>
              {/* Latency at notarization point - only show if text exists and we have notarization */}
              {!renderFinalizedWithoutNotarization &&
                (status === "notarized" || status === "finalized") &&
                notarizedWidth > 0 &&
                notarizedLatencyText && (
                  <div
                    className="latency-text notarized-latency"
                    style={{
                      left: `${notarizedLabelPosition}px`,
                      color: "#000",
                    }}
                  >
                    {notarizedLatencyText}
                  </div>
                )}

              {/* Total latency marker for finalized views - only show if text exists */}
              {status === "finalized" && finalizedLatencyText && (
                <div
                  className="latency-text finalized-latency"
                  style={{
                    left: `${finalizedLabelPosition}px`,
                    color: "#228B22ff",
                  }}
                >
                  {finalizedLatencyText}
                </div>
              )}

              {/* Latency for growing bars - follows the tip - only show if text exists */}
              {(status === "growing" || status === "unknown") && growingLatencyText && (
                <div
                  className="latency-text growing-latency"
                  style={{
                    left: `${growingLabelPosition}px`,
                  }}
                >
                  {growingLatencyText}
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default App;
