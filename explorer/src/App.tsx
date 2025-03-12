import React, { useEffect, useState, useRef, useCallback } from "react";
import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
import { LatLng, DivIcon } from "leaflet";
import "leaflet/dist/leaflet.css";
import init, { parse_seed, parse_notarized, parse_finalized, leader_index } from "./alto_types/alto_types.js";
import { BACKEND_URL, LOCATIONS, PUBLIC_KEY_HEX } from "./config";
import { SeedJs, NotarizedJs, FinalizedJs, BlockJs } from "./types";
import "./App.css";

/**
 * Converts a hexadecimal string to a Uint8Array.
 * @param hex - The hexadecimal string to convert.
 * @returns A Uint8Array representation of the hex string.
 * @throws Error if the hex string has an odd length or contains invalid characters.
 */
function hexToUint8Array(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) {
    throw new Error("Hex string must have an even length");
  }
  const bytes: number[] = [];
  for (let i = 0; i < hex.length; i += 2) {
    const byteStr = hex.substr(i, 2);
    const byte = parseInt(byteStr, 16);
    if (isNaN(byte)) {
      throw new Error(`Invalid hex character in string: ${byteStr}`);
    }
    bytes.push(byte);
  }
  return new Uint8Array(bytes);
}

// Export PUBLIC_KEY as a Uint8Array for use in the application
const PUBLIC_KEY = hexToUint8Array(PUBLIC_KEY_HEX);

type ViewStatus = "growing" | "notarized" | "finalized" | "timed_out";

interface ViewData {
  view: number;
  location?: [number, number];
  locationName?: string;
  status: ViewStatus;
  startTime: number;
  notarizationTime?: number;
  finalizationTime?: number;
  signature?: Uint8Array;
  block?: BlockJs;
  timeoutId?: NodeJS.Timeout;
}

const TIMEOUT_DURATION = 1000; // 1 second

const markerIcon = new DivIcon({
  className: "custom-div-icon",
  html: `<div style="
      background-color: #aaa;
      width: 16px;
      height: 16px;
      border-radius: 50%;
      border: 1px solid black;
    "></div>`,
  iconSize: [12, 12],
  iconAnchor: [6, 6],
});

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
  const [views, setViews] = useState<ViewData[]>([]);
  const [lastObservedView, setLastObservedView] = useState<number | null>(null);
  const [isMobile, setIsMobile] = useState<boolean>(false);
  const [isConnected, setIsConnected] = useState<boolean>(false);
  const currentTimeRef = useRef(Date.now());
  const wsRef = useRef<WebSocket | null>(null);

  // Manage WebSocket lifecycle
  const handleSeedRef = useRef<typeof handleSeed>(null!);
  const handleNotarizedRef = useRef<typeof handleNotarization>(null!);
  const handleFinalizedRef = useRef<typeof handleFinalization>(null!);
  const isInitializedRef = useRef(false);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);

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

  const handleSeed = useCallback((seed: SeedJs) => {
    const view = seed.view + 1; // Next view is determined by seed - 1

    setViews((prevViews) => {
      // Create a copy of the current views that we'll modify
      let newViews = [...prevViews];

      // If we haven't observed any views yet, or if the new view is greater than the last observed view + 1,
      // handle potentially missed views
      if (lastObservedView === null || view > lastObservedView + 1) {
        const startViewIndex = lastObservedView !== null ? lastObservedView + 1 : view;

        // Add any missed views as skipped/timed out
        for (let missedView = startViewIndex; missedView < view; missedView++) {
          // Check if this view already exists
          const existingIndex = newViews.findIndex(v => v.view === missedView);

          if (existingIndex === -1) {
            // Only add if it doesn't already exist
            newViews.unshift({
              view: missedView,
              location: undefined,
              locationName: undefined,
              status: "timed_out",
              startTime: Date.now(),
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
          const locationIndex = leader_index(seed.signature, LOCATIONS.length);

          // Only update location and signature info, preserve all timing and status
          newViews[existingIndex] = {
            ...newViews[existingIndex],
            location: LOCATIONS[locationIndex][0],
            locationName: LOCATIONS[locationIndex][1],
            signature: seed.signature,
          };

          return newViews;
        }

        // If it exists but is in another state, clear its timeout but preserve everything else
        if (newViews[existingIndex].timeoutId) {
          clearTimeout(newViews[existingIndex].timeoutId);
        }
      }

      // Create the new view data
      const locationIndex = leader_index(seed.signature, LOCATIONS.length);
      const newView: ViewData = {
        view,
        location: LOCATIONS[locationIndex][0],
        locationName: LOCATIONS[locationIndex][1],
        status: "growing",
        startTime: Date.now(),
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
          location: LOCATIONS[locationIndex][0],
          locationName: LOCATIONS[locationIndex][1],
        };
      } else {
        // Add as new
        newViews.unshift(viewWithTimeout);
      }

      // Update the last observed view if this is a new maximum
      if (lastObservedView === null || view > lastObservedView) {
        setLastObservedView(view);
      }

      // Limit the number of views to 50
      if (newViews.length > 50) {
        // Clean up any timeouts for views we're about to remove
        for (let i = 50; i < newViews.length; i++) {
          if (newViews[i].timeoutId) {
            clearTimeout(newViews[i].timeoutId);
          }
        }
        newViews = newViews.slice(0, 50);
      }

      return newViews;
    });
  }, [lastObservedView]);

  const handleNotarization = useCallback((notarized: NotarizedJs) => {
    const view = notarized.proof.view;
    setViews((prevViews) => {
      const index = prevViews.findIndex((v) => v.view === view);
      let newViews = [...prevViews];
      const currentTime = Date.now();

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

        // Only update if not already finalized (finalized is the final state)
        if (viewData.status === "finalized") {
          return prevViews;
        }

        const updatedView: ViewData = {
          ...viewData,
          status: "notarized",
          notarizationTime: currentTime,
          // If no start time exists, use the block timestamp
          startTime: viewData.startTime || calculatedStartTime,
          block: notarized.block,
          timeoutId: undefined,
        };

        newViews = [
          ...prevViews.slice(0, index),
          updatedView,
          ...prevViews.slice(index + 1),
        ];
      } else {
        // If view doesn't exist, create it with block timestamp as start time
        newViews = [{
          view,
          location: undefined,
          locationName: undefined,
          status: "notarized",
          startTime: calculatedStartTime,
          notarizationTime: currentTime,
          block: notarized.block,
        }, ...prevViews];
      }

      // Limit the number of views to 50
      if (newViews.length > 50) {
        // Clean up any timeouts for views we're about to remove
        for (let i = 50; i < newViews.length; i++) {
          if (newViews[i].timeoutId) {
            clearTimeout(newViews[i].timeoutId);
          }
        }
        newViews = newViews.slice(0, 50);
      }

      return newViews;
    });
  }, []);

  const handleFinalization = useCallback((finalized: FinalizedJs) => {
    const view = finalized.proof.view;
    setViews((prevViews) => {
      const index = prevViews.findIndex((v) => v.view === view);
      let newViews = [...prevViews];
      const currentTime = Date.now();

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
        };

        newViews = [
          ...prevViews.slice(0, index),
          updatedView,
          ...prevViews.slice(index + 1),
        ];
      } else {
        // If view doesn't exist, create it with just the data we have
        newViews = [{
          view,
          location: undefined,
          locationName: undefined,
          status: "finalized",
          startTime: calculatedStartTime,
          // No notarization time observed yet
          finalizationTime: currentTime,
          block: finalized.block,
        }, ...prevViews];
      }

      // Limit the number of views to 50
      if (newViews.length > 50) {
        // Clean up any timeouts for views we're about to remove
        for (let i = 50; i < newViews.length; i++) {
          if (newViews[i].timeoutId) {
            clearTimeout(newViews[i].timeoutId);
          }
        }
        newViews = newViews.slice(0, 50);
      }

      return newViews;
    });
  }, []);

  // Update current time every 50ms to force re-render for growing bars
  useEffect(() => {
    const interval = setInterval(() => {
      currentTimeRef.current = Date.now();
      // Force re-render without relying on state updates
      setViews(views => [...views]);
    }, 50);
    return () => clearInterval(interval);
  }, []);

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
    // Skip if already initialized to prevent duplicate connections during development mode's double-invocation
    if (isInitializedRef.current) return;
    isInitializedRef.current = true;

    const connectWebSocket = () => {
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
      const ws = new WebSocket(BACKEND_URL);
      wsRef.current = ws;
      ws.binaryType = "arraybuffer";

      ws.onopen = () => {
        console.log("WebSocket connected");
        setIsConnected(true);
      };

      ws.onmessage = (event) => {
        const data = new Uint8Array(event.data);
        const kind = data[0];
        const payload = data.slice(1);

        switch (kind) {
          case 0: // Seed
            const seed = parse_seed(PUBLIC_KEY, payload);
            if (seed) handleSeedRef.current(seed);
            break;
          case 1: // Notarization
            const notarized = parse_notarized(PUBLIC_KEY, payload);
            if (notarized) handleNotarizedRef.current(notarized);
            break;
          case 3: // Finalization
            const finalized = parse_finalized(PUBLIC_KEY, payload);
            if (finalized) handleFinalizedRef.current(finalized);
            break;
        }
      };

      ws.onerror = (error) => {
        console.error("WebSocket error:", error);
      };

      ws.onclose = (event) => {
        console.error(`WebSocket closed with code: ${event.code}`);
        setIsConnected(false);

        // Only attempt to reconnect if we still have a reference to this websocket
        if (wsRef.current === ws) {
          reconnectTimeoutRef.current = setTimeout(() => {
            reconnectTimeoutRef.current = null;
            connectWebSocket();
          }, 5000);
        }
      };
    };

    const setup = async () => {
      await init();
      connectWebSocket();
    };

    setup();

    // Cleanup function when component unmounts
    return () => {
      // Clear any reconnection timers
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }

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
  }, []);

  // Define center using LatLng
  const center = new LatLng(0, 0);

  return (
    <div className="app-container">
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
        <div className="connection-status">
          <div className={`status-indicator ${isConnected ? "connected" : "disconnected"}`}></div>
          <span>{isConnected ? "Connected" : "Disconnected"}</span>
        </div>
      </header>

      <main className="app-main">
        {/* Network Key */}
        <div className="public-key-display">
          <span className="public-key-label">Network Key:</span>
          <span className="public-key-value">{PUBLIC_KEY_HEX}</span>
        </div>

        {/* Map */}
        <div className="map-container">
          <MapContainer center={center} zoom={1} style={{ height: "100%", width: "100%" }}>
            <TileLayer
              url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
            />
            {views.length > 0 && views[0].location !== undefined && (
              <Marker
                key={views[0].view}
                position={views[0].location}
                icon={markerIcon}
              >
                <Popup>
                  <div>
                    <strong>View: {views[0].view}</strong><br />
                    Location: {views[0].locationName}<br />
                    Status: {views[0].status}<br />
                    {views[0].block && (
                      <>Block Height: {views[0].block.height}<br /></>
                    )}
                    {views[0].startTime && (
                      <>Start Time: {new Date(views[0].startTime).toLocaleTimeString()}<br /></>
                    )}
                  </div>
                </Popup>
              </Marker>
            )}
          </MapContainer>
        </div>

        {/* Bars with integrated legend */}
        <div className="bars-container">
          <div className="bars-header">
            <h2 className="bars-title">Views</h2>
            <div className="legend-container">
              <LegendItem color="#aaa" label="Notarization" />
              <LegendItem color="#d9ead3ff" label="Finalization" />
              <LegendItem color="#274e13ff" label="Finalized" />
              <LegendItem color="#f4ccccff" label="Skipped" />
            </div>
          </div>
          <div className="bars-list">
            {views.slice(0, 50).map((viewData) => (
              <Bar
                key={viewData.view}
                viewData={viewData}
                currentTime={currentTimeRef.current}
                isMobile={isMobile}
              />
            ))}
          </div>
        </div>
      </main >

      <footer className="footer">
        &copy; {new Date().getFullYear()} Commonware, Inc. All rights reserved.
      </footer>
    </div >
  );
};

interface LegendItemProps {
  color: string;
  label: string;
}

const LegendItem: React.FC<LegendItemProps> = ({ color, label }) => {
  return (
    <div className="legend-item">
      <div className="legend-color" style={{ backgroundColor: color }}></div>
      <span className="legend-label">{label}</span>
    </div>
  );
};

interface BarProps {
  viewData: ViewData;
  currentTime: number;
  isMobile: boolean;
  maxContainerWidth?: number;
}

const Bar: React.FC<BarProps> = ({ viewData, currentTime, isMobile }) => {
  const { view, status, startTime, notarizationTime, finalizationTime, signature, block } = viewData;
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
  const growthRate = measuredWidth / TIMEOUT_DURATION; // pixels per ms
  const minBarWidth = isMobile ? 30 : 60; // Minimum width for completed bars
  const minSegmentWidth = isMobile ? 15 : 30; // Minimum segment width

  // Calculate widths for different stages
  let totalWidth = 0;
  let notarizedWidth = 0;
  let finalizedWidth = 0;

  // Calculate the current or final width
  if (status === "growing") {
    const elapsed = currentTime - startTime;
    totalWidth = elapsed <= 50 ? 0 : Math.min(elapsed * growthRate, measuredWidth);
  } else if (status === "notarized" || status === "finalized") {
    // Calculate notarization segment if notarization time exists
    if (notarizationTime) {
      const notarizeElapsed = notarizationTime - startTime;
      // If time is too small, use a minimum width
      if (notarizeElapsed < 50) {
        notarizedWidth = minSegmentWidth * 1.5;
      } else {
        notarizedWidth = Math.min(notarizeElapsed * growthRate, measuredWidth);
      }
      // Ensure minimum width
      notarizedWidth = Math.max(notarizedWidth, minSegmentWidth);
    }

    // For finalized status with notarization time, calculate finalization segment
    if (status === "finalized" && finalizationTime) {
      if (notarizationTime) {
        // With notarization time, calculate based on time difference
        const finalizeElapsed = finalizationTime - notarizationTime;
        // If time is too small, use a minimum width
        if (finalizeElapsed < 50) {
          finalizedWidth = minSegmentWidth;
        } else {
          finalizedWidth = Math.min(finalizeElapsed * growthRate, measuredWidth - notarizedWidth);
        }
        // Ensure minimum width
        finalizedWidth = Math.max(finalizedWidth, minSegmentWidth / 2);
      } else {
        // Without notarization time, use the entire bar for finalization
        // (from start to finalization)
        const totalElapsed = finalizationTime - startTime;
        totalWidth = Math.min(totalElapsed * growthRate, measuredWidth);
        totalWidth = Math.max(totalWidth, minBarWidth);
        notarizedWidth = 0; // No notarization segment
        finalizedWidth = 0; // No separate finalization segment, using the full bar instead
      }
    }

    // Set total width based on segments if notarization exists
    if (notarizationTime) {
      totalWidth = notarizedWidth + finalizedWidth;
    }

    // Ensure the total is at least the minimum bar width when using segments
    if (notarizationTime && totalWidth < minBarWidth) {
      // Adjust segments proportionally
      const ratio = notarizedWidth / (notarizedWidth + finalizedWidth || 1);
      notarizedWidth = minBarWidth * ratio;
      finalizedWidth = minBarWidth * (1 - ratio);
      totalWidth = minBarWidth;
    }
  } else {
    // Timed out
    totalWidth = measuredWidth;
  }

  // Format timing texts with improved clarity
  let inBarText = ""; // Text to display inside the bar (block info only)
  let notarizedLatencyText = ""; // Text to display below the notarized point
  let finalizedLatencyText = ""; // Text to display below the finalized point
  let growingLatencyText = ""; // Text to display below the growing bar tip

  if (status === "growing") {
    const elapsed = currentTime - startTime;
    // Only show latency if it's positive
    if (elapsed > 1) {
      growingLatencyText = `${Math.round(elapsed)}ms`;
    }
  } else if (status === "notarized") {
    let latency = notarizationTime ? (notarizationTime - startTime) : 0;

    // Only show latency if it's positive
    if (latency > 0) {
      notarizedLatencyText = `${Math.round(latency)}ms`;
    }

    // Format inBarText for block information
    if (block) {
      inBarText = isMobile ? `#${block.height}` : `#${block.height} | ${shortenUint8Array(block.digest)}`;
    }
  } else if (status === "finalized") {
    // Only show notarization latency if notarization time exists
    if (notarizationTime) {
      const notarizeLatency = notarizationTime - startTime;
      if (notarizeLatency > 0) {
        notarizedLatencyText = `${Math.round(notarizeLatency)}ms`;
      }
    }

    // Always show total latency (seed to finalization, or just finalization if no seed)
    if (finalizationTime) {
      const totalLatency = finalizationTime - startTime;
      if (totalLatency > 0) {
        finalizedLatencyText = `${Math.round(totalLatency)}ms`;
      }
    }

    // Set block info
    if (block) {
      inBarText = isMobile ? `#${block.height}` : `#${block.height} | ${shortenUint8Array(block.digest)}`;
    }
  } else {
    // Timed out
    inBarText = "SKIPPED";
  }

  // Determine what content to render in bar - for finalized without notarization
  const renderFinalizedWithoutNotarization = status === "finalized" && !notarizationTime;

  // Calculate positions for timing labels to prevent overlap
  const labelWidth = isMobile ? 45 : 60; // Estimated width of a timing label
  const minLabelSpacing = labelWidth + 5; // Increased minimum space needed between labels

  // Calculate ideal positions for notarization and finalization labels (centered on their respective points)
  let notarizedLabelPosition = notarizedWidth > 0 ? Math.max(0, notarizedWidth - (labelWidth / 2)) : 0;
  let finalizedLabelPosition = totalWidth > 0 ? Math.max(0, totalWidth - (labelWidth / 2)) : 0;

  // Check if labels would overlap
  const wouldOverlap = status === "finalized" &&
    notarizationTime &&
    (finalizedLabelPosition - notarizedLabelPosition < minLabelSpacing);

  // Adjust positions if overlap detected - option 3: ensure minimum spacing
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

  return (
    <div className="bar-row">
      <div className="view-info" style={{ width: `${viewInfoWidth}px` }}>
        <div className="view-number">{view}</div>
        <div className="view-signature">
          {signature ? shortenUint8Array(signature) : ""}
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
          {(status === "timed_out" || status === "growing") && (
            <div
              className={`bar-segment ${status === "timed_out" ? "timed-out" : "growing"}`}
              style={{ width: "100%" }}
            >
              {inBarText}
            </div>
          )}

          {/* Notarized state */}
          {status === "notarized" && (
            <div
              className="bar-segment growing"
              style={{ width: "100%" }}
            >
              {inBarText}
            </div>
          )}

          {/* Finalized state with notarization */}
          {status === "finalized" && !renderFinalizedWithoutNotarization && (
            <>
              {/* Base segment (seed to notarization) */}
              <div
                className="bar-segment growing"
                style={{ width: `${notarizedWidth}px` }}
              >
                {inBarText}
              </div>

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
          {/* Only show timing if not skipped */}
          {signature && (
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
                  }}
                >
                  {finalizedLatencyText}
                </div>
              )}

              {/* Latency for growing bars - follows the tip - only show if text exists */}
              {status === "growing" && growingLatencyText && (
                <div
                  className="latency-text growing-latency"
                  style={{
                    left: `${Math.max(0, totalWidth - (labelWidth / 2))}px`,
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

function shortenUint8Array(arr: Uint8Array | undefined): string {
  if (!arr || arr.length === 0) return "";

  // Convert the entire array to hex
  const fullHex = Array.from(arr, (b) => b.toString(16).padStart(2, "0")).join("");

  // Get first 5 characters of the hex string
  return fullHex.slice(0, 5);
}

export default App;