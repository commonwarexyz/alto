import React, { useEffect, useState, useRef } from "react";
import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
import { LatLng, Icon, DivIcon } from "leaflet";
import "leaflet/dist/leaflet.css";
import init, { parse_seed, parse_notarized, parse_finalized } from "./alto_types/alto_types.js";
import { WS_URL, PUBLIC_KEY } from "./config";
import { SeedJs, NotarizedJs, FinalizedJs, BlockJs } from "./types";

// Array of locations for deterministic mapping
const locations: [number, number][] = [
  [37.7749, -122.4194], // San Francisco
  [51.5074, -0.1278],   // London
  [35.6895, 139.6917],  // Tokyo
  [-33.8688, 151.2093], // Sydney
  [55.7558, 37.6173],   // Moscow
  [-23.5505, -46.6333], // Sao Paulo
  [28.6139, 77.2090],   // New Delhi
  [40.7128, -74.0060],  // New York
  [19.4326, -99.1332],  // Mexico City
  [31.2304, 121.4737],  // Shanghai
];

// Location names for popups
const locationNames: string[] = [
  "San Francisco", "London", "Tokyo", "Sydney", "Moscow",
  "Sao Paulo", "New Delhi", "New York", "Mexico City", "Shanghai"
];

type ViewStatus = "growing" | "notarized" | "finalized" | "timed_out";

interface ViewData {
  view: number;
  location: [number, number];
  locationName: string;
  status: ViewStatus;
  startTime: number;
  notarizationTime?: number;
  finalizationTime?: number;
  signature?: Uint8Array;
  block?: BlockJs;
  timeoutId?: NodeJS.Timeout;
}

const TIMEOUT_DURATION = 2000; // 2 seconds

// Custom marker icons
const createCustomIcon = (status: ViewStatus) => {
  const color =
    status === "growing" ? "#808080" :
      status === "notarized" ? "#4CAF50" :
        status === "finalized" ? "#1B5E20" :
          "#F44336"; // timed_out

  return new DivIcon({
    className: "custom-div-icon",
    html: `<div style="
      background-color: ${color};
      width: 12px;
      height: 12px;
      border-radius: 50%;
      border: 2px solid white;
      box-shadow: 0 0 4px rgba(0,0,0,0.4);
    "></div>`,
    iconSize: [15, 15],
    iconAnchor: [8, 8]
  });
};

const App: React.FC = () => {
  const [views, setViews] = useState<ViewData[]>([]);
  const [lastObservedView, setLastObservedView] = useState<number | null>(null);
  const [isConnected, setIsConnected] = useState<boolean>(false);
  const [statsData, setStatsData] = useState({
    totalViews: 0,
    finalized: 0,
    notarized: 0,
    growing: 0,
    timedOut: 0
  });
  const currentTimeRef = useRef(Date.now());
  const wsRef = useRef<WebSocket | null>(null);

  // Update current time every 100ms to force re-render for growing bars
  useEffect(() => {
    const interval = setInterval(() => {
      currentTimeRef.current = Date.now();
      // Force re-render without relying on state updates
      setViews(views => [...views]);
    }, 100);
    return () => clearInterval(interval);
  }, []);

  // Update stats whenever views change
  useEffect(() => {
    const stats = {
      totalViews: views.length,
      finalized: views.filter((v: ViewData) => v.status === "finalized").length,
      notarized: views.filter((v: ViewData) => v.status === "notarized").length,
      growing: views.filter((v: ViewData) => v.status === "growing").length,
      timedOut: views.filter((v: ViewData) => v.status === "timed_out").length
    };
    setStatsData(stats);
  }, [views]);

  // Initialize WebSocket
  useEffect(() => {
    const setup = async () => {
      await init();
      connectWebSocket();
    };

    const connectWebSocket = () => {
      const ws = new WebSocket(WS_URL);
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
            if (seed) handleSeed(seed);
            break;
          case 1: // Notarization
            const notarized = parse_notarized(PUBLIC_KEY, payload);
            if (notarized) handleNotarization(notarized);
            break;
          case 3: // Finalization
            const finalized = parse_finalized(PUBLIC_KEY, payload);
            if (finalized) handleFinalization(finalized);
            break;
        }
      };

      ws.onerror = (error) => {
        console.error("WebSocket error:", error);
        setIsConnected(false);
      };

      ws.onclose = () => {
        console.log("WebSocket closed, trying to reconnect in 5 seconds");
        setIsConnected(false);
        setTimeout(connectWebSocket, 5000);
      };
    };

    setup();

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, []);

  const handleSeed = (seed: SeedJs) => {
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
          const locationIndex = missedView % locations.length;

          // Check if this view already exists
          const existingIndex = newViews.findIndex(v => v.view === missedView);

          if (existingIndex === -1) {
            // Only add if it doesn't already exist
            newViews.unshift({
              view: missedView,
              location: locations[locationIndex],
              locationName: locationNames[locationIndex],
              status: "timed_out",
              startTime: Date.now(),
            });
          }
        }
      }

      // Check if this view already exists
      const existingIndex = newViews.findIndex(v => v.view === view);

      if (existingIndex !== -1) {
        // If it exists and is already finalized or notarized, don't update it
        const existingStatus = newViews[existingIndex].status;
        if (existingStatus === "finalized" || existingStatus === "notarized") {
          return newViews;
        }

        // If it exists but is in another state, clear its timeout but preserve everything else
        if (newViews[existingIndex].timeoutId) {
          clearTimeout(newViews[existingIndex].timeoutId);
        }
      }

      // Create the new view data
      const locationIndex = view % locations.length;
      const newView: ViewData = {
        view,
        location: locations[locationIndex],
        locationName: locationNames[locationIndex],
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
          timeoutId: timeoutId
        };
      } else {
        // Add as new
        newViews.unshift(viewWithTimeout);
      }

      // Update the last observed view if this is a new maximum
      if (lastObservedView === null || view > lastObservedView) {
        setLastObservedView(view);
      }

      return newViews;
    });
  };

  const handleNotarization = (notarized: NotarizedJs) => {
    const view = notarized.proof.view;
    setViews((prevViews) => {
      const index = prevViews.findIndex((v) => v.view === view);
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
          notarizationTime: Date.now(),
          block: notarized.block,
          timeoutId: undefined,
        };

        return [
          ...prevViews.slice(0, index),
          updatedView,
          ...prevViews.slice(index + 1),
        ];
      }

      // If view doesn't exist, create it
      const locationIndex = view % locations.length;
      return [{
        view,
        location: locations[locationIndex],
        locationName: locationNames[locationIndex],
        status: "notarized",
        startTime: Date.now(),
        notarizationTime: Date.now(),
        block: notarized.block,
      }, ...prevViews];
    });
  };

  const handleFinalization = (finalized: FinalizedJs) => {
    const view = finalized.proof.view;
    setViews((prevViews) => {
      const index = prevViews.findIndex((v) => v.view === view);
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

        const updatedView: ViewData = {
          ...viewData,
          status: "finalized",
          finalizationTime: Date.now(),
          block: finalized.block,
          timeoutId: undefined,
        };

        return [
          ...prevViews.slice(0, index),
          updatedView,
          ...prevViews.slice(index + 1),
        ];
      }

      // If view doesn't exist, create it
      const locationIndex = view % locations.length;
      return [{
        view,
        location: locations[locationIndex],
        locationName: locationNames[locationIndex],
        status: "finalized",
        startTime: Date.now(),
        finalizationTime: Date.now(),
        block: finalized.block,
      }, ...prevViews];
    });
  };

  // Define center using LatLng
  const center = new LatLng(20, 0);

  return (
    <div style={{
      padding: "0",
      background: "#121212",
      color: "#eee",
      fontFamily: "Inter, system-ui, sans-serif",
      minHeight: "100vh"
    }}>
      <header style={{
        padding: "20px",
        background: "#1c1c1c",
        borderBottom: "1px solid #333",
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center"
      }}>
        <h1 style={{
          margin: 0,
          fontSize: "28px",
          fontWeight: "600",
          background: "linear-gradient(to right, #00ff99, #33ccff)",
          WebkitBackgroundClip: "text",
          WebkitTextFillColor: "transparent"
        }}>
          Alto
        </h1>
        <div style={{
          display: "flex",
          alignItems: "center",
          gap: "10px"
        }}>
          <div style={{
            width: "10px",
            height: "10px",
            borderRadius: "50%",
            background: isConnected ? "#4CAF50" : "#F44336"
          }}></div>
          <span>{isConnected ? "Connected" : "Disconnected"}</span>
        </div>
      </header>

      <main style={{ padding: "20px" }}>
        {/* Map */}
        <div style={{
          height: "400px",
          marginBottom: "20px",
          borderRadius: "8px",
          overflow: "hidden",
          boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)"
        }}>
          <MapContainer center={center} zoom={2} style={{ height: "100%", width: "100%" }}>
            <TileLayer
              url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
            />
            {views.length > 0 && (
              <Marker
                key={views[0].view}
                position={views[0].location}
                icon={createCustomIcon(views[0].status)}
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

        {/* Legend */}
        <div style={{
          display: "flex",
          gap: "15px",
          marginBottom: "20px",
          padding: "10px",
          background: "#1c1c1c",
          borderRadius: "6px"
        }}>
          <LegendItem color="#555" label="Seed to Notarized" />
          <LegendItem color="#2E7D32" label="Notarized to Finalized" />
          <LegendItem color="#1B5E20" label="Finalization Point" />
          <LegendItem color="#F44336" label="Timed Out" />
        </div>

        {/* Bars */}
        <div style={{
          background: "#1c1c1c",
          borderRadius: "8px",
          padding: "20px",
          boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)"
        }}>
          <h2 style={{
            margin: "0 0 20px 0",
            fontSize: "20px",
            fontWeight: "500"
          }}>
            Consensus Views
          </h2>
          {views.slice(0, 100).map((viewData) => (
            <Bar key={viewData.view} viewData={viewData} currentTime={currentTimeRef.current} />
          ))}
        </div>
      </main>
    </div>
  );
};

interface LegendItemProps {
  color: string;
  label: string;
}

const LegendItem: React.FC<LegendItemProps> = ({ color, label }) => {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "5px" }}>
      <div style={{
        width: "12px",
        height: "12px",
        background: color,
        borderRadius: "3px"
      }}></div>
      <span style={{ fontSize: "14px" }}>{label}</span>
    </div>
  );
};

interface BarProps {
  viewData: ViewData;
  currentTime: number;
}

const Bar: React.FC<BarProps> = ({ viewData, currentTime }) => {
  const { view, status, startTime, notarizationTime, finalizationTime, signature, block } = viewData;

  const maxWidth = 800; // pixels - increased to show more detail
  const growthRate = maxWidth / TIMEOUT_DURATION; // pixels per ms - more pixels per ms for zoomed in view
  const minBarWidth = 30; // minimum width for completed bars that happened very quickly

  // Calculate widths for different stages
  let totalWidth: number;
  let notarizedWidth: number = 0;
  let finalizedWidth: number = 0;

  // Calculate the current or final width
  if (status === "growing") {
    const elapsed = currentTime - startTime;
    totalWidth = Math.min(elapsed * growthRate, maxWidth);
  } else if (status === "notarized" || status === "finalized") {
    // Calculate notarization segment
    if (notarizationTime) {
      const notarizeElapsed = notarizationTime - startTime;
      notarizedWidth = Math.min(notarizeElapsed * growthRate, maxWidth);
      notarizedWidth = Math.max(notarizedWidth, minBarWidth); // Ensure minimum width
    }

    // Calculate finalization segment (if applicable)
    if (status === "finalized" && finalizationTime && notarizationTime) {
      const finalizeElapsed = finalizationTime - notarizationTime;
      finalizedWidth = Math.min(finalizeElapsed * growthRate, maxWidth - notarizedWidth);
      finalizedWidth = Math.max(finalizedWidth, minBarWidth / 2); // Ensure minimum width
    }

    totalWidth = notarizedWidth + finalizedWidth;
  } else {
    // Timed out
    totalWidth = maxWidth;
  }

  // Format timing texts
  let inBarText = ""; // Text to display inside the bar (block info only)
  let notarizedLatencyText = ""; // Text to display below the notarized point
  let finalizedLatencyText = ""; // Text to display below the finalized point
  let growingLatencyText = ""; // Text to display below the growing bar tip

  if (status === "growing") {
    const elapsed = currentTime - startTime;
    growingLatencyText = `${Math.round(elapsed)}ms`;
  } else if (status === "notarized") {
    const latency = notarizationTime ? (notarizationTime - startTime) : 0;
    notarizedLatencyText = `${Math.round(latency)}ms`;
    // Format inBarText for block information
    if (block) {
      inBarText = `#${block.height} | ${shortenUint8Array(block.digest)}`;
    }
  } else if (status === "finalized") {
    // Get seed to notarization time
    const notarizeLatency = notarizationTime ? (notarizationTime - startTime) : 0;
    // Get total time (seed to finalization)
    const totalLatency = finalizationTime ? (finalizationTime - startTime) : 0;

    // Set latency text
    notarizedLatencyText = `${Math.round(notarizeLatency)}ms`;
    finalizedLatencyText = `${Math.round(totalLatency)}ms`; // Show total time at finalization point

    // Set block info
    if (block) {
      inBarText = `#${block.height} | ${shortenUint8Array(block.digest)}`;
    }
  } else {
    // Timed out
    inBarText = "TIMED OUT";
  }

  return (
    <div style={{
      display: "flex",
      marginBottom: "20px", // More space for timing text below
      fontSize: "14px"
    }}>
      <div style={{
        width: "80px",
        textAlign: "right",
        marginRight: "15px",
        flexShrink: 0
      }}>
        <div style={{
          fontWeight: "500",
          color: "#fff"
        }}>
          {view}
        </div>
        <div style={{
          fontSize: "12px",
          color: "#888",
          textOverflow: "ellipsis",
          overflow: "hidden"
        }}>
          {signature ? shortenUint8Array(signature) : "Skipped"}
        </div>
      </div>

      <div style={{ position: "relative" }}>
        {/* Main bar container */}
        <div style={{
          height: "26px",
          position: "relative",
          width: `${totalWidth}px`,
          borderRadius: "4px",
          overflow: "hidden",
          transition: "width 0.1s linear",
          backgroundColor: "#333",
          boxShadow: "0 1px 3px rgba(0,0,0,0.3)"
        }}>
          {/* Base bar - grey represents time from seed receipt to notarization (or current time if growing) */}
          <div style={{
            position: "absolute",
            top: 0,
            left: 0,
            height: "100%",
            width: status === "timed_out" ? "100%" :
              (status === "growing" ? "100%" :
                `${notarizedWidth}px`),
            backgroundColor: status === "timed_out" ? "#F44336" : "#555",
            borderRadius: "4px",
            display: "flex",
            alignItems: "center",
            paddingLeft: "10px",
            paddingRight: "10px",
            color: "white",
            fontSize: "13px",
            overflow: "hidden",
            whiteSpace: "nowrap",
            textOverflow: "ellipsis"
          }}>
            {inBarText}
          </div>

          {/* Notarized to finalized segment */}
          {status === "finalized" && finalizedWidth > 0 && (
            <div style={{
              position: "absolute",
              top: 0,
              left: `${notarizedWidth}px`,
              height: "100%",
              width: `${finalizedWidth}px`,
              backgroundColor: "#2E7D32", // Darker green
              zIndex: 1
            }} />
          )}

          {/* Marker for notarization point - now same color as base bar */}
          {(status === "notarized" || status === "finalized") && (
            <div style={{
              position: "absolute",
              left: `${notarizedWidth - 1}px`,
              top: 0,
              bottom: 0,
              width: "2px",
              backgroundColor: "#555", // Same as base bar color
              zIndex: 2
            }} />
          )}

          {/* Marker for finalization point */}
          {status === "finalized" && (
            <div style={{
              position: "absolute",
              right: 0,
              top: 0,
              bottom: 0,
              width: "3px",
              backgroundColor: "#1B5E20", // Dark green
              zIndex: 3
            }} />
          )}
        </div>

        {/* Timing information underneath */}
        <div style={{ position: "relative", height: "20px", marginTop: "2px" }}>
          {/* Only show timing if not skipped */}
          {signature && (
            <>
              {/* Latency at notarization point */}
              {(status === "notarized" || status === "finalized") && notarizedWidth > 0 && (
                <div style={{
                  position: "absolute",
                  left: `${notarizedWidth - 30}px`, // Position under the notarization marker
                  color: "#aaa",
                  fontSize: "12px",
                  whiteSpace: "nowrap"
                }}>
                  {notarizedLatencyText}
                </div>
              )}

              {/* Latency for growing bars - follows the tip */}
              {status === "growing" && (
                <div style={{
                  position: "absolute",
                  left: `${totalWidth - 30}px`, // Position under the growing bar tip
                  color: "#aaa",
                  fontSize: "12px",
                  whiteSpace: "nowrap",
                  transition: "left 0.1s linear"
                }}>
                  {growingLatencyText}
                </div>
              )}

              {/* Total latency marker for finalized views */}
              {status === "finalized" && (
                <div style={{
                  position: "absolute",
                  left: `${totalWidth - 30}px`, // Position under the finalization marker
                  color: "#aaa",
                  fontSize: "12px",
                  whiteSpace: "nowrap"
                }}>
                  {finalizedLatencyText}
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

  // Get first 'length' bytes (2 hex chars per byte)
  const firstPart = fullHex.slice(0, 3);

  // Get last 'length' bytes
  const lastPart = fullHex.slice(-3);

  // Return formatted string with first and last parts
  return `${firstPart}..${lastPart}`;
}

export default App;