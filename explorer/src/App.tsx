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

const TIMEOUT_DURATION = 10000; // 10 seconds
// We'll only display the latest view on the map

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
    const view = seed.view;
    setViews((prevViews) => {
      let newViews = [...prevViews];

      // Handle skipped views
      if (lastObservedView !== null && view > lastObservedView + 1) {
        for (let missedView = lastObservedView + 1; missedView < view; missedView++) {
          const locationIndex = missedView % locations.length;
          newViews.unshift({
            view: missedView,
            location: locations[locationIndex],
            locationName: locationNames[locationIndex],
            status: "timed_out",
            startTime: Date.now(),
          });
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

        // If it exists but is in another state, update it
        if (newViews[existingIndex].timeoutId) {
          clearTimeout(newViews[existingIndex].timeoutId);
        }
      }

      // Add new view
      const locationIndex = view % locations.length;
      const newView: ViewData = {
        view,
        location: locations[locationIndex],
        locationName: locationNames[locationIndex],
        status: "growing",
        startTime: Date.now(),
        signature: seed.signature,
      };

      // Only set timeout for growing views
      const timeoutId = setTimeout(() => {
        setViews((prev) => {
          return prev.map((v) => {
            // Only time out views that are still in growing state
            if (v.view === view && v.status === "growing") {
              return { ...v, status: "timed_out", timeoutId: undefined };
            }
            return v;
          });
        });
      }, TIMEOUT_DURATION);

      if (existingIndex !== -1) {
        // Replace existing view
        newViews[existingIndex] = { ...newView, timeoutId };
      } else {
        // Add new view
        newViews.unshift({ ...newView, timeoutId });
      }

      setLastObservedView(view);
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
          Alto Blockchain Explorer
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
        {/* Stats Cards */}
        <div style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
          gap: "20px",
          marginBottom: "20px"
        }}>
          <StatCard title="Total Views" value={statsData.totalViews} color="#90CAF9" />
          <StatCard title="Finalized" value={statsData.finalized} color="#81C784" />
          <StatCard title="Notarized" value={statsData.notarized} color="#AED581" />
          <StatCard title="Growing" value={statsData.growing} color="#E0E0E0" />
          <StatCard title="Timed Out" value={statsData.timedOut} color="#EF9A9A" />
        </div>

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
          <LegendItem color="#808080" label="Growing" />
          <LegendItem color="#4CAF50" label="Notarized" />
          <LegendItem color="#1B5E20" label="Finalized" />
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

interface StatCardProps {
  title: string;
  value: number;
  color: string;
}

const StatCard: React.FC<StatCardProps> = ({ title, value, color }) => {
  return (
    <div style={{
      background: "#1c1c1c",
      borderRadius: "8px",
      padding: "15px",
      boxShadow: "0 2px 4px rgba(0, 0, 0, 0.1)",
      borderLeft: `4px solid ${color}`
    }}>
      <h3 style={{
        margin: "0 0 10px 0",
        color: "#aaa",
        fontSize: "14px",
        fontWeight: "400"
      }}>
        {title}
      </h3>
      <p style={{
        margin: 0,
        fontSize: "24px",
        fontWeight: "600",
        color: "#fff"
      }}>
        {value}
      </p>
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

  const maxWidth = 500; // pixels
  const growthRate = maxWidth / TIMEOUT_DURATION; // pixels per ms

  let width: number;
  let text: string = "";

  if (status === "growing") {
    const elapsed = currentTime - startTime;
    width = Math.min(elapsed * growthRate, maxWidth);
    text = `Latency: ${(elapsed / 1000).toFixed(1)}s`;
  } else if (status === "notarized" || status === "finalized") {
    const endTime = status === "notarized" ? notarizationTime! : finalizationTime!;
    const latency = (endTime - startTime) / 1000;
    width = Math.min((endTime - startTime) * growthRate, maxWidth);

    // For notarized and finalized, the text includes more details
    if (block) {
      text = `${status === "finalized" ? "Finalized" : "Notarized"} | Latency: ${latency.toFixed(1)}s | Height: ${block.height} | Digest: ${shortenUint8Array(block.digest)}`;
    } else {
      text = `${status === "finalized" ? "Finalized" : "Notarized"} | Latency: ${latency.toFixed(1)}s`;
    }
  } else {
    // Timed out
    width = maxWidth;
    text = "TIMED OUT";
  }

  return (
    <div style={{
      display: "flex",
      alignItems: "center",
      marginBottom: "12px",
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
      <div
        style={{
          height: "24px",
          position: "relative",
          width: `${width}px`,
          borderRadius: "4px",
          overflow: "hidden",
          transition: "width 0.1s linear"
        }}
      >
        {/* Base bar - always grey to show latency */}
        <div style={{
          position: "absolute",
          top: 0,
          left: 0,
          height: "100%",
          width: "100%",
          backgroundColor: status === "timed_out" ? "#F44336" : "#555",
          borderRadius: "4px"
        }} />

        {/* Text overlay */}
        <div style={{
          position: "absolute",
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          display: "flex",
          alignItems: "center",
          padding: "0 10px",
          color: "white",
          fontSize: "13px",
          textShadow: "0 1px 2px rgba(0,0,0,0.5)",
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
          zIndex: 2
        }}>
          {text}
        </div>

        {/* Finalized indicator cap */}
        {status === "finalized" && (
          <div style={{
            position: "absolute",
            right: 0,
            top: 0,
            bottom: 0,
            width: "5px",
            backgroundColor: "#1B5E20",
            zIndex: 1
          }} />
        )}
      </div>
    </div>
  );
};

function shortenUint8Array(arr: Uint8Array | undefined, length: number = 4): string {
  if (!arr) return "";
  const hex = Array.from(arr.slice(0, length), (b) => b.toString(16).padStart(2, "0")).join("");
  return `0x${hex}...`;
}

export default App;