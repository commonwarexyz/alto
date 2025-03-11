import React, { useEffect, useState, useRef } from "react";
import { MapContainer, TileLayer, Marker } from "react-leaflet";
import { LatLng } from "leaflet";
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

type ViewStatus = "growing" | "notarized" | "finalized" | "timed_out";

interface ViewData {
  view: number;
  location: [number, number];
  status: ViewStatus;
  startTime: number;
  notarizationTime?: number;
  finalizationTime?: number;
  signature?: Uint8Array;
  block?: BlockJs;
  timeoutId?: NodeJS.Timeout;
}

const TIMEOUT_DURATION = 10000; // 10 seconds

const App: React.FC = () => {
  const [views, setViews] = useState<ViewData[]>([]);
  const [lastObservedView, setLastObservedView] = useState<number | null>(null);
  const currentTimeRef = useRef(Date.now());

  // Update current time every 100ms and force re-render for growing bars
  useEffect(() => {
    const interval = setInterval(() => {
      currentTimeRef.current = Date.now();
      // Trigger re-render by updating views with a new array reference
      setViews((prev) => [...prev]);
    }, 100);
    return () => clearInterval(interval);
  }, []);

  // Initialize WebSocket
  useEffect(() => {
    const setup = async () => {
      await init();
      const ws = new WebSocket(WS_URL);
      ws.binaryType = "arraybuffer";

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

      ws.onerror = () => console.error("WebSocket error");
      ws.onclose = () => console.log("WebSocket closed");
    };

    setup();
  }, []);

  const handleSeed = (seed: SeedJs) => {
    const view = seed.view;
    setViews((prevViews) => {
      let newViews = [...prevViews];

      // Handle skipped views
      if (lastObservedView !== null && view > lastObservedView + 1) {
        for (let missedView = lastObservedView + 1; missedView < view; missedView++) {
          newViews.unshift({
            view: missedView,
            location: locations[missedView % locations.length],
            status: "timed_out",
            startTime: Date.now(),
          });
        }
      }

      // Add new view
      const newView: ViewData = {
        view,
        location: locations[view % locations.length],
        status: "growing",
        startTime: Date.now(),
        signature: seed.signature,
      };
      const timeoutId = setTimeout(() => {
        setViews((prev) =>
          prev.map((v) => (v.view === view ? { ...v, status: "timed_out" } : v))
        );
      }, TIMEOUT_DURATION);
      newViews.unshift({ ...newView, timeoutId });

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
        if (viewData.timeoutId) clearTimeout(viewData.timeoutId);
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
      // If view doesn’t exist, create it
      return [{
        view,
        location: locations[view % locations.length],
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
        if (viewData.timeoutId) clearTimeout(viewData.timeoutId);
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
      // If view doesn’t exist, create it
      return [{
        view,
        location: locations[view % locations.length],
        status: "finalized",
        startTime: Date.now(),
        finalizationTime: Date.now(),
        block: finalized.block,
      }, ...prevViews];
    });
  };

  // Define center using LatLng
  const center = new LatLng(0, 0);

  return (
    <div style={{ padding: "20px", background: "#1a1a1a", color: "#fff", fontFamily: "Arial" }}>
      <h1 style={{ textAlign: "center", color: "#00ff99" }}>Alto Blockchain Explorer</h1>

      {/* Map */}
      <div style={{ height: "400px", marginBottom: "20px" }}>
        <MapContainer center={center} zoom={1} style={{ height: "100%", width: "100%" }}>
          <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
          {views.length > 0 && <Marker position={views[0].location} />}
        </MapContainer>
      </div>

      {/* Bars */}
      <div>
        {views.slice(0, 100).map((viewData) => (
          <Bar key={viewData.view} viewData={viewData} currentTime={currentTimeRef.current} />
        ))}
      </div>
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
  let color: string;
  let text: string = "";

  if (status === "growing") {
    const elapsed = currentTime - startTime;
    width = Math.min(elapsed * growthRate, maxWidth);
    color = "grey";
    text = `Latency: ${(elapsed / 1000).toFixed(1)}s`;
  } else if (status === "notarized" || status === "finalized") {
    const endTime = finalizationTime || notarizationTime!;
    const latency = (endTime - startTime) / 1000;
    width = (endTime - startTime) * growthRate;
    color = "green";
    text = `${status === "finalized" ? "Finalized" : "Notarized"} | Latency: ${latency.toFixed(1)}s | Height: ${block?.height} | Digest: ${shortenUint8Array(block?.digest)}`;
  } else {
    width = maxWidth;
    color = "red";
    text = "TIMED OUT";
  }

  return (
    <div style={{ display: "flex", alignItems: "center", marginBottom: "10px" }}>
      <div style={{ width: "100px", textAlign: "right", marginRight: "10px" }}>
        <div>{view}</div>
        <div style={{ fontSize: "0.8em" }}>
          {signature ? shortenUint8Array(signature) : "Skipped"}
        </div>
      </div>
      <div
        style={{
          height: "20px",
          backgroundColor: color,
          width: `${width}px`,
          position: "relative",
          transition: "width 0.1s linear",
        }}
      >
        <div
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            color: "white",
            fontSize: "0.9em",
          }}
        >
          {text}
        </div>
        {status === "finalized" && (
          <div
            style={{
              position: "absolute",
              right: 0,
              top: 0,
              bottom: 0,
              width: "5px",
              backgroundColor: "darkgreen",
            }}
          />
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