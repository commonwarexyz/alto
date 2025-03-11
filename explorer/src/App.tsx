import React, { useEffect, useState, useRef } from "react";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  LineElement,
  PointElement,
  LinearScale,
  TimeScale,
  Title,
  Tooltip,
  Legend,
} from "chart.js";
import "chartjs-adapter-date-fns";
import init, { parse_seed, parse_notarized, parse_finalized } from "./alto_types/alto_types.js";
import { WS_URL, PUBLIC_KEY } from "./config";
import { SeedJs, NotarizedJs, FinalizedJs } from "./types";

ChartJS.register(LineElement, PointElement, LinearScale, TimeScale, Title, Tooltip, Legend);


type ConsensusEvent =
  | { type: "Seed"; data: SeedJs; timestamp: number }
  | { type: "Notarization"; data: NotarizedJs; timestamp: number }
  | { type: "Finalization"; data: FinalizedJs; timestamp: number };

const App: React.FC = () => {
  const [events, setEvents] = useState<ConsensusEvent[]>([]);
  const [finalizationCounts, setFinalizationCounts] = useState<number[]>(Array(60).fill(0));
  const wsRef = useRef<WebSocket | null>(null);
  const chartRef = useRef<ChartJS<"line"> | null>(null);

  // Initialize WASM and WebSocket
  useEffect(() => {
    const setup = async () => {
      await init();

      const ws = new WebSocket(WS_URL);
      ws.binaryType = "arraybuffer";
      wsRef.current = ws;

      ws.onmessage = (event) => {
        const data = new Uint8Array(event.data);
        const kind = data[0];
        const payload = data.slice(1);
        console.log(`Kind: ${kind}, Payload length: ${payload.length}`);

        switch (kind) {
          case 0: // Seed
            const seed = parse_seed(PUBLIC_KEY, payload);
            if (seed) {
              setEvents((prev) => [
                { type: "Seed", data: seed, timestamp: Date.now() },
                ...prev.slice(0, 99), // Limit to 100 events
              ]);
            }
            break;
          case 1: // Notarization
            const notarized = parse_notarized(PUBLIC_KEY, payload);
            if (notarized) {
              setEvents((prev) => [
                { type: "Notarization", data: notarized, timestamp: Date.now() },
                ...prev.slice(0, 99),
              ]);
            }
            break;
          case 3: // Finalization
            const finalized = parse_finalized(PUBLIC_KEY, payload);
            if (finalized) {
              setEvents((prev) => [
                { type: "Finalization", data: finalized, timestamp: Date.now() },
                ...prev.slice(0, 99),
              ]);
              setFinalizationCounts((prev) => [prev[0] + 1, ...prev.slice(0, -1)]);
            }
            break;
          default:
            // Ignore other kinds (e.g., Nullification)
            break;
        }
      };

      ws.onerror = () => console.error("WebSocket error");
      ws.onclose = () => console.log("WebSocket closed");
    };

    setup();

    return () => {
      if (wsRef.current) wsRef.current.close();
    };
  }, []);

  // Update chart every second
  useEffect(() => {
    const interval = setInterval(() => {
      setFinalizationCounts((prev) => [0, ...prev.slice(0, -1)]);
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  // Chart data
  const chartData = {
    labels: Array.from({ length: 60 }, (_, i) => new Date(Date.now() - (59 - i) * 1000)),
    datasets: [
      {
        label: "Finalizations per Second",
        data: finalizationCounts,
        borderColor: "#00ff99",
        backgroundColor: "rgba(0, 255, 153, 0.2)",
        fill: true,
      },
    ],
  };

  const chartOptions = {
    scales: {
      x: { type: "time" as const, time: { unit: "second" as const } },
      y: { beginAtZero: true, title: { display: true, text: "Finalizations" } },
    },
    animation: { duration: 500 },
  };

  // Format event for display
  const formatEvent = (event: ConsensusEvent) => {
    const age = Math.floor((Date.now() - event.timestamp) / 1000);
    switch (event.type) {
      case "Seed":
        return `Seed | View: ${event.data.view} | Age: ${age}s`;
      case "Notarization":
        const n = event.data as NotarizedJs;
        return `Notarization | View: ${n.proof.view} | Height: ${n.block.height} | Timestamp: ${n.block.timestamp} | Age: ${age}s`;
      case "Finalization":
        const f = event.data as FinalizedJs;
        return `Finalization | View: ${f.proof.view} | Height: ${f.block.height} | Timestamp: ${f.block.timestamp} | Age: ${age}s`;
    }
  };

  return (
    <div style={{ padding: "20px", background: "#1a1a1a", color: "#fff", fontFamily: "Arial" }}>
      <h1 style={{ textAlign: "center", color: "#00ff99" }}>Alto Blockchain Explorer</h1>

      <section style={{ marginBottom: "40px" }}>
        <h2>Consensus Stream</h2>
        <ul style={{ listStyle: "none", padding: 0, maxHeight: "400px", overflowY: "auto" }}>
          {events.map((event, index) => (
            <li
              key={index}
              style={{
                padding: "10px",
                background: "#333",
                marginBottom: "5px",
                borderRadius: "5px",
                transition: "all 0.3s",
              }}
            >
              {formatEvent(event)}
            </li>
          ))}
        </ul>
      </section>

      <section>
        <h2>Finalization Rate</h2>
        <div style={{ maxWidth: "800px", margin: "0 auto" }}>
          <Line data={chartData} options={chartOptions} />
        </div>
      </section>
    </div>
  );
};

export default App;