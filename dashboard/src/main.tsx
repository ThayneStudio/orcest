import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import App from "./App";
import FleetDashboard from "./FleetDashboard";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    {new URLSearchParams(window.location.search).get("view") === "diagnostics" ? <App /> : <FleetDashboard />}
  </StrictMode>
);
