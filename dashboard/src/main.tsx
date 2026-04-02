import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./pages/App";
import "@carbon/styles/css/styles.css";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

