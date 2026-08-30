import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { App } from "./pages/App";
import "@carbon/styles/css/styles.css";
import "./app.css";

// BrowserRouter (not HashRouter) so shared links are clean paths. This requires
// the server to serve index.html for unknown non-/api paths; see mount_static()
// in ui/server.py.
ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
