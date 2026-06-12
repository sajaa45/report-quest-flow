const KEY = "equitygraph.backend_url";

export const DEFAULT_BACKEND_URL = "http://localhost:8000";

export function getBackendUrl(): string {
  if (typeof window === "undefined") return DEFAULT_BACKEND_URL;
  return localStorage.getItem(KEY) || DEFAULT_BACKEND_URL;
}

export function setBackendUrl(url: string): void {
  if (typeof window === "undefined") return;
  localStorage.setItem(KEY, url.replace(/\/$/, ""));
}

export const PIPELINE_STEPS = [
  { id: 1, name: "parse_document", label: "Parse & convert document", description: "Convert the uploaded filing into structured sections." },
  { id: 2, name: "extract_target_entities", label: "Build target company graph", description: "Extract financial metrics, risks, and operating entities." },
  { id: 3, name: "build_peer_graph", label: "Build peer intelligence graph", description: "Find peers, retrieve metrics and risks, and populate the peer graph." },
] as const;
