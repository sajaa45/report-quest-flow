import { useCallback, useEffect, useRef, useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { Upload, Play, FileText, Settings, RotateCcw, AlertCircle } from "lucide-react";
import { StepItem, type StepData, type StepStatus } from "@/components/StepItem";
import {
  PIPELINE_STEPS,
  getBackendUrl,
  setBackendUrl,
  DEFAULT_BACKEND_URL,
} from "@/lib/pipeline-config";

export const Route = createFileRoute("/")({
  component: Index,
});

type PhaseStatus = "idle" | "running" | "complete" | "failed";

function fmtBytes(n: number) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

function Index() {
  // -------- Backend config --------
  const [backendUrl, setBackendUrlState] = useState(DEFAULT_BACKEND_URL);
  const [showSettings, setShowSettings] = useState(false);
  const [connected, setConnected] = useState<boolean | null>(null);

  useEffect(() => {
    setBackendUrlState(getBackendUrl());
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await fetch(`${backendUrl}/docs`, { method: "GET" });
        if (!cancelled) setConnected(r.ok);
      } catch {
        if (!cancelled) setConnected(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [backendUrl]);

  // -------- Phase 1 state --------
  const [file, setFile] = useState<File | null>(null);
  const [fiscalYear, setFiscalYear] = useState("2024");
  const [phase1Status, setPhase1Status] = useState<PhaseStatus>("idle");
  const [phase1Error, setPhase1Error] = useState<string | null>(null);
  const [steps, setSteps] = useState<StepData[]>(() =>
    PIPELINE_STEPS.map((s) => ({
      id: s.id,
      label: s.label,
      description: s.description,
      status: "pending" as StepStatus,
      logs: [],
    })),
  );
  const stepStartRef = useRef<Record<number, number>>({});
  const eventSourceRef = useRef<EventSource | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [dragOver, setDragOver] = useState(false);

  const resetSteps = useCallback(() => {
    setSteps(
      PIPELINE_STEPS.map((s) => ({
        id: s.id,
        label: s.label,
        description: s.description,
        status: "pending" as StepStatus,
        logs: [],
      })),
    );
    stepStartRef.current = {};
  }, []);

  const runPipeline = useCallback(async () => {
    if (!file) return;
    setPhase1Error(null);
    setPhase1Status("running");
    resetSteps();

    try {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("fiscal_year", fiscalYear);

      const r = await fetch(`${backendUrl}/pipeline/run`, {
        method: "POST",
        body: fd,
      });
      if (!r.ok) {
        const t = await r.text();
        throw new Error(`Upload failed (${r.status}): ${t}`);
      }
      const { job_id } = (await r.json()) as { job_id: string };

      // Open SSE stream
      const es = new EventSource(`${backendUrl}/pipeline/${job_id}/stream`);
      eventSourceRef.current = es;

      es.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data);

          if (data.type === "pipeline_complete") {
            setPhase1Status("complete");
            es.close();
            return;
          }
          if (data.type === "pipeline_failed") {
            setPhase1Status("failed");
            setPhase1Error(data.error || "Pipeline failed");
            es.close();
            return;
          }

          const stepId = data.step as number;
          if (!stepId) return;

          setSteps((prev) =>
            prev.map((s) => {
              if (s.id !== stepId) return s;
              const next: StepData = { ...s };
              if (data.status === "running") {
                next.status = "running";
                if (!stepStartRef.current[stepId]) {
                  stepStartRef.current[stepId] = Date.now();
                }
                if (data.message) {
                  next.logs = [...(s.logs || []), data.message];
                }
              } else if (data.status === "done") {
                next.status = "done";
                next.summary = data.summary || data.message;
                const start = stepStartRef.current[stepId];
                if (start) next.durationMs = Date.now() - start;
              } else if (data.status === "failed") {
                next.status = "failed";
                next.summary = data.error || data.message || "Step failed";
              }
              return next;
            }),
          );
        } catch (e) {
          console.error("Failed to parse SSE event", e);
        }
      };

      es.onerror = () => {
        // Stream closed — if no terminal event arrived, mark as failed
        es.close();
        setPhase1Status((prev) =>
          prev === "running" ? "failed" : prev,
        );
      };
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      setPhase1Error(msg);
      setPhase1Status("failed");
    }
  }, [file, fiscalYear, backendUrl, resetSteps]);

  useEffect(() => {
    return () => {
      eventSourceRef.current?.close();
    };
  }, []);

  const phase1Badge = (() => {
    switch (phase1Status) {
      case "complete":
        return { text: "Phase Complete", cls: "text-success bg-success/5 ring-success/20" };
      case "running":
        return { text: "Running", cls: "text-accent bg-accent/5 ring-accent/20" };
      case "failed":
        return { text: "Failed", cls: "text-destructive bg-destructive/5 ring-destructive/20" };
      default:
        return { text: "Ready", cls: "text-muted-foreground bg-muted ring-border" };
    }
  })();

  const handleFiles = (files: FileList | null) => {
    if (!files || !files[0]) return;
    setFile(files[0]);
  };

  return (
    <div className="min-h-screen bg-background font-sans text-foreground selection:bg-accent/10">
      {/* Nav */}
      <nav className="sticky top-0 z-50 border-b border-border bg-background/80 backdrop-blur-md">
        <div className="mx-auto flex h-14 max-w-3xl items-center justify-between px-6">
          <div className="flex items-center gap-4">
            <span className="font-mono text-xs font-bold tracking-tighter uppercase">
              — EquityGraph v1.0
            </span>
            <div className="h-4 w-px bg-border" />
            <span className="text-xs text-muted-foreground hidden sm:inline">
              Pipeline: PROD-FIN-SEC-KG
            </span>
          </div>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <span className="font-mono text-[10px] text-muted-foreground uppercase tracking-widest hidden sm:inline">
                {connected === null
                  ? "Status: Checking"
                  : connected
                  ? "Status: Connected"
                  : "Status: Offline"}
              </span>
              <div
                className={`h-2 w-2 rounded-full ${
                  connected === null
                    ? "bg-muted-foreground/40"
                    : connected
                    ? "bg-success"
                    : "bg-destructive"
                }`}
              />
            </div>
            <button
              onClick={() => setShowSettings((s) => !s)}
              className="rounded p-1 text-muted-foreground transition-colors hover:text-foreground"
              aria-label="Settings"
            >
              <Settings className="h-4 w-4" />
            </button>
          </div>
        </div>
        {showSettings && (
          <div className="border-t border-border bg-background">
            <div className="mx-auto max-w-3xl px-6 py-3 flex items-center gap-3">
              <label className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
                Backend URL
              </label>
              <input
                type="text"
                value={backendUrl}
                onChange={(e) => setBackendUrlState(e.target.value)}
                onBlur={(e) => setBackendUrl(e.target.value)}
                placeholder="http://localhost:8000"
                className="flex-1 rounded border border-input bg-card px-3 py-1.5 text-xs font-mono outline-none focus:ring-2 focus:ring-ring"
              />
            </div>
          </div>
        )}
      </nav>

      <main className="mx-auto max-w-3xl px-6 py-12">
        {/* PHASE 1 */}
        <section style={{ animation: "slideUp 0.6s var(--ease-out-expo) both" }}>
          <div className="mb-8 flex items-baseline justify-between gap-4">
            <div>
              <h2 className="font-mono text-[10px] font-bold tracking-widest text-muted-foreground uppercase mb-1">
                (01) Pipeline Extraction
              </h2>
              <h1 className="text-2xl font-semibold tracking-tight">Document Ingestion</h1>
            </div>
            <span
              className={`text-xs font-mono px-2 py-1 rounded ring-1 whitespace-nowrap ${phase1Badge.cls}`}
            >
              {phase1Badge.text}
            </span>
          </div>

          <div className="grid gap-6">
            {/* Upload */}
            <div className="rounded-lg border border-dashed border-border bg-card p-6 transition-colors hover:border-accent/40">
              <label
                onDragOver={(e) => {
                  e.preventDefault();
                  setDragOver(true);
                }}
                onDragLeave={() => setDragOver(false)}
                onDrop={(e) => {
                  e.preventDefault();
                  setDragOver(false);
                  handleFiles(e.dataTransfer.files);
                }}
                className={`flex h-32 w-full cursor-pointer flex-col items-center justify-center gap-2 rounded outline outline-1 -outline-offset-1 transition-colors ${
                  dragOver
                    ? "bg-accent/5 outline-accent/40"
                    : "bg-muted outline-border"
                }`}
              >
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".html,.htm,.pdf"
                  className="hidden"
                  onChange={(e) => handleFiles(e.target.files)}
                />
                <Upload className="h-5 w-5 text-muted-foreground" />
                <span className="text-[10px] font-medium uppercase tracking-widest text-muted-foreground">
                  Drop financial statement (HTML / PDF) or click to browse
                </span>
              </label>

              <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
                <div className="flex items-center gap-2 min-w-0">
                  <FileText className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                  <span className="text-sm font-medium truncate">
                    {file ? file.name : "No file selected"}
                  </span>
                  {file && (
                    <span className="text-[10px] text-muted-foreground uppercase font-mono tracking-tighter shrink-0">
                      {fmtBytes(file.size)}
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-2">
                  <div className="flex items-center gap-2 rounded border border-input bg-card px-2 py-1">
                    <label className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
                      FY
                    </label>
                    <input
                      type="text"
                      value={fiscalYear}
                      onChange={(e) => setFiscalYear(e.target.value)}
                      className="w-14 bg-transparent text-xs font-mono outline-none"
                    />
                  </div>
                  <button
                    onClick={runPipeline}
                    disabled={!file || phase1Status === "running"}
                    className="flex items-center gap-1.5 bg-foreground text-background text-xs font-medium px-4 py-1.5 rounded transition-all hover:bg-foreground/90 disabled:opacity-40 disabled:cursor-not-allowed"
                  >
                    {phase1Status === "complete" || phase1Status === "failed" ? (
                      <>
                        <RotateCcw className="h-3 w-3" /> Rerun Pipeline
                      </>
                    ) : (
                      <>
                        <Play className="h-3 w-3" /> Run Pipeline
                      </>
                    )}
                  </button>
                </div>
              </div>

              {phase1Error && (
                <div className="mt-3 flex items-start gap-2 rounded border border-destructive/30 bg-destructive/5 p-2 text-xs text-destructive">
                  <AlertCircle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
                  <span className="font-mono">{phase1Error}</span>
                </div>
              )}
            </div>

            {/* Stepper */}
            <div className="relative space-y-0 pl-2">
              <div className="absolute left-4 top-2 bottom-2 w-px bg-border" />
              {steps.map((s, i) => (
                <StepItem
                  key={s.id}
                  step={s}
                  isLast={i === steps.length - 1}
                />
              ))}
            </div>
          </div>
        </section>

        {/* Divider */}
        <div className="my-16 h-px w-full bg-border" />

        {/* PHASE 2 */}
        <section
          className="opacity-60"
          style={{ animation: "slideUp 0.8s var(--ease-out-expo) both" }}
        >
          <div className="mb-8 flex items-baseline justify-between gap-4">
            <div>
              <h2 className="font-mono text-[10px] font-bold tracking-widest text-muted-foreground uppercase mb-1">
                (02) Retrieval & Inference
              </h2>
              <h1 className="text-2xl font-semibold tracking-tight">GraphRAG Query</h1>
            </div>
            <span className="text-xs font-mono px-2 py-1 rounded ring-1 ring-border bg-muted text-muted-foreground whitespace-nowrap">
              Awaiting backend
            </span>
          </div>

          <div className="rounded-lg ring-1 ring-border bg-card shadow-sm">
            <div className="border-b border-border p-4">
              <input
                type="text"
                placeholder="Ask a question about your peer group… (e.g. What are the primary risk factors?)"
                disabled
                className="w-full bg-transparent text-sm font-medium outline-none placeholder:text-muted-foreground"
              />
            </div>
            <div className="p-4 space-y-4">
              <div className="flex flex-col gap-2 border-l-2 border-accent/20 pl-4 py-1">
                <span className="font-mono text-[10px] text-accent font-bold uppercase tracking-wider">
                  Process Trace
                </span>
                <div className="space-y-1 font-mono text-[10px] text-muted-foreground">
                  {[
                    "cypher_translation",
                    "graph_traversal",
                    "context_retrieval",
                    "answer_generation",
                    "citation_attachment",
                  ].map((step) => (
                    <div key={step} className="flex justify-between">
                      <span>{step}</span>
                      <span>PENDING</span>
                    </div>
                  ))}
                </div>
              </div>
              <p className="text-xs text-muted-foreground italic">
                Wire up a <code className="font-mono">POST /qa/run</code> + SSE endpoint on your
                backend and Phase 2 will stream sub-steps the same way Phase 1 does.
              </p>
            </div>
          </div>
        </section>

        <div className="my-16 h-px w-full bg-border" />

        {/* PHASE 3 */}
        <section
          className="opacity-60"
          style={{ animation: "slideUp 1s var(--ease-out-expo) both" }}
        >
          <div className="mb-8 flex items-end justify-between gap-4">
            <div>
              <h2 className="font-mono text-[10px] font-bold tracking-widest text-muted-foreground uppercase mb-1">
                (03) Validation Engine
              </h2>
              <h1 className="text-2xl font-semibold tracking-tight">Fidelity Evaluation</h1>
            </div>
            <button
              disabled
              className="flex items-center gap-2 bg-accent text-accent-foreground text-xs font-semibold px-4 py-2 rounded shadow-sm transition-all disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <span>Run Scorecard</span>
            </button>
          </div>

          <div className="overflow-hidden rounded-lg ring-1 ring-border bg-card">
            <table className="w-full border-collapse text-left">
              <thead>
                <tr className="bg-muted border-b border-border">
                  <th className="p-4 text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                    Dimension
                  </th>
                  <th className="p-4 text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                    Metric
                  </th>
                  <th className="p-4 text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                    Status
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border text-sm">
                {[
                  ["Claim Decomposition", "Pending"],
                  ["Source Matching", "Pending"],
                  ["Faithfulness Judge", "Pending"],
                  ["Hallucination Check", "Pending"],
                ].map(([dim, metric]) => (
                  <tr key={dim}>
                    <td className="p-4 font-medium">{dim}</td>
                    <td className="p-4 font-mono text-xs text-muted-foreground">{metric}</td>
                    <td className="p-4">
                      <span className="inline-block h-1.5 w-1.5 rounded-full bg-muted-foreground/40" />
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="bg-muted">
                  <td colSpan={2} className="p-4 text-xs font-bold uppercase">
                    Weighted Pipeline Score
                  </td>
                  <td className="p-4 font-mono font-bold text-accent">—</td>
                </tr>
              </tfoot>
            </table>
          </div>
        </section>
      </main>

      <footer className="mx-auto max-w-3xl px-6 py-10 border-t border-border mt-20 flex justify-between items-center">
        <div className="flex items-center gap-2 text-muted-foreground">
          <div className="h-2 w-2 rounded-full bg-muted-foreground/40" />
          <span className="text-[10px] font-mono uppercase tracking-widest">
            Engine: PeersGraphRAG · FastAPI · Neo4j
          </span>
        </div>
        <div className="text-[10px] text-muted-foreground font-mono">
          BACKEND: {backendUrl.replace(/^https?:\/\//, "")}
        </div>
      </footer>
    </div>
  );
}
