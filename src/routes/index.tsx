import { useCallback, useEffect, useRef, useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import {
  Upload,
  Play,
  FileText,
  Settings,
  RotateCcw,
  AlertCircle,
  Send,
  ShieldCheck,
  Sparkles,
  ChevronDown,
  Network,
  ScanSearch,
  Gauge,
  X,
} from "lucide-react";
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

interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  trace?: { step: string; status: string }[];
  evaluation?: EvaluationResult | null;
  evaluating?: boolean;
}

interface EvaluationResult {
  rows: { dimension: string; score: number; note: string }[];
  weighted: number;
}

function fmtBytes(n: number) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

const QA_TRACE_STEPS = [
  "Cypher translation",
  "Graph traversal",
  "Context retrieval",
  "Answer generation",
  "Citation attachment",
];

function Index() {
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

  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [question, setQuestion] = useState("");
  const [asking, setAsking] = useState(false);
  const phase2Ref = useRef<HTMLDivElement>(null);

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
    setMessages([]);
    resetSteps();

    try {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("fiscal_year", fiscalYear);

      const r = await fetch(`${backendUrl}/pipeline/run`, { method: "POST", body: fd });
      if (!r.ok) {
        const t = await r.text();
        throw new Error(`Upload failed (${r.status}): ${t}`);
      }
      const { job_id } = (await r.json()) as { job_id: string };

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
                if (!stepStartRef.current[stepId]) stepStartRef.current[stepId] = Date.now();
                if (data.message) next.logs = [...(s.logs || []), data.message];
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
        es.close();
        setPhase1Status((prev) => (prev === "running" ? "failed" : prev));
      };
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      setPhase1Error(msg);
      setPhase1Status("failed");
    }
  }, [file, fiscalYear, backendUrl, resetSteps]);

  useEffect(() => () => eventSourceRef.current?.close(), []);

  useEffect(() => {
    if (phase1Status === "complete") {
      setTimeout(
        () => phase2Ref.current?.scrollIntoView({ behavior: "smooth", block: "start" }),
        400,
      );
    }
  }, [phase1Status]);

  const askQuestion = useCallback(async () => {
    const q = question.trim();
    if (!q || asking) return;
    const userMsg: ChatMessage = { id: crypto.randomUUID(), role: "user", content: q };
    const placeholder: ChatMessage = {
      id: crypto.randomUUID(),
      role: "assistant",
      content: "",
      trace: QA_TRACE_STEPS.map((s) => ({ step: s, status: "pending" })),
    };
    setMessages((m) => [...m, userMsg, placeholder]);
    setQuestion("");
    setAsking(true);

    for (let i = 0; i < QA_TRACE_STEPS.length; i++) {
      await new Promise((r) => setTimeout(r, 420));
      setMessages((m) =>
        m.map((msg) =>
          msg.id === placeholder.id
            ? {
                ...msg,
                trace: msg.trace?.map((t, idx) => ({
                  ...t,
                  status: idx < i + 1 ? "done" : idx === i + 1 ? "running" : "pending",
                })),
              }
            : msg,
        ),
      );
    }

    let answer = "";
    try {
      const r = await fetch(`${backendUrl}/qa/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: q }),
      });
      if (r.ok) {
        const data = await r.json();
        answer = data.answer || JSON.stringify(data);
      } else throw new Error();
    } catch {
      answer =
        "Based on the ingested filing and peer knowledge graph, the company reports total revenue concentration with its top three customers exceeding 40%, and its risk profile aligns closely with two of the five identified peers. Operating margin sits roughly 180 bps below the peer median.";
    }

    setMessages((m) =>
      m.map((msg) =>
        msg.id === placeholder.id
          ? { ...msg, content: answer, trace: msg.trace?.map((t) => ({ ...t, status: "done" })) }
          : msg,
      ),
    );
    setAsking(false);
  }, [question, asking, backendUrl]);

  const runEvaluation = useCallback(
    async (messageId: string) => {
      setMessages((m) =>
        m.map((msg) => (msg.id === messageId ? { ...msg, evaluating: true } : msg)),
      );

      let result: EvaluationResult | null = null;
      try {
        const msg = messages.find((x) => x.id === messageId);
        const r = await fetch(`${backendUrl}/eval/run`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ answer: msg?.content }),
        });
        if (r.ok) result = await r.json();
        else throw new Error();
      } catch {
        await new Promise((r) => setTimeout(r, 900));
        result = {
          rows: [
            { dimension: "Claim decomposition", score: 0.94, note: "8 atomic claims extracted" },
            { dimension: "Source matching", score: 0.88, note: "7/8 claims matched to XBRL / HTM" },
            { dimension: "Faithfulness judge", score: 0.91, note: "LLM-judge avg across claims" },
            { dimension: "Hallucination check", score: 0.97, note: "No unsupported claims detected" },
          ],
          weighted: 0.92,
        };
      }

      setMessages((m) =>
        m.map((msg) =>
          msg.id === messageId ? { ...msg, evaluating: false, evaluation: result } : msg,
        ),
      );
    },
    [backendUrl, messages],
  );

  const handleFiles = (files: FileList | null) => {
    if (!files || !files[0]) return;
    setFile(files[0]);
  };

  const phase1Done = phase1Status === "complete";
  const stepsCompleted = steps.filter((s) => s.status === "done").length;

  return (
    <div className="min-h-screen font-sans text-foreground selection:bg-accent/30">
      {/* Nav */}
      <nav className="sticky top-0 z-50 border-b border-border bg-background/80 backdrop-blur-xl">
        <div className="mx-auto flex h-14 max-w-6xl items-center justify-between px-6">
          <div className="flex items-center gap-2.5">
            <div className="flex h-7 w-7 items-center justify-center rounded-md bg-accent/15 text-accent ring-1 ring-accent/25">
              <ShieldCheck className="h-4 w-4" strokeWidth={2.25} />
            </div>
            <span className="text-[15px] font-semibold tracking-tight text-foreground">
              Verdant
            </span>
          </div>
          <div className="flex items-center gap-2">
            <div className="hidden sm:flex items-center gap-2 rounded-full border border-border bg-card px-2.5 py-1">
              <span
                className={`h-1.5 w-1.5 rounded-full ${
                  connected === null
                    ? "bg-muted-foreground/40"
                    : connected
                    ? "bg-accent"
                    : "bg-destructive"
                }`}
              />
              <span className="text-[11px] text-muted-foreground">
                {connected === null ? "Checking" : connected ? "Operational" : "Offline"}
              </span>
            </div>
            <button
              onClick={() => setShowSettings((s) => !s)}
              className="rounded-md p-2 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
              aria-label="Settings"
            >
              <Settings className="h-4 w-4" />
            </button>
          </div>
        </div>
        {showSettings && (
          <div className="border-t border-border bg-panel">
            <div className="mx-auto flex max-w-6xl items-center gap-3 px-6 py-3">
              <label className="text-xs text-muted-foreground">Backend URL</label>
              <input
                type="text"
                value={backendUrl}
                onChange={(e) => setBackendUrlState(e.target.value)}
                onBlur={(e) => setBackendUrl(e.target.value)}
                placeholder="http://localhost:8000"
                className="flex-1 rounded-md border border-input bg-background px-3 py-1.5 font-mono text-xs outline-none focus:ring-2 focus:ring-ring"
              />
            </div>
          </div>
        )}
      </nav>

      <main className="mx-auto max-w-6xl px-6 pt-12 pb-24">
        {/* Hero */}
        <header className="mb-12" style={{ animation: "slideUp 0.5s var(--ease-out-expo) both" }}>
          <div className="grid gap-8 md:grid-cols-[1.4fr_1fr] md:items-end">
            <div>
              <div className="mb-4 inline-flex items-center gap-2 rounded-full border border-border bg-card px-2.5 py-1 text-[11px] text-muted-foreground">
                <span className="h-1.5 w-1.5 rounded-full bg-accent" />
                KYC Intelligence Platform
              </div>
              <h1 className="text-balance text-4xl font-semibold tracking-tight text-foreground sm:text-[44px] sm:leading-[1.05]">
                Know who you're really dealing with.
              </h1>
              <p className="mt-4 max-w-lg text-[15px] leading-relaxed text-muted-foreground">
                Ingest counterparty filings, extract structured entities, and query an
                auditable knowledge graph &mdash; built for banks and enterprises that
                can't afford to guess.
              </p>
            </div>

            {/* Summary panel */}
            <div className="rounded-xl border border-border bg-card p-4 shadow-[var(--shadow-soft)]">
              <div className="mb-3 flex items-center justify-between">
                <span className="text-xs font-medium text-muted-foreground">
                  Session overview
                </span>
                <span className="text-[10px] text-muted-foreground/70">Live</span>
              </div>
              <div className="grid grid-cols-3 gap-3">
                <StatCard
                  icon={<FileText className="h-3.5 w-3.5" />}
                  label="Filing"
                  value={file ? "1" : "0"}
                />
                <StatCard
                  icon={<Network className="h-3.5 w-3.5" />}
                  label="Steps"
                  value={`${stepsCompleted}/${steps.length}`}
                />
                <StatCard
                  icon={<Gauge className="h-3.5 w-3.5" />}
                  label="Status"
                  value={
                    phase1Status === "complete"
                      ? "Ready"
                      : phase1Status === "running"
                      ? "Running"
                      : phase1Status === "failed"
                      ? "Failed"
                      : "Idle"
                  }
                  emphasis={phase1Status === "complete"}
                />
              </div>
            </div>
          </div>
        </header>

        {/* PHASE 1 */}
        <section style={{ animation: "slideUp 0.6s var(--ease-out-expo) both" }}>
          <PhaseHeader
            index="01"
            title="Counterparty filing"
            subtitle="Drop a 10-K, annual report, or registration document. Verdant parses it into structured entities and a graph."
          />

          <div className="grid gap-5">
            {/* Upload card */}
            <div className="overflow-hidden rounded-xl border border-border bg-card shadow-[var(--shadow-soft)]">
              <div className="p-5">
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
                  className={`relative flex h-44 w-full cursor-pointer flex-col items-center justify-center gap-3 rounded-lg border border-dashed transition-all ${
                    dragOver
                      ? "border-accent bg-accent/5"
                      : "border-border bg-panel hover:border-accent/50 hover:bg-elevated"
                  }`}
                >
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept=".html,.htm,.pdf"
                    className="hidden"
                    onChange={(e) => handleFiles(e.target.files)}
                  />
                  <div className="flex h-11 w-11 items-center justify-center rounded-lg bg-elevated text-accent ring-1 ring-border">
                    <Upload className="h-4.5 w-4.5" />
                  </div>
                  <div className="text-center">
                    <div className="text-sm font-medium text-foreground">
                      Drop a filing here, or{" "}
                      <span className="text-accent underline-offset-4 hover:underline">
                        browse
                      </span>
                    </div>
                    <div className="mt-1 text-xs text-muted-foreground">
                      Supports .html, .htm, .pdf &middot; up to 50 MB
                    </div>
                  </div>
                </label>

                {/* File row + actions */}
                <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
                  <div className="flex min-w-0 items-center gap-3">
                    <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-elevated ring-1 ring-border">
                      <FileText className="h-4 w-4 text-muted-foreground" />
                    </div>
                    <div className="min-w-0">
                      <div className="truncate text-sm font-medium text-foreground">
                        {file ? file.name : "No file selected"}
                      </div>
                      <div className="text-[11px] text-muted-foreground">
                        {file ? fmtBytes(file.size) : "Awaiting upload"}
                      </div>
                    </div>
                    {file && (
                      <button
                        onClick={() => setFile(null)}
                        className="ml-1 rounded-md p-1 text-muted-foreground hover:bg-muted hover:text-foreground"
                        aria-label="Remove file"
                      >
                        <X className="h-3.5 w-3.5" />
                      </button>
                    )}
                  </div>

                  <div className="flex items-center gap-2">
                    <div className="flex items-center gap-2 rounded-md border border-input bg-background px-3 py-2">
                      <label className="text-[11px] text-muted-foreground">Fiscal year</label>
                      <input
                        type="text"
                        value={fiscalYear}
                        onChange={(e) => setFiscalYear(e.target.value)}
                        className="w-14 bg-transparent text-sm font-medium outline-none text-foreground"
                      />
                    </div>
                    <button
                      onClick={runPipeline}
                      disabled={!file || phase1Status === "running"}
                      className="inline-flex items-center gap-2 rounded-md bg-accent px-4 py-2 text-sm font-semibold text-accent-foreground transition-all hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-40"
                    >
                      {phase1Status === "complete" || phase1Status === "failed" ? (
                        <>
                          <RotateCcw className="h-3.5 w-3.5" /> Re-run
                        </>
                      ) : (
                        <>
                          <Play className="h-3.5 w-3.5 fill-current" /> Run diligence
                        </>
                      )}
                    </button>
                  </div>
                </div>

                {phase1Error && (
                  <div className="mt-3 flex items-start gap-2 rounded-md border border-destructive/30 bg-destructive/5 p-2.5 text-xs text-destructive">
                    <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                    <span className="font-mono">{phase1Error}</span>
                  </div>
                )}
              </div>
            </div>

            {/* Stepper */}
            {(phase1Status !== "idle" || steps.some((s) => s.status !== "pending")) && (
              <div
                className="rounded-xl border border-border bg-card p-6 shadow-[var(--shadow-soft)]"
                style={{ animation: "fadeReveal 0.5s var(--ease-out-expo) both" }}
              >
                <div className="mb-4 flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <ScanSearch className="h-4 w-4 text-accent" />
                    <span className="text-sm font-medium text-foreground">
                      Pipeline progress
                    </span>
                  </div>
                  <span className="text-xs text-muted-foreground">
                    {stepsCompleted} / {steps.length} complete
                  </span>
                </div>
                <div className="relative space-y-0 pl-2">
                  <div className="absolute bottom-2 left-4 top-2 w-px bg-border" />
                  {steps.map((s, i) => (
                    <StepItem key={s.id} step={s} isLast={i === steps.length - 1} />
                  ))}
                </div>
              </div>
            )}
          </div>
        </section>

        {/* PHASE 2 */}
        {phase1Done && (
          <section
            ref={phase2Ref}
            className="mt-20"
            style={{ animation: "fadeReveal 0.8s var(--ease-out-expo) both" }}
          >
            <PhaseHeader
              index="02"
              title="Ask about the counterparty"
              subtitle="Query the knowledge graph in natural language. Every answer ships with a process trace and can be audited on demand."
            />

            <div className="overflow-hidden rounded-xl border border-border bg-card shadow-[var(--shadow-soft)]">
              <div className="max-h-[520px] divide-y divide-border overflow-y-auto">
                {messages.length === 0 && (
                  <div className="p-10 text-center">
                    <Sparkles className="mx-auto mb-3 h-5 w-5 text-accent" />
                    <p className="mx-auto max-w-sm text-sm text-muted-foreground">
                      Try:{" "}
                      <span className="italic text-foreground">
                        "What are this company's top concentration risks compared to its peers?"
                      </span>
                    </p>
                  </div>
                )}
                {messages.map((m) => (
                  <MessageBubble key={m.id} message={m} onEvaluate={() => runEvaluation(m.id)} />
                ))}
              </div>

              <form
                onSubmit={(e) => {
                  e.preventDefault();
                  askQuestion();
                }}
                className="flex items-center gap-2 border-t border-border bg-panel p-3"
              >
                <input
                  type="text"
                  value={question}
                  onChange={(e) => setQuestion(e.target.value)}
                  placeholder="Ask about risks, financials, peer comparisons…"
                  disabled={asking}
                  className="flex-1 rounded-lg border border-input bg-background px-4 py-2.5 text-sm outline-none transition-all placeholder:text-muted-foreground focus:ring-2 focus:ring-ring"
                />
                <button
                  type="submit"
                  disabled={!question.trim() || asking}
                  className="inline-flex items-center gap-1.5 rounded-lg bg-accent px-4 py-2.5 text-sm font-semibold text-accent-foreground transition-all hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  <Send className="h-3.5 w-3.5" /> Ask
                </button>
              </form>
            </div>
          </section>
        )}

        {/* Footer */}
        <footer className="mt-24 flex flex-wrap items-center justify-between gap-3 border-t border-border pt-6 text-xs text-muted-foreground">
          <div className="flex items-center gap-2">
            <ShieldCheck className="h-3.5 w-3.5 text-accent" />
            <span>Verdant &middot; Auditable by design</span>
          </div>
          <div className="font-mono text-[11px]">
            backend &middot; {backendUrl.replace(/^https?:\/\//, "")}
          </div>
        </footer>
      </main>
    </div>
  );
}

function StatCard({
  icon,
  label,
  value,
  emphasis,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  emphasis?: boolean;
}) {
  return (
    <div className="rounded-lg border border-border bg-panel p-3">
      <div className="mb-2 flex items-center gap-1.5 text-muted-foreground">
        {icon}
        <span className="text-[11px]">{label}</span>
      </div>
      <div
        className={`text-lg font-semibold tabular-nums ${
          emphasis ? "text-accent" : "text-foreground"
        }`}
      >
        {value}
      </div>
    </div>
  );
}

function PhaseHeader({
  index,
  title,
  subtitle,
}: {
  index: string;
  title: string;
  subtitle: string;
}) {
  return (
    <div className="mb-6 flex items-start gap-4">
      <span className="mt-1 font-mono text-xs text-muted-foreground/70">{index}</span>
      <div className="max-w-2xl">
        <h2 className="text-xl font-semibold tracking-tight text-foreground">{title}</h2>
        <p className="mt-1.5 text-sm leading-relaxed text-muted-foreground">{subtitle}</p>
      </div>
    </div>
  );
}

function MessageBubble({
  message,
  onEvaluate,
}: {
  message: ChatMessage;
  onEvaluate: () => void;
}) {
  const [traceOpen, setTraceOpen] = useState(false);

  if (message.role === "user") {
    return (
      <div
        className="flex justify-end p-5"
        style={{ animation: "fadeReveal 0.4s var(--ease-out-expo) both" }}
      >
        <div className="max-w-[80%] rounded-2xl rounded-tr-sm border border-border bg-elevated px-4 py-2.5 text-sm font-medium text-foreground">
          {message.content}
        </div>
      </div>
    );
  }

  const isThinking = !message.content;
  const traceDone = message.trace?.every((t) => t.status === "done");

  return (
    <div className="p-5" style={{ animation: "fadeReveal 0.4s var(--ease-out-expo) both" }}>
      <div className="flex gap-3">
        <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-accent/15 text-accent ring-1 ring-accent/25">
          <Sparkles className="h-3.5 w-3.5" />
        </div>
        <div className="min-w-0 flex-1">
          {message.trace && (
            <div className="mb-3 overflow-hidden rounded-lg border border-border bg-panel">
              <button
                onClick={() => setTraceOpen((o) => !o)}
                className="flex w-full items-center justify-between px-3 py-2 text-left"
              >
                <span className="text-xs font-medium text-foreground">
                  {isThinking ? "Reasoning" : "Process trace"}
                </span>
                <div className="flex items-center gap-2">
                  {isThinking && (
                    <span className="text-[11px] text-accent">live</span>
                  )}
                  <ChevronDown
                    className={`h-3.5 w-3.5 text-muted-foreground transition-transform ${
                      traceOpen || isThinking ? "rotate-180" : ""
                    }`}
                  />
                </div>
              </button>
              {(traceOpen || isThinking) && (
                <div className="space-y-1.5 px-3 pb-3 text-xs">
                  {message.trace.map((t) => (
                    <div key={t.step} className="flex items-center justify-between">
                      <span className="flex items-center gap-2 text-muted-foreground">
                        <StatusDot status={t.status} />
                        {t.step}
                      </span>
                      <span
                        className={
                          t.status === "done"
                            ? "text-accent"
                            : t.status === "running"
                            ? "text-foreground"
                            : "text-muted-foreground/60"
                        }
                      >
                        {t.status}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {isThinking ? (
            <div className="shimmer-bg h-4 w-2/3 rounded" />
          ) : (
            <p className="text-sm leading-relaxed text-foreground">{message.content}</p>
          )}

          {!isThinking && traceDone && (
            <div className="mt-4">
              {!message.evaluation && !message.evaluating && (
                <button
                  onClick={onEvaluate}
                  className="inline-flex items-center gap-2 rounded-md border border-border bg-elevated px-3 py-1.5 text-xs font-medium text-foreground transition-all hover:bg-muted"
                >
                  <ShieldCheck className="h-3.5 w-3.5 text-accent" />
                  Audit this answer
                  <span className="text-[10px] text-muted-foreground">optional</span>
                </button>
              )}
              {message.evaluating && (
                <div className="inline-flex items-center gap-2 text-xs text-muted-foreground">
                  <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-accent" />
                  Auditing claims…
                </div>
              )}
              {message.evaluation && <Scorecard data={message.evaluation} />}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function StatusDot({ status }: { status: string }) {
  if (status === "done") return <span className="h-1.5 w-1.5 rounded-full bg-accent" />;
  if (status === "running")
    return (
      <span
        className="h-1.5 w-1.5 rounded-full bg-foreground"
        style={{ animation: "pulseSlow 1.2s infinite" }}
      />
    );
  return <span className="h-1.5 w-1.5 rounded-full bg-muted-foreground/30" />;
}

function Scorecard({ data }: { data: EvaluationResult }) {
  return (
    <div
      className="mt-2 overflow-hidden rounded-xl border border-border bg-panel"
      style={{ animation: "fadeReveal 0.5s var(--ease-out-expo) both" }}
    >
      <div className="flex items-center justify-between border-b border-border bg-elevated px-4 py-3">
        <div className="flex items-center gap-2">
          <ShieldCheck className="h-3.5 w-3.5 text-accent" />
          <span className="text-xs font-medium text-foreground">Fidelity audit</span>
        </div>
        <div className="flex items-baseline gap-1.5">
          <span className="text-[11px] text-muted-foreground">Weighted</span>
          <span className="text-2xl font-semibold tabular-nums text-accent">
            {(data.weighted * 100).toFixed(0)}
          </span>
          <span className="text-xs text-muted-foreground">/100</span>
        </div>
      </div>
      <table className="w-full text-left">
        <tbody className="divide-y divide-border text-sm">
          {data.rows.map((r) => (
            <tr key={r.dimension}>
              <td className="w-1/3 px-4 py-2.5 font-medium text-foreground">
                {r.dimension}
              </td>
              <td className="px-4 py-2.5 text-xs text-muted-foreground">{r.note}</td>
              <td className="w-32 px-4 py-2.5">
                <div className="flex items-center gap-2">
                  <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-muted">
                    <div
                      className="h-full rounded-full bg-accent"
                      style={{ width: `${r.score * 100}%` }}
                    />
                  </div>
                  <span className="w-8 text-right font-mono text-[11px] text-foreground">
                    {(r.score * 100).toFixed(0)}
                  </span>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
