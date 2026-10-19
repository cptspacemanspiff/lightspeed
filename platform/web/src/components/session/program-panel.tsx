import { useEffect, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ChevronDown, ChevronRight, CircleStop, LoaderCircle, Play } from "lucide-react";
import type {
  ContentPreviewView,
  ProgramCallCountsView,
  ProgramCallStatusView,
  ProgramCallView,
  RunStatus,
  RunSummaryView,
  SessionDriverView,
} from "@lightspeed-ai/sdk";
import { api } from "@/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { formatDuration } from "@/lib/sessions/transcript";
import { cn } from "@/lib/utils";

/// How often a page still changing is read again.
const LIVE_REFRESH_MS = 2_000;
const RUN_PAGE = 20;
const CALL_PAGE = 50;

type ProgramDriver = Extract<SessionDriverView, { kind: "program" }>;

interface RunPage {
  runs: RunSummaryView[];
  nextCursor: string | null;
}

interface CallPage {
  calls: ProgramCallView[];
  nextCursor: string | null;
  counts: ProgramCallCountsView;
}

export function isProgramDriver(driver: SessionDriverView | undefined | null): driver is ProgramDriver {
  return driver?.kind === "program";
}

/// Whether a run may still change: its status, calls and outcome.
function runLive(status: RunStatus): boolean {
  return status === "queued" || status === "running" || status === "parked" || status === "cancelling";
}

/// A program session's runs: start one with JSON input, follow each run's
/// calls as they happen, and open what a call started (a child session, an
/// inbox request).
export function ProgramPanel({
  universeId,
  slug,
  sessionId,
  driver,
  runRevision,
  canControl,
  closed,
  sessionHref,
  loadFullText,
}: {
  universeId: string;
  slug: string;
  sessionId: string;
  driver: ProgramDriver;
  /// Changes whenever the session's runs do; the run list is read again.
  runRevision: number;
  canControl: boolean;
  closed: boolean;
  sessionHref: (sessionId: string) => string;
  loadFullText: (blobRef: string) => Promise<string>;
}) {
  const queryClient = useQueryClient();
  const runsKey = ["program-runs", universeId, sessionId];
  const runs = useInfiniteQuery({
    queryKey: runsKey,
    initialPageParam: null as string | null,
    queryFn: ({ pageParam }) =>
      api<RunPage>(
        "GET",
        `/api/v1/universes/${universeId}/sessions/${sessionId}/runs?limit=${RUN_PAGE}${pageParam ? `&cursor=${encodeURIComponent(pageParam)}` : ""}`,
      ),
    getNextPageParam: (page) => page.nextCursor,
    refetchInterval: (query) =>
      query.state.data?.pages[0]?.runs.some((run) => runLive(run.status)) ? LIVE_REFRESH_MS : false,
  });
  useEffect(() => {
    void queryClient.invalidateQueries({ queryKey: ["program-runs", universeId, sessionId] });
  }, [queryClient, universeId, sessionId, runRevision]);
  const allRuns = runs.data?.pages.flatMap((page) => page.runs) ?? [];
  // The runs shown open; until one is opened or closed, the newest.
  const [expanded, setExpanded] = useState<Set<string> | null>(null);
  const newest = allRuns[0]?.id;
  const openRuns = expanded ?? new Set(newest ? [newest] : []);
  const toggle = (runId: string) => {
    const next = new Set(openRuns);
    if (next.has(runId)) next.delete(runId);
    else next.add(runId);
    setExpanded(next);
  };

  return (
    <div className="min-h-0 flex-1 overflow-y-auto">
      <div className="mx-auto flex w-full max-w-5xl flex-col gap-4 px-4 py-6 md:px-8">
        <ProgramHeader driver={driver} loadFullText={loadFullText} />
        {canControl && !closed && (
          <StartRunForm
            universeId={universeId}
            sessionId={sessionId}
            onStarted={(runId) => {
              setExpanded(new Set([...openRuns, runId]));
              void queryClient.invalidateQueries({ queryKey: runsKey });
            }}
          />
        )}
        <section aria-label="Runs" className="flex flex-col gap-2">
          <h2 className="text-sm font-medium">Runs</h2>
          {runs.isLoading && <p className="text-sm text-muted-foreground">Loading runs…</p>}
          {runs.error && <p className="text-sm text-destructive">Could not read runs: {runs.error.message}</p>}
          {runs.data && allRuns.length === 0 && (
            <p className="text-sm text-muted-foreground">No runs yet.</p>
          )}
          {allRuns.map((run) => (
            <RunCard
              key={run.id}
              universeId={universeId}
              slug={slug}
              sessionId={sessionId}
              run={run}
              open={openRuns.has(run.id)}
              onToggle={() => toggle(run.id)}
              canControl={canControl}
              sessionHref={sessionHref}
              loadFullText={loadFullText}
            />
          ))}
          {runs.hasNextPage && (
            <Button variant="ghost" size="sm" className="self-start" disabled={runs.isFetchingNextPage}
              onClick={() => void runs.fetchNextPage()}>
              {runs.isFetchingNextPage ? "Loading…" : "Older runs"}
            </Button>
          )}
        </section>
      </div>
    </div>
  );
}

function ProgramHeader({ driver, loadFullText }: { driver: ProgramDriver; loadFullText: (blobRef: string) => Promise<string> }) {
  const [showSchema, setShowSchema] = useState(false);
  const schemaRef = driver.inputSchemaRef ?? null;
  const schema = useQuery({
    queryKey: ["blob-text", schemaRef],
    queryFn: () => loadFullText(schemaRef!),
    enabled: showSchema && schemaRef !== null,
    staleTime: Infinity,
  });
  return (
    <div className="flex flex-col gap-1">
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="text-sm font-medium">Program</span>
        <span className="font-mono text-sm">{driver.programId ?? "inline"}</span>
        {driver.revision !== undefined && driver.revision !== null && (
          <span className="text-xs text-muted-foreground">revision {driver.revision}</span>
        )}
        {schemaRef && (
          <Button variant="ghost" size="sm" className="h-auto px-1 py-0 text-xs" onClick={() => setShowSchema((value) => !value)}>
            {showSchema ? "Hide input schema" : "Input schema"}
          </Button>
        )}
      </div>
      {showSchema && (
        <pre className="max-h-64 overflow-auto rounded-md bg-muted p-2 text-xs">
          {schema.data ? prettyJson(schema.data) : schema.error ? `Could not read the schema: ${schema.error.message}` : "Loading…"}
        </pre>
      )}
    </div>
  );
}

function StartRunForm({
  universeId,
  sessionId,
  onStarted,
}: {
  universeId: string;
  sessionId: string;
  onStarted: (runId: string) => void;
}) {
  const [text, setText] = useState("{}");
  const [parseError, setParseError] = useState<string | null>(null);
  const start = useMutation({
    mutationFn: (input: unknown) =>
      api<{ run: { id: string; status: RunStatus } }>(
        "POST",
        `/api/v1/universes/${universeId}/sessions/${sessionId}/program-runs`,
        { input, submissionId: crypto.randomUUID() },
      ),
    onSuccess: (response) => onStarted(response.run.id),
  });
  const submit = (event: FormEvent) => {
    event.preventDefault();
    let input: unknown;
    try {
      input = JSON.parse(text);
    } catch (error) {
      setParseError(error instanceof Error ? error.message : "Not JSON");
      return;
    }
    setParseError(null);
    start.mutate(input);
  };
  return (
    <form onSubmit={submit} className="flex flex-col gap-2 rounded-lg border p-3" aria-label="Start a run">
      <label htmlFor="program-run-input" className="text-sm font-medium">Input</label>
      <Textarea
        id="program-run-input"
        className="min-h-24 font-mono text-xs"
        value={text}
        onChange={(event) => setText(event.target.value)}
        spellCheck={false}
      />
      <div className="flex items-center gap-2">
        <p className="min-w-0 flex-1 text-xs text-destructive" role="alert">
          {parseError ? `Input is not JSON: ${parseError}` : start.error ? start.error.message : ""}
        </p>
        <Button type="submit" size="sm" disabled={start.isPending}>
          {start.isPending ? <LoaderCircle className="animate-spin" /> : <Play />}
          Start run
        </Button>
      </div>
    </form>
  );
}

const RUN_STATUS_LABEL: Record<RunStatus, string> = {
  queued: "Queued",
  running: "Running",
  parked: "Waiting",
  cancelling: "Cancelling",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
};

function statusVariant(status: RunStatus | ProgramCallStatusView): "default" | "secondary" | "destructive" | "outline" {
  if (status === "failed") return "destructive";
  if (status === "completed" || status === "succeeded") return "secondary";
  return "outline";
}

function RunCard({
  universeId,
  slug,
  sessionId,
  run,
  open,
  onToggle,
  canControl,
  sessionHref,
  loadFullText,
}: {
  universeId: string;
  slug: string;
  sessionId: string;
  run: RunSummaryView;
  open: boolean;
  onToggle: () => void;
  canControl: boolean;
  sessionHref: (sessionId: string) => string;
  loadFullText: (blobRef: string) => Promise<string>;
}) {
  const queryClient = useQueryClient();
  const cancel = useMutation({
    mutationFn: () =>
      api("POST", `/api/v1/universes/${universeId}/sessions/${sessionId}/runs/${run.id}/cancel`),
    onSettled: () => void queryClient.invalidateQueries({ queryKey: ["program-runs", universeId, sessionId] }),
  });
  const live = runLive(run.status);
  const took = run.startedAtMs && run.completedAtMs ? formatDuration(run.completedAtMs - run.startedAtMs) : null;
  return (
    <div className="rounded-lg border" data-run={run.id}>
      <div className="flex items-center gap-2 px-3 py-2">
        <button type="button" onClick={onToggle} aria-expanded={open}
          className="flex min-w-0 flex-1 items-center gap-2 text-left text-sm">
          {open ? <ChevronDown className="size-4 shrink-0" /> : <ChevronRight className="size-4 shrink-0" />}
          <span className="font-mono">{run.id}</span>
          <Badge variant={statusVariant(run.status)}>
            {live && run.status !== "parked" && <LoaderCircle className="animate-spin" />}
            {RUN_STATUS_LABEL[run.status]}
          </Badge>
          {took && <span className="text-xs text-muted-foreground">in {took}</span>}
          {run.failure && (
            <span className="min-w-0 truncate text-xs text-destructive">{run.failure.message}</span>
          )}
        </button>
        {canControl && live && run.status !== "cancelling" && (
          <Button variant="ghost" size="sm" disabled={cancel.isPending} onClick={() => cancel.mutate()}>
            <CircleStop /> Cancel
          </Button>
        )}
      </div>
      {open && (
        <div className="flex flex-col gap-3 border-t px-3 py-3">
          {run.source.type === "program" && (
            <StoredJson label="Input" blobRef={run.source.inputRef} loadFullText={loadFullText} />
          )}
          {run.output && (
            <StoredJson label="Output" blobRef={run.output.contentRef} loadFullText={loadFullText} />
          )}
          {run.failure && (
            <div className="flex flex-col gap-1">
              <span className="text-xs font-medium">Failure ({run.failure.kind})</span>
              <pre className="max-h-64 overflow-auto rounded-md bg-muted p-2 text-xs whitespace-pre-wrap">
                {prettyJson(run.failure.message)}{run.failure.messageTruncated ? "…" : ""}
              </pre>
            </div>
          )}
          <CallList
            universeId={universeId}
            slug={slug}
            sessionId={sessionId}
            runId={run.id}
            live={live}
            sessionHref={sessionHref}
            loadFullText={loadFullText}
          />
        </div>
      )}
    </div>
  );
}

function StoredJson({ label, blobRef, loadFullText }: { label: string; blobRef: string; loadFullText: (blobRef: string) => Promise<string> }) {
  const text = useQuery({
    queryKey: ["blob-text", blobRef],
    queryFn: () => loadFullText(blobRef),
    staleTime: Infinity,
  });
  return (
    <div className="flex flex-col gap-1">
      <span className="text-xs font-medium">{label}</span>
      <pre className="max-h-64 overflow-auto rounded-md bg-muted p-2 text-xs whitespace-pre-wrap">
        {text.data !== undefined ? prettyJson(text.data) : text.error ? `Could not read it: ${text.error.message}` : "Loading…"}
      </pre>
    </div>
  );
}

const CALL_FILTERS: { label: string; status: ProgramCallStatusView | null }[] = [
  { label: "All", status: null },
  { label: "Failed", status: "failed" },
  { label: "Pending", status: "pending" },
  { label: "Waiting", status: "waiting" },
];

function CallList({
  universeId,
  slug,
  sessionId,
  runId,
  live,
  sessionHref,
  loadFullText,
}: {
  universeId: string;
  slug: string;
  sessionId: string;
  runId: string;
  live: boolean;
  sessionHref: (sessionId: string) => string;
  loadFullText: (blobRef: string) => Promise<string>;
}) {
  const [filter, setFilter] = useState<ProgramCallStatusView | null>(null);
  const calls = useInfiniteQuery({
    queryKey: ["program-calls", universeId, sessionId, runId, filter],
    initialPageParam: null as string | null,
    queryFn: ({ pageParam }) => {
      const query = new URLSearchParams({ limit: String(CALL_PAGE) });
      if (pageParam) query.set("cursor", pageParam);
      if (filter) query.set("status", filter);
      return api<CallPage>(
        "GET",
        `/api/v1/universes/${universeId}/sessions/${sessionId}/runs/${runId}/calls?${query}`,
      );
    },
    getNextPageParam: (page) => page.nextCursor,
    refetchInterval: live ? LIVE_REFRESH_MS : false,
  });
  const counts = calls.data?.pages[0]?.counts;
  const all = calls.data?.pages.flatMap((page) => page.calls) ?? [];
  return (
    <section aria-label="Calls" className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-xs font-medium">Calls</span>
        {counts && <span className="text-xs text-muted-foreground">{countsText(counts)}</span>}
        <div className="ml-auto flex gap-1" role="group" aria-label="Show calls">
          {CALL_FILTERS.map((option) => (
            <Button key={option.label} variant={filter === option.status ? "secondary" : "ghost"} size="sm"
              className="h-6 px-2 text-xs" aria-pressed={filter === option.status}
              onClick={() => setFilter(option.status)}>
              {option.label}
            </Button>
          ))}
        </div>
      </div>
      {calls.isLoading && <p className="text-xs text-muted-foreground">Loading calls…</p>}
      {calls.error && <p className="text-xs text-destructive">Could not read calls: {calls.error.message}</p>}
      {calls.data && all.length === 0 && <p className="text-xs text-muted-foreground">No calls.</p>}
      <ol className="flex flex-col divide-y rounded-md border">
        {all.map((call) => (
          <CallRow key={call.id} call={call} slug={slug} sessionHref={sessionHref} loadFullText={loadFullText} />
        ))}
      </ol>
      {calls.hasNextPage && (
        <Button variant="ghost" size="sm" className="self-start" disabled={calls.isFetchingNextPage}
          onClick={() => void calls.fetchNextPage()}>
          {calls.isFetchingNextPage ? "Loading…" : "More calls"}
        </Button>
      )}
    </section>
  );
}

function countsText(counts: ProgramCallCountsView): string {
  const parts = [`${counts.total} total`];
  if (counts.succeeded) parts.push(`${counts.succeeded} succeeded`);
  if (counts.failed) parts.push(`${counts.failed} failed`);
  if (counts.pending) parts.push(`${counts.pending} pending`);
  if (counts.waiting) parts.push(`${counts.waiting} waiting`);
  if (counts.cancelled) parts.push(`${counts.cancelled} cancelled`);
  return parts.join(" · ");
}

const CALL_STATUS_LABEL: Record<ProgramCallStatusView, string> = {
  pending: "Pending",
  waiting: "Waiting",
  succeeded: "Succeeded",
  failed: "Failed",
  cancelled: "Cancelled",
};

function CallRow({
  call,
  slug,
  sessionHref,
  loadFullText,
}: {
  call: ProgramCallView;
  slug: string;
  sessionHref: (sessionId: string) => string;
  loadFullText: (blobRef: string) => Promise<string>;
}) {
  const [open, setOpen] = useState(false);
  const took = call.completedAtMs !== undefined && call.completedAtMs !== null
    ? formatDuration(Math.max(0, call.completedAtMs - call.admittedAtMs))
    : null;
  return (
    <li className="flex flex-col" data-call={call.requestId}>
      <div className="flex min-w-0 items-center gap-2 px-2 py-1.5 text-xs">
        <button type="button" onClick={() => setOpen((value) => !value)} aria-expanded={open}
          className="flex min-w-0 flex-1 items-center gap-2 text-left">
          {open ? <ChevronDown className="size-3.5 shrink-0" /> : <ChevronRight className="size-3.5 shrink-0" />}
          <span className="shrink-0 font-mono text-muted-foreground">
            {call.execution > 0 ? `#${call.execution + 1} ` : ""}{call.requestId}
          </span>
          <span className="shrink-0 font-mono">{call.contract ?? call.tool}</span>
          <Badge variant={statusVariant(call.status)} className="shrink-0">
            {(call.status === "pending") && <LoaderCircle className="animate-spin" />}
            {CALL_STATUS_LABEL[call.status]}
          </Badge>
          {took && <span className="shrink-0 text-muted-foreground">{took}</span>}
          {call.annotation
            ? <span className="min-w-0 truncate italic text-muted-foreground" title={call.annotation}>{call.annotation}</span>
            : <span className="min-w-0 truncate text-muted-foreground">{oneLine(call.arguments.preview)}</span>}
        </button>
        {call.childSessionId && (
          <Link className="shrink-0 underline-offset-2 hover:underline" to={sessionHref(call.childSessionId)}>
            Child session
          </Link>
        )}
        {call.inputRequestId && (
          <Link className="shrink-0 underline-offset-2 hover:underline" to={`/u/${slug}/inbox/${call.inputRequestId}`}>
            Inbox request
          </Link>
        )}
      </div>
      {open && (
        <div className={cn("flex flex-col gap-2 px-7 pb-2")}>
          {call.annotation && (
            <p className="text-xs"><span className="font-medium">Why: </span>{call.annotation}</p>
          )}
          <Preview label="Arguments" preview={call.arguments} loadFullText={loadFullText} />
          {call.output && <Preview label="Result" preview={call.output} loadFullText={loadFullText} />}
          {call.error && <Preview label="Error" preview={call.error} loadFullText={loadFullText} />}
          {call.cancelRequested && <span className="text-xs text-muted-foreground">The program asked to cancel this call.</span>}
        </div>
      )}
    </li>
  );
}

function Preview({ label, preview, loadFullText }: { label: string; preview: ContentPreviewView; loadFullText: (blobRef: string) => Promise<string> }) {
  const [full, setFull] = useState(false);
  const text = useQuery({
    queryKey: ["blob-text", preview.contentRef],
    queryFn: () => loadFullText(preview.contentRef),
    enabled: full,
    staleTime: Infinity,
  });
  const shown = full && text.data !== undefined ? text.data : preview.preview;
  return (
    <div className="flex flex-col gap-1">
      <span className="text-xs font-medium">{label}</span>
      <pre className="max-h-64 overflow-auto rounded-md bg-muted p-2 text-xs whitespace-pre-wrap">
        {prettyJson(shown)}{!full && preview.previewTruncated ? "…" : ""}
      </pre>
      {preview.previewTruncated && !full && (
        <Button variant="ghost" size="sm" className="h-6 self-start px-2 text-xs" onClick={() => setFull(true)}>
          Show all
        </Button>
      )}
      {full && text.error && <span className="text-xs text-destructive">Could not read it: {text.error.message}</span>}
    </div>
  );
}

/// JSON shown indented; anything else as it is.
function prettyJson(text: string): string {
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

function oneLine(text: string): string {
  return text.replace(/\s+/g, " ").slice(0, 160);
}
