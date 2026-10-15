import { useState, type ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, NavLink, useParams } from "react-router-dom";
import { ChevronRight, Inbox } from "lucide-react";
import {
  api,
  type Attribution,
  type InboxRequestListResponse,
  type InboxRequestResponse,
  type InputRequestStatus,
  type InputRequestView,
} from "@/api";
import { ListPane } from "@/components/list-pane";
import { DetailPrompt, ListNote, LoadingNote, UniverseNotFound } from "@/components/page";
import { ReadError } from "@/components/read-error";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { useActionPermissions } from "@/lib/permissions";
import { useActiveUniverse } from "@/lib/universes";
import { cn } from "@/lib/utils";

/// The universe's human-input inbox: questions program runs are waiting on.
/// Pane = requests, pending first by default; detail = the question, its
/// context and the answer form. Answering needs control of the request's
/// session, which the runtime checks.
export function InboxPage(_props: { admin: boolean }) {
  const { universe, slug, isLoading } = useActiveUniverse();
  const { requestId } = useParams<{ requestId: string }>();
  const [filter, setFilter] = useState<InboxFilter>("pending");

  if (isLoading) {
    return <LoadingNote />;
  }
  if (!universe || !universe.role) {
    return (
      <div className="p-6">
        <UniverseNotFound slug={slug} />
      </div>
    );
  }

  return (
    <div className="flex min-h-0 min-w-0 flex-1">
      <ListPane detailOpen={Boolean(requestId)}>
        <RequestPane
          universeId={universe.id}
          slug={slug!}
          activeId={requestId}
          filter={filter}
          onFilter={setFilter}
        />
      </ListPane>
      <div className={cn("min-w-0 flex-1 flex-col", requestId ? "flex" : "hidden md:flex")}>
        {requestId ? (
          <RequestDetail key={requestId} universeId={universe.id} slug={slug!} requestId={requestId} />
        ) : (
          <DetailPrompt icon={<Inbox className="size-10 text-muted-foreground/60" />}>
            Pick a request to answer it.
          </DetailPrompt>
        )}
      </div>
    </div>
  );
}

type InboxFilter = "pending" | "all";

function RequestPane({
  universeId,
  slug,
  activeId,
  filter,
  onFilter,
}: {
  universeId: string;
  slug: string;
  activeId: string | undefined;
  filter: InboxFilter;
  onFilter: (filter: InboxFilter) => void;
}) {
  const requests = useQuery({
    queryKey: ["inbox", universeId, filter],
    queryFn: () =>
      api<InboxRequestListResponse>(
        "GET",
        `/api/v1/universes/${universeId}/inbox/requests${filter === "pending" ? "?status=pending" : ""}`,
      ),
    // Requests arrive while program runs work; keep the list current.
    refetchInterval: 10_000,
  });

  return (
    <>
      <div className="flex h-12 shrink-0 items-center gap-2 border-b px-4">
        <h1 className="text-sm font-semibold">Inbox</h1>
        <Tabs className="ml-auto" value={filter} onValueChange={(value) => onFilter(value as InboxFilter)}>
          <TabsList className="group-data-horizontal/tabs:h-8">
            <TabsTrigger value="pending" className="px-2 text-xs">Pending</TabsTrigger>
            <TabsTrigger value="all" className="px-2 text-xs">All</TabsTrigger>
          </TabsList>
        </Tabs>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto">
        {requests.isLoading && <p className="p-4 text-sm text-muted-foreground">Loading…</p>}
        {requests.error && <ReadError error={requests.error} loading={!requests.data} className="p-4" />}
        {requests.data && requests.data.requests.length === 0 && (
          <ListNote>
            {filter === "pending"
              ? "Nothing is waiting for an answer."
              : "No requests yet. Programs ask questions here with input.request or input.open."}
          </ListNote>
        )}
        <ul>
          {requests.data?.requests.map((request) => (
            <li key={request.requestId}>
              <NavLink
                to={`/u/${slug}/inbox/${request.requestId}`}
                className={cn(
                  "flex flex-col gap-0.5 border-b px-4 py-2.5 text-sm hover:bg-muted/50",
                  request.requestId === activeId && "bg-muted",
                )}
              >
                <span className="flex items-center gap-2">
                  <span className="truncate font-medium">{request.ask}</span>
                  {request.status !== "pending" && (
                    <span className="ml-auto shrink-0 text-xs text-muted-foreground">{STATUS_LABELS[request.status]}</span>
                  )}
                </span>
                <span className="truncate text-xs text-muted-foreground">
                  {request.status === "pending"
                    ? `${deadlineLabel(request.deadlineAtMs)} · ${peopleLabel(request.assignees)}`
                    : peopleLabel(request.assignees)}
                </span>
              </NavLink>
            </li>
          ))}
        </ul>
      </div>
    </>
  );
}

const STATUS_LABELS: Record<InputRequestStatus, string> = {
  pending: "Pending",
  answered: "Answered",
  expired: "Expired",
  cancelled: "Withdrawn",
};

function RequestDetail({
  universeId,
  slug,
  requestId,
}: {
  universeId: string;
  slug: string;
  requestId: string;
}) {
  const request = useQuery({
    queryKey: ["inbox", universeId, "request", requestId],
    queryFn: () =>
      api<InboxRequestResponse>("GET", `/api/v1/universes/${universeId}/inbox/requests/${requestId}`),
  });
  const view = request.data?.request;

  return (
    <>
      <div className="flex h-12 min-w-0 shrink-0 items-center gap-1.5 border-b px-2 md:gap-3 md:px-4">
        <NavLink to={`/u/${slug}/inbox`} className="flex size-8 shrink-0 items-center justify-center md:hidden" aria-label="Back to inbox">
          <ChevronRight className="size-4 rotate-180" />
        </NavLink>
        <h1 className="min-w-0 flex-1 truncate text-sm font-semibold">{view?.ask ?? requestId}</h1>
        {view && (
          <Badge variant={view.status === "pending" ? "default" : "secondary"}>{STATUS_LABELS[view.status]}</Badge>
        )}
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto p-4 md:p-6">
        {request.isLoading && <LoadingNote />}
        {request.error && <ReadError error={request.error} loading={!request.data} />}
        {view && (
          <div className="grid max-w-3xl gap-6">
            <p className="whitespace-pre-wrap text-base">{view.ask}</p>
            <dl className="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
              <Fact label="Asked of">{peopleLabel(view.assignees)}</Fact>
              {view.escalateTo && view.escalateTo.length > 0 && (
                <Fact label={view.escalated ? "Escalated from" : "Escalates to"}>
                  {view.escalated ? "the original assignees" : peopleLabel(view.escalateTo)}
                </Fact>
              )}
              <Fact label="Asked">{new Date(view.createdAtMs).toLocaleString()}</Fact>
              {view.status === "pending" && (
                <Fact label="Deadline">
                  {new Date(view.deadlineAtMs).toLocaleString()} ({deadlineLabel(view.deadlineAtMs)})
                </Fact>
              )}
              {view.reminderCount > 0 && <Fact label="Reminders">{view.reminderCount}</Fact>}
              <Fact label="Waiting run">
                <Link className="underline-offset-4 hover:underline" to={`/u/${slug}/sessions/${view.sessionId}`}>
                  {view.sessionId}
                </Link>{" "}
                <span className="text-muted-foreground">run {view.runId}</span>
              </Fact>
            </dl>
            {view.payload !== undefined && view.payload !== null && (
              <JsonBlock title="Context" value={view.payload} />
            )}
            {view.answer && (
              <section className="grid gap-1 rounded-lg border p-4 text-sm">
                <p className="font-medium">
                  {view.answer.approved ? "Approved" : "Rejected"} by {attributionLabel(view.answer.answeredBy)}
                </p>
                <p className="text-muted-foreground">{new Date(view.answer.answeredAtMs).toLocaleString()}</p>
                {view.answer.comment && <p className="whitespace-pre-wrap">{view.answer.comment}</p>}
                {view.answer.value !== undefined && view.answer.value !== null && (
                  <pre className="overflow-x-auto rounded bg-muted p-3 font-mono text-xs">
                    {JSON.stringify(view.answer.value, null, 2)}
                  </pre>
                )}
              </section>
            )}
            {view.status === "pending" && <AnswerForm universeId={universeId} request={view} />}
          </div>
        )}
      </div>
    </>
  );
}

function AnswerForm({ universeId, request }: { universeId: string; request: InputRequestView }) {
  const queryClient = useQueryClient();
  const canAnswer = useActionPermissions(universeId).can("control_session");
  const schema = request.responseSchema;
  const [valueText, setValueText] = useState("");
  const [comment, setComment] = useState("");
  const [formError, setFormError] = useState<string | null>(null);
  const answer = useMutation({
    mutationFn: (body: { approved: boolean; value?: unknown; comment?: string }) =>
      api<InboxRequestResponse>(
        "POST",
        `/api/v1/universes/${universeId}/inbox/requests/${request.requestId}/answer`,
        body,
      ),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["inbox", universeId] }),
  });

  if (!canAnswer) {
    return <p className="text-sm text-muted-foreground">You can read this request but not answer it.</p>;
  }

  const submit = (approved: boolean) => {
    const value = approved && schema ? parseAnswerValue(valueText) : undefined;
    if (typeof value === "string") {
      setFormError(value);
      return;
    }
    setFormError(null);
    answer.mutate({
      approved,
      ...(value ? { value: value.value } : {}),
      ...(comment.trim() ? { comment } : {}),
    });
  };

  return (
    <section className="grid gap-4 rounded-lg border p-4">
      {schema && (
        <>
          <JsonBlock title="Expected answer" value={schema} />
          <Field>
            <FieldLabel htmlFor="inbox-answer-value">Answer (JSON)</FieldLabel>
            <Textarea
              id="inbox-answer-value"
              className="min-h-24 font-mono text-xs"
              value={valueText}
              onChange={(event) => setValueText(event.target.value)}
            />
            <FieldDescription>An approval carries a value that matches the schema above.</FieldDescription>
          </Field>
        </>
      )}
      <Field>
        <FieldLabel htmlFor="inbox-answer-comment">Comment</FieldLabel>
        <Textarea
          id="inbox-answer-comment"
          value={comment}
          onChange={(event) => setComment(event.target.value)}
        />
      </Field>
      {(formError ?? answer.error) && (
        <p className="text-sm text-destructive">{formError ?? answer.error?.message}</p>
      )}
      <div className="flex gap-2">
        <Button disabled={answer.isPending} onClick={() => submit(true)}>Approve</Button>
        <Button variant="outline" disabled={answer.isPending} onClick={() => submit(false)}>Reject</Button>
      </div>
    </section>
  );
}

function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="min-w-0 break-words">{children}</dd>
    </>
  );
}

function JsonBlock({ title, value }: { title: string; value: unknown }) {
  return (
    <section className="grid gap-1">
      <h2 className="text-sm font-medium">{title}</h2>
      <pre className="max-h-96 overflow-auto rounded bg-muted p-3 font-mono text-xs">
        {JSON.stringify(value, null, 2)}
      </pre>
    </section>
  );
}

/// An approval's value from the form: the parsed JSON, or why it is not.
export function parseAnswerValue(text: string): { value: unknown } | string {
  if (!text.trim()) return "An approval of this request needs an answer.";
  try {
    return { value: JSON.parse(text) as unknown };
  } catch {
    return "The answer is not valid JSON.";
  }
}

/// How long a pending request has left, or how long ago its deadline passed.
export function deadlineLabel(deadlineAtMs: number, nowMs = Date.now()): string {
  const left = deadlineAtMs - nowMs;
  if (left <= 0) return "due now";
  if (left < 3_600_000) return `${Math.max(1, Math.floor(left / 60_000))}m left`;
  if (left < 86_400_000) return `${Math.floor(left / 3_600_000)}h left`;
  return `${Math.floor(left / 86_400_000)}d left`;
}

export function peopleLabel(people: string[]): string {
  return people.length === 0 ? "Anyone" : people.join(", ");
}

export function attributionLabel(attribution: Attribution): string {
  switch (attribution.kind) {
    case "actor":
      return attribution.id;
    case "key":
      return `API key ${attribution.prefix}`;
    case "local":
      return "a local user";
    case "internal":
      return attribution.component;
  }
}
