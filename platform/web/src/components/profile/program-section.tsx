import { useEffect, useState } from "react";
import type { ProgramContractRequirement, ProgramLimitsInput, ProgramSpec } from "@lightspeed-ai/sdk";
import type { ProfileDocument } from "@/api";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";

/// The program a new program profile starts with: it returns its input.
export const STARTER_PROGRAM = `export default {
  async run(input, env, ctx) {
    // env holds the contracts listed under Required contracts, such as
    // await env.time.sleep({ ms: 1000 }) with time.sleep required.
    return { received: input };
  },
};
`;

/// A new profile whose sessions run a program instead of a model.
export function programProfileDocument(profileId: string, displayName: string): ProfileDocument {
  return {
    profileId,
    revision: 0,
    ...(displayName ? { displayName } : {}),
    program: {
      source: { type: "text", text: STARTER_PROGRAM },
      semanticsVersion: 1,
      inputSchema: { type: "object" },
      requires: [],
    } satisfies ProgramSpec,
  };
}

export function profileProgram(document: ProfileDocument): ProgramSpec | null {
  const program = document.program;
  return program && typeof program === "object" ? (program as ProgramSpec) : null;
}

const LIMITS: { key: keyof ProgramLimitsInput; label: string; placeholder: string; hint: string }[] = [
  { key: "maxCalls", label: "Max calls", placeholder: "50000", hint: "Tool calls a run may make." },
  { key: "maxWorkflowCalls", label: "Max workflow calls", placeholder: "1000", hint: "Calls a run may make to each workflow tool (child agents and programs, questions, approvals); at most 10,000." },
  { key: "maxInFlight", label: "Max in flight", placeholder: "64", hint: "Calls a run may have outstanding at once." },
  { key: "maxCompute", label: "Max compute", placeholder: "250000", hint: "Work a run's program may do, in interrupt checks." },
  { key: "inputDeadlineMs", label: "Input deadline (ms)", placeholder: "deployment default", hint: "How long a question waits when the call sets no deadline." },
  { key: "contextRetained", label: "Contexts kept", placeholder: "10", hint: "Runs whose program context stays in the session's active context." },
];

/// One contract per line: its name, then optionally the digest to pin.
function requiresText(requires: ProgramContractRequirement[] | undefined): string {
  return (requires ?? [])
    .map((requirement) => (requirement.digest ? `${requirement.contract} ${requirement.digest}` : requirement.contract))
    .join("\n");
}

function parseRequires(text: string): ProgramContractRequirement[] {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [contract, digest] = line.split(/\s+/, 2);
      return digest ? { contract: contract!, digest } : { contract: contract! };
    });
}

function schemaText(schema: ProgramSpec["inputSchema"]): string {
  return schema === undefined ? "" : JSON.stringify(schema, null, 2);
}

/// Text that is empty (no schema) or a JSON object.
function parseSchema(text: string): { schema?: Record<string, unknown>; error?: string } {
  if (!text.trim()) return {};
  try {
    const value: unknown = JSON.parse(text);
    if (!value || typeof value !== "object" || Array.isArray(value)) return { error: "a schema is a JSON object" };
    return { schema: value as Record<string, unknown> };
  } catch (error) {
    return { error: error instanceof Error ? error.message : "not JSON" };
  }
}

const textareaClass =
  "w-full resize-y rounded-lg border border-input bg-transparent p-3 font-mono text-xs outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 dark:bg-input/30";

/// A program profile's program: its source, the contracts it requires, its
/// input and output schemas, and its per-run limits. Schema text that is
/// not a JSON object is reported through `onValidityChange` and not applied.
export function ProgramSection({
  program,
  onChange,
  readOnly,
  onValidityChange,
}: {
  program: ProgramSpec;
  onChange: (program: ProgramSpec) => void;
  readOnly: boolean;
  onValidityChange: (error: string | null) => void;
}) {
  const [requires, setRequires] = useState(() => requiresText(program.requires));
  const [inputSchema, setInputSchema] = useState(() => schemaText(program.inputSchema));
  const [outputSchema, setOutputSchema] = useState(() => schemaText(program.outputSchema));
  const inputParsed = parseSchema(inputSchema);
  const outputParsed = parseSchema(outputSchema);
  const error = inputParsed.error
    ? `input schema: ${inputParsed.error}`
    : outputParsed.error ? `output schema: ${outputParsed.error}` : null;
  useEffect(() => onValidityChange(error), [error, onValidityChange]);

  const change = (fn: (next: ProgramSpec) => void) => {
    const next = structuredClone(program);
    fn(next);
    onChange(next);
  };
  const setSchema = (key: "inputSchema" | "outputSchema", text: string) => {
    (key === "inputSchema" ? setInputSchema : setOutputSchema)(text);
    const parsed = parseSchema(text);
    if (parsed.error) return;
    change((next) => {
      if (parsed.schema) next[key] = parsed.schema;
      else delete next[key];
    });
  };
  const limits = program.limits ?? {};

  return (
    <div className="grid gap-4">
      <Field>
        <FieldLabel htmlFor="program-source">Source</FieldLabel>
        {program.source.type === "text" ? (
          <textarea
            id="program-source"
            className={`${textareaClass} min-h-64`}
            readOnly={readOnly}
            value={program.source.text}
            onChange={(event) => change((next) => { next.source = { type: "text", text: event.target.value }; })}
            spellCheck={false}
          />
        ) : (
          <p className="text-sm text-muted-foreground">
            Stored as a blob reference (<code className="font-mono text-xs">{program.source.blobRef}</code>)
            — edit via the JSON tab.
          </p>
        )}
        <FieldDescription>
          A module whose default export has <code className="font-mono">run(input, env, ctx)</code>; what it
          returns is the run's output.
        </FieldDescription>
      </Field>
      <Field>
        <FieldLabel htmlFor="program-requires">Required contracts</FieldLabel>
        <textarea
          id="program-requires"
          className={`${textareaClass} min-h-20`}
          readOnly={readOnly}
          value={requires}
          placeholder={"time.sleep\nweb.fetch\nagent.run"}
          onChange={(event) => {
            setRequires(event.target.value);
            change((next) => { next.requires = parseRequires(event.target.value); });
          }}
          spellCheck={false}
        />
        <FieldDescription>
          One per line, optionally followed by the digest to pin. Only these reach the program's{" "}
          <code className="font-mono">env</code>; the session's configuration must grant each.
        </FieldDescription>
      </Field>
      <div className="grid gap-4 sm:grid-cols-2">
        {(["inputSchema", "outputSchema"] as const).map((key) => {
          const text = key === "inputSchema" ? inputSchema : outputSchema;
          const parsed = key === "inputSchema" ? inputParsed : outputParsed;
          return (
            <Field key={key}>
              <FieldLabel htmlFor={`program-${key}`}>{key === "inputSchema" ? "Input schema" : "Output schema"}</FieldLabel>
              <textarea
                id={`program-${key}`}
                className={`${textareaClass} min-h-32`}
                readOnly={readOnly}
                value={text}
                placeholder='{ "type": "object" }'
                onChange={(event) => setSchema(key, event.target.value)}
                spellCheck={false}
              />
              <FieldDescription className={parsed.error ? "text-destructive" : undefined}>
                {parsed.error
                  ? parsed.error
                  : key === "inputSchema" ? "JSON Schema each run's input must satisfy; empty accepts any JSON." : "JSON Schema each run's output must satisfy; empty accepts any JSON."}
              </FieldDescription>
            </Field>
          );
        })}
      </div>
      <div className="grid gap-4 sm:grid-cols-3">
        {LIMITS.map((limit) => (
          <Field key={limit.key}>
            <FieldLabel htmlFor={`program-limit-${limit.key}`}>{limit.label}</FieldLabel>
            <Input
              id={`program-limit-${limit.key}`}
              type="number"
              min={1}
              readOnly={readOnly}
              value={limits[limit.key] ?? ""}
              placeholder={limit.placeholder}
              onChange={(event) => change((next) => {
                const value = Number(event.target.value);
                const updated: ProgramLimitsInput = { ...(next.limits ?? {}) };
                if (event.target.value && Number.isFinite(value) && value > 0) updated[limit.key] = value;
                else delete updated[limit.key];
                if (Object.keys(updated).length) next.limits = updated;
                else delete next.limits;
              })}
            />
            <FieldDescription>{limit.hint}</FieldDescription>
          </Field>
        ))}
      </div>
    </div>
  );
}
