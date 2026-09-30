import ts from "typescript";

export type CompiledSource =
  | { ok: true; code: string }
  | { ok: false; message: string };

const WRAPPER_PREFIX = "async function __main(input) {\n";

/**
 * Compile a script body to the JavaScript the sandbox runs. Types are
 * stripped, never checked: tool schemas are enforced at each call instead.
 * Hosts call this at admission so a malformed script never starts.
 */
export function compileSource(body: string): CompiledSource {
  const wrapped = `${WRAPPER_PREFIX}${body}\n}`;
  const output = ts.transpileModule(wrapped, {
    fileName: "script.ts",
    reportDiagnostics: true,
    compilerOptions: {
      target: ts.ScriptTarget.ES2020,
      module: ts.ModuleKind.ESNext,
      removeComments: false,
    },
  });
  const diagnostic = output.diagnostics?.find(
    (candidate) => candidate.category === ts.DiagnosticCategory.Error,
  );
  if (diagnostic) return { ok: false, message: describe(diagnostic) };

  // The body must stay inside the wrapper: a stray closing brace would turn
  // the rest of the script into top-level code that runs before `input` exists.
  const parsed = ts.createSourceFile("script.ts", wrapped, ts.ScriptTarget.ES2020);
  const [only] = parsed.statements;
  if (parsed.statements.length !== 1 || !only || !ts.isFunctionDeclaration(only)) {
    return { ok: false, message: "the script must be a single async function body" };
  }
  return { ok: true, code: output.outputText };
}

function describe(diagnostic: ts.Diagnostic): string {
  const text = ts.flattenDiagnosticMessageText(diagnostic.messageText, " ");
  if (!diagnostic.file || diagnostic.start === undefined) return text;
  const { line, character } = diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start);
  // `line` is zero-based in the wrapped text, whose first line is the wrapper,
  // so it already equals the one-based line within the body.
  return `line ${Math.max(line, 1)}, column ${character + 1}: ${text}`;
}
