// One decision as a slip: the verdict in its slot colour and word, the amount, the score in Doto,
// the reason codes. Approve wears slot one, step up slot three, decline slot two.
const COLOR: Record<string, string> = { approve: "var(--good)", step_up: "var(--friction)", decline: "var(--fraud)" };
const WORD: Record<string, string> = { approve: "approve", step_up: "step up", decline: "decline" };

export function Slip({ verdict, amount, score, codes, label, decision }: {
  verdict: string; amount?: string; score: number; codes: string[]; label: string; decision?: string;
}) {
  // The raw verdict, score and decision id ride on the slip so a separate client can read the
  // decision back from the API and compare it with what the browser printed (scripts/live_check.py).
  return (
    <article className="slip w-full p-3" data-testid="slip" data-verdict={verdict} data-score={score}
      data-decision={decision} aria-label={`${label}: ${WORD[verdict] ?? verdict}`}>
      <div className="flex items-baseline justify-between gap-2">
        <span className="font-semibold uppercase tracking-wide" style={{ color: COLOR[verdict] ?? "var(--ink)" }}>
          {WORD[verdict] ?? verdict}
        </span>
        <span className="text-xs" style={{ color: "var(--ink2)" }}>{label}</span>
      </div>
      <div className="perf my-2" />
      <div className="flex items-baseline justify-between">
        <span className="num text-sm">{amount ?? ""}</span>
        <span className="doto text-2xl">{score.toFixed(4)}</span>
      </div>
      <div className="mt-1 text-xs num" style={{ textAlign: "left" }}>{codes.length ? codes.join(" ") : "no codes"}</div>
    </article>
  );
}
