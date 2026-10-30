"use client";

import { useState } from "react";
import { Slip } from "@/components/Slip";
import { API, site, type Expected } from "@/lib/site";

type Printed = Expected & { label: string; amount?: string; live: boolean; decision?: string };
const PER_RUN = 20;

async function probe(): Promise<boolean> {
  if (!API) return false;
  for (let i = 0; i < 2; i++) {
    try {
      const r = await fetch(`${API}/health`, { cache: "no-store" });
      if (r.ok) return true;
    } catch {
      // asleep or unreachable; probe once more
    }
    await new Promise((res) => setTimeout(res, 1500));
  }
  return false;
}

export function Replay() {
  const desks = Object.keys(site.replay);
  const [desk, setDesk] = useState(desks[0] ?? "payments");
  const [mode, setMode] = useState<"idle" | "live" | "recorded">("idle");
  const [slips, setSlips] = useState<Printed[]>([]);
  const set = site.replay[desk];
  if (!set) return <p>No replay set has been exported yet.</p>;

  async function run() {
    if (!set) return;
    setSlips([]);
    const live = await probe();
    setMode(live ? "live" : "recorded");
    const events = set.events.slice(0, PER_RUN);
    for (let i = 0; i < events.length; i++) {
      const e = events[i]!;
      const exp = set.recorded[i]!;
      const amount = typeof e.amount_cents === "number" ? `$${(e.amount_cents / 100).toFixed(2)}` : undefined;
      let got: Printed = { ...exp, label: `${desk} ${exp.event_ref}`, amount, live: false };
      if (live) {
        try {
          const r = await fetch(`${API}/v1/${desk}/score`, {
            method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(e),
          });
          if (r.ok) {
            const j = await r.json();
            got = { event_ref: exp.event_ref, score: j.score, verdict: j.verdict, codes: j.codes, label: got.label, amount,
              live: true, decision: j.decision_id };
          }
        } catch {
          // keep the recorded slip for this event
        }
      }
      setSlips((s) => [got, ...s]);
    }
  }

  return (
    <div>
      <div className="no-print flex flex-wrap items-center gap-3">
        <label className="text-sm">Desk{" "}
          <select value={desk} onChange={(e) => setDesk(e.target.value)} className="slip px-2 py-1">
            {desks.map((d) => <option key={d} value={d}>{d}</option>)}
          </select>
        </label>
        <button type="button" onClick={run} className="slip px-3 py-1 font-semibold">Send {PER_RUN} events in order</button>
        <span className="text-sm" data-testid="replay-mode">
          {mode === "live" ? "live API" : mode === "recorded" ? "recorded session: the API is asleep or not deployed" : ""}
        </span>
      </div>
      <div className="mt-4 grid gap-2 sm:grid-cols-2" aria-live="polite">
        {slips.map((s) => (
          <Slip key={s.event_ref} verdict={s.verdict} score={s.score} codes={s.codes} amount={s.amount}
            label={`${s.label}${s.live ? "" : " (recorded)"}`} decision={s.decision} />
        ))}
      </div>
    </div>
  );
}
