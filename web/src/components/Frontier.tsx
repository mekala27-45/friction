import type { Desk } from "@/lib/site";

// The frontier: friction per 1,000 good events across, dollar capture up, one line per model with its
// interval band, the floor hatched in control gray, the registered budget as a rule and the chosen
// operating point as a ring with its numbers. Dashes add texture to hue; every line is labeled.
const W = 640;
const H = 360;
const M = { l: 48, r: 132, t: 16, b: 40 };
const DASH: Record<string, string> = { floor: "2 3", logistic: "6 3", lightgbm: "", freeman: "1 0", lightgbm_rep: "8 2 2 2" };

function pct(y: number) {
  return `${Math.round(y * 100)}%`;
}

export function Frontier({ desk, title, unit }: { desk: Desk; title: string; unit: string }) {
  const xmax = Math.max(desk.budget * 3, ...desk.models.map((m) => m.op.x * 1.2));
  const sx = (x: number) => M.l + (Math.min(x, xmax) / xmax) * (W - M.l - M.r);
  const sy = (y: number) => H - M.b - y * (H - M.t - M.b);
  const ticks = [0, 0.25, 0.5, 0.75, 1];
  const xt = [0, xmax / 3, (2 * xmax) / 3, xmax];
  return (
    <figure className="my-6" data-testid="frontier">
      <figcaption className="mb-2 font-medium">{title}</figcaption>
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" role="img" aria-label={title}>
        <defs>
          <pattern id="hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <line x1="0" y1="0" x2="0" y2="6" stroke="var(--control)" strokeWidth="1.5" opacity="0.5" />
          </pattern>
        </defs>
        {ticks.map((t) => (
          <g key={t}>
            <line x1={M.l} x2={W - M.r} y1={sy(t)} y2={sy(t)} stroke="var(--hairline)" />
            <text x={M.l - 6} y={sy(t) + 4} textAnchor="end" fontSize="11" fill="var(--ink2)">{pct(t)}</text>
          </g>
        ))}
        {xt.map((t) => (
          <text key={t} x={sx(t)} y={H - M.b + 16} textAnchor="middle" fontSize="11" fill="var(--ink2)">
            {t.toFixed(t < 10 ? 1 : 0)}
          </text>
        ))}
        <text x={(M.l + W - M.r) / 2} y={H - 6} textAnchor="middle" fontSize="11" fill="var(--ink2)">{unit}</text>
        <line x1={sx(desk.budget)} x2={sx(desk.budget)} y1={M.t} y2={H - M.b} stroke="var(--ink)" strokeDasharray="4 4" />
        <text x={sx(desk.budget) + 4} y={M.t + 10} fontSize="11" fill="var(--ink)">budget</text>
        {desk.models.map((m) => {
          const pts = m.points.filter((p) => p.x <= xmax);
          if (!pts.length) return null;
          const band = [...pts.map((p) => `${sx(p.x)},${sy(p.hi)}`), ...pts.slice().reverse().map((p) => `${sx(p.x)},${sy(p.lo)}`)].join(" ");
          const line = pts.map((p) => `${sx(p.x)},${sy(p.y)}`).join(" ");
          const last = pts[pts.length - 1]!;
          const color = `var(--${m.color})`;
          return (
            <g key={m.model} data-series={m.model}>
              <polygon points={band} fill={m.model === "floor" ? "url(#hatch)" : color} opacity={m.model === "floor" ? 1 : 0.15} />
              <polyline points={line} fill="none" stroke={color} strokeWidth="2" strokeDasharray={DASH[m.model] ?? ""} />
              <text x={sx(last.x) + 6} y={sy(last.y) + 4} fontSize="11" fill={color}>{m.name}</text>
              {m.model === desk.champion || m.model === "floor" ? (
                <g>
                  <circle cx={sx(m.op.x)} cy={sy(m.op.y)} r="6" fill="none" stroke={color} strokeWidth="2" />
                  <text x={sx(m.op.x) + 9} y={sy(m.op.y) - 8} fontSize="11" fill={color}>
                    {pct(m.op.y)} ({pct(m.op.lo)} to {pct(m.op.hi)})
                  </text>
                </g>
              ) : null}
            </g>
          );
        })}
      </svg>
    </figure>
  );
}
