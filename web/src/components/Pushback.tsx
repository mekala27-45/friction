export function Pushback({ who, children }: { who: string; children: React.ReactNode }) {
  return (
    <aside className="perf measure mt-8 pt-4 text-sm">
      <h2 className="mb-1 font-semibold">What {who} would push back on</h2>
      <div style={{ color: "var(--ink2)" }}>{children}</div>
    </aside>
  );
}
