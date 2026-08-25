import type { DataProductPointer, RuntimeControllerView } from "@/lib/control-plane";

const panel = {
  padding: 16,
  border: "1px solid #4b5563",
  background: "#111",
  borderRadius: 8,
  marginBottom: 16,
} as const;

const dangerPanel = {
  ...panel,
  borderColor: "#ef4444",
  background: "#341414",
} as const;

const muted = { color: "#aaa" } as const;
const danger = { color: "#fecaca" } as const;

function PointerFreshness({
  label,
  sourceKey,
  pointer,
}: {
  label: string;
  sourceKey: string;
  pointer: DataProductPointer | null;
}) {
  if (pointer === null) {
    return (
      <li>
        <strong>{label}:</strong>{" "}
        <span style={danger}>
          {sourceKey} unavailable; this is not healthy or empty.
        </span>
      </li>
    );
  }
  return (
    <li>
      <strong>{label}:</strong> <code>{sourceKey}</code> {pointer.published_at}
      <br />
      <span style={muted}>
        Generation {pointer.generation_key} · {pointer.record_count.toLocaleString()} records ·{" "}
        {pointer.artifact_key}
      </span>
    </li>
  );
}

export function RuntimeOverview({
  controller,
  quotePointer,
  structureManifest,
  jobCounts,
}: {
  controller: RuntimeControllerView;
  quotePointer: DataProductPointer | null;
  structureManifest: DataProductPointer | null;
  jobCounts: Record<string, number>;
}) {
  const controllerUnavailable = controller.status === "unavailable";
  return (
    <section style={controllerUnavailable || controller.status === "critical" ? dangerPanel : panel}>
      <h2 style={{ marginTop: 0 }}>Runtime overview</h2>
      <h3>Controller state</h3>
      {controllerUnavailable ? (
        <p style={danger}>
          m1-runtime-reconciler is unavailable ({controller.reason}); this is not healthy or empty.
        </p>
      ) : (
        <dl>
          <dt>Status</dt>
          <dd>
            <strong>{controller.status}</strong>
          </dd>
          <dt>Epoch</dt>
          <dd>{controller.epoch}</dd>
          <dt>Tick</dt>
          <dd>{controller.last_tick_at}</dd>
          <dt>Lease</dt>
          <dd>
            owner {controller.owner_id} · expires {controller.lease_expires_at} · age{" "}
            {controller.lease_age_seconds}s · overdue {controller.lease_overdue_seconds}s
          </dd>
        </dl>
      )}
      <h3>Data-product freshness</h3>
      <ul>
        <PointerFreshness
          label="Quote current pointer"
          pointer={quotePointer}
          sourceKey="quote.current_pointer.published_at"
        />
        <PointerFreshness
          label="Structure latest manifest"
          pointer={structureManifest}
          sourceKey="structure.latest_manifest.published_at"
        />
      </ul>
      <p style={muted}>
        Durable jobs:{" "}
        {Object.entries(jobCounts)
          .map(([state, count]) => `${state}=${count}`)
          .join(" · ") || "none"}
      </p>
    </section>
  );
}
