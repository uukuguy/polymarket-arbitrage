import type { AlertDeliveryBacklog, DatabaseCapacity } from "@/lib/control-plane";

const panel = {
  padding: 16,
  border: "1px solid #4b5563",
  background: "#111",
  borderRadius: 8,
  marginBottom: 16,
} as const;

function statusColor(state: Exclude<DatabaseCapacity["state"], "unavailable">): string {
  if (state === "exhausted") return "#fecaca";
  if (state === "critical") return "#fde68a";
  if (state === "warning") return "#fde68a";
  return "#bbf7d0";
}

export function RecoveryReadiness({
  capacity,
  alertDelivery,
}: {
  capacity: DatabaseCapacity;
  alertDelivery: AlertDeliveryBacklog;
}) {
  if (capacity.state === "unavailable") {
    return (
      <section style={{ ...panel, borderColor: "#ef4444", background: "#341414" }}>
        <h2 style={{ marginTop: 0 }}>Recovery readiness</h2>
        <h3>Database capacity</h3>
        <p style={{ color: "#fecaca" }}>
          Capacity measurement is unavailable ({capacity.reason_code}); this is not healthy or empty.
        </p>
        <h3>Alert delivery</h3>
        <p>Pending: <strong>{alertDelivery.pending_count.toLocaleString()}</strong></p>
      </section>
    );
  }
  const unhealthy = capacity.state === "critical" || capacity.state === "exhausted";
  const pending = alertDelivery.pending_count > 0;
  const latestReceipt = alertDelivery.latest_delivery_state === "failed"
    && alertDelivery.latest_delivery_channel === "telegram"
    && alertDelivery.latest_delivery_error_class === "TelegramCredentialError"
    ? "Telegram credential rejected; Telegram delivery is isolated until reconfigured. Dashboard delivery remains available."
    : null;
  return (
    <section
      style={{
        ...panel,
        borderColor: unhealthy || pending ? "#ef4444" : "#4b5563",
        background: unhealthy || pending ? "#341414" : "#111",
      }}
    >
      <h2 style={{ marginTop: 0 }}>Recovery readiness</h2>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 16 }}>
        <div>
          <h3>Database capacity</h3>
          <p style={{ color: statusColor(capacity.state) }}>
            <strong>{capacity.state}</strong> · {capacity.used_percent}%
          </p>
          <p>
            {capacity.used_bytes.toLocaleString()} / {capacity.budget_bytes.toLocaleString()} bytes
          </p>
          <p style={{ color: "#aaa" }}>Reason: {capacity.reason_code}</p>
          <h3>Top relations</h3>
          {capacity.largest_relations.length === 0 ? (
            <p style={{ color: "#aaa" }}>No relation-size rows returned.</p>
          ) : (
            <ol style={{ margin: 0, paddingLeft: 20, fontSize: 13 }}>
              {capacity.largest_relations.map((relation) => (
                <li key={relation.relation}>
                  <code>{relation.relation}</code> · {relation.used_bytes.toLocaleString()} bytes
                </li>
              ))}
            </ol>
          )}
        </div>
        <div>
          <h3>Alert delivery</h3>
          <p>
            Pending: <strong>{alertDelivery.pending_count.toLocaleString()}</strong>
          </p>
          <p>
            Oldest pending: {alertDelivery.oldest_pending_age_seconds === null
              ? "none"
              : `${Math.floor(alertDelivery.oldest_pending_age_seconds)}s`}
          </p>
          <p style={{ color: "#aaa" }}>
            Latest receipt: {alertDelivery.latest_delivery_state ?? "none"}
            {alertDelivery.latest_delivery_channel ? ` / ${alertDelivery.latest_delivery_channel}` : ""}
            {alertDelivery.latest_delivery_at ? ` · ${alertDelivery.latest_delivery_at}` : " · not observed"}
          </p>
          {latestReceipt && <p style={{ color: "#fde68a" }}>{latestReceipt}</p>}
        </div>
      </div>
      {(unhealthy || pending) && <p style={{ color: "#fecaca", marginBottom: 0 }}>Recovery is blocked; this is not healthy or empty.</p>}
    </section>
  );
}
