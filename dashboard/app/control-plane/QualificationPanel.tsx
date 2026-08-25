import type { QualificationView } from "@/lib/control-plane";

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

function qualificationPercent(qualification: QualificationView): string {
  if (qualification.required_seconds === null || qualification.required_seconds === 0) {
    return "unavailable";
  }
  return `${((qualification.eligible_seconds / qualification.required_seconds) * 100).toFixed(2)}%`;
}

function certificateAnchor(certificateId: string): string {
  return `certificate-${certificateId.replace(/[^a-zA-Z0-9_-]/g, "-")}`;
}

export function QualificationPanel({
  qualification,
}: {
  qualification: QualificationView;
}) {
  const hasPolicy =
    qualification.policy_version !== null &&
    qualification.release_id !== null &&
    qualification.config_id !== null &&
    qualification.role_identity.length > 0;
  const certificate = qualification.certificate;
  const alarming =
    qualification.state === "invalidated" ||
    qualification.state === "recovering" ||
    !hasPolicy;

  return (
    <section style={alarming ? dangerPanel : panel}>
      <h2 style={{ marginTop: 0 }}>Rolling qualification</h2>
      <p>
        <strong>{qualification.state}</strong> · Eligible {qualification.eligible_seconds}s · Required{" "}
        {qualification.required_seconds ?? "unavailable"}s · {qualificationPercent(qualification)}
      </p>
      <dl>
        <dt>Policy identity</dt>
        <dd>
          {hasPolicy ? (
            <>
              {qualification.policy_version} · {qualification.release_id} · {qualification.config_id} ·{" "}
              {qualification.role_identity.join("/")}
            </>
          ) : (
            <span style={danger}>unavailable; this is not healthy or empty</span>
          )}
        </dd>
        <dt>Last fact</dt>
        <dd>
          {qualification.last_fact_at ?? "unavailable"} · age{" "}
          {qualification.last_fact_age_seconds ?? "unavailable"}s
        </dd>
        <dt>Last breaker</dt>
        <dd>
          {qualification.last_breaker === null
            ? "none"
            : `${qualification.last_breaker.reason} · ${qualification.last_breaker.fact_id} · ${qualification.last_breaker.observed_at}`}
        </dd>
      </dl>
      {certificate === null ? (
        <p style={muted}>No qualification certificate for this epoch.</p>
      ) : (
        <p id={certificateAnchor(certificate.certificate_id)}>
          <a href={`#${certificateAnchor(certificate.certificate_id)}`}>
            {certificate.certificate_id}
          </a>
          <br />
          Digest {certificate.certificate_digest} · evidence {certificate.evidence_digest}
          <br />
          <span style={muted}>
            Qualified {certificate.qualified_at} · created {certificate.created_at}
          </span>
        </p>
      )}
    </section>
  );
}
