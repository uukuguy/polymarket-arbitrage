"""Read-only operator projection for L2's public health contract."""

# ruff: noqa: E501

from __future__ import annotations

from starlette.requests import Request
from starlette.responses import HTMLResponse


def l2_console(_request: Request) -> HTMLResponse:
    """Serve a same-origin operational view without granting control authority."""
    return HTMLResponse(
        """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>L2 operations console</title>
<style>body{background:#0b0d10;color:#e6edf3;font:15px system-ui,sans-serif;margin:0}main{max-width:1100px;margin:auto;padding:24px}.row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}.card{border:1px solid #30363d;border-radius:8px;padding:16px;margin:12px 0;background:#11161c}.fail{border-color:#f85149}.warn{border-color:#d29922}.muted{color:#9aa4b2}.error{color:#ff7b72}button,a{background:#21262d;color:#58a6ff;border:1px solid #30363d;border-radius:6px;padding:7px 10px;text-decoration:none;cursor:pointer}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#090c10;padding:10px;border-radius:5px}</style>
</head><body><main><div class="row"><div><h1>L2 operations console</h1><div class="muted">Read-only, same-origin view of the live L2 health contract. A reachable page is not an all-clear.</div></div><button id="refresh">Refresh now</button><a href="/health">Health JSON</a><a href="/healthz">Probe JSON</a></div><p id="status" class="muted">Loading live L2 evidence…</p><section id="faults"></section><script>
const root=document.getElementById("faults"),status=document.getElementById("status");
const actions={
 "ws:connection_state":"No manual recovery is required while event-age and L3 evidence checks pass. Investigate only if websocket freshness, mirror freshness, or L3 evidence is no longer passing.",
 "ws:last_event_age_seconds":"Inspect websocket reachability and the quiet-refresh evidence path; if retries remain exhausted, restart only through the approved L2 recovery runbook.",
 "mirror:l2_tob_age_seconds":"Inspect Supabase mirror writes and credentials; verify the next successful L2 mirror receipt before clearing the incident.",
 "l3:evidence_sample_age_seconds":"Inspect the evidence refresh cause (for example evidence_timeout) and runtime-event queue capacity; do not accept reconnect logs as recovery evidence.",
 "l3:promoter_ledger_age_seconds":"Inspect candidate ingestion and the durable L3 promoter ledger; recovery requires a new committed five-market receipt.",
 "l3:membership_convergence":"Inspect candidate desired/committed membership and websocket subscriptions; recovery requires 5 markets / 10 tokens convergence.",
 "l3:worst_market_freshness":"Wait for a complete persisted five-market freshness sample; investigate any missing token before treating L3 as usable.",
 "event_bus:connection_state":"Inspect the Supabase LISTEN connection and reconciliation fallback; a reconnecting listener is not healthy until a reconciliation succeeds.",
 "candidates:supabase_fetch_age_seconds":"Inspect candidate-feed credentials and fetch errors; recovery requires a successful current candidate fetch."
};
function card(check){const item=document.createElement("article"),value=check.observedValue??"not recorded";item.className="card "+check.status;const title=document.createElement("h2");title.textContent=`${check.status.toUpperCase()} · ${check.name}`;item.append(title);for(const [label,value] of [["Impact",check.output||"live check is not passing"],["Observed value",String(value)],["Automatic recovery",check.status==="fail"?"L2 continues bounded reconnect / reconciliation attempts; evidence is retained for inspection.":"No destructive action; continues normal bounded recovery."],["Next operator action",actions[check.name]||"Inspect this health check's raw evidence and L2 logs; verify a new successful receipt before closing."]]){const row=document.createElement("div"),key=document.createElement("strong");key.textContent=label+": ";row.append(key,document.createTextNode(value));item.append(row)}const proof=document.createElement("pre");proof.textContent="Raw health evidence\\n"+JSON.stringify(check,null,2);item.append(proof);return item}
async function refresh(){status.className="muted";status.textContent="Loading live L2 evidence…";root.replaceChildren();try{const response=await fetch("/health",{cache:"no-store"});const body=await response.json();const checks=Object.entries(body.checks||{}).flatMap(([name,rows])=>(rows||[]).map(row=>({...row,name}))).filter(check=>check.status==="fail"||check.status==="warn");status.className=response.ok&&body.status!=="fail"?"muted":"error";status.textContent=`${body.status||"unknown"} · ${checks.length} warning/failure check(s) · release ${body.releaseId||"unknown"}`;if(!checks.length){root.textContent="No warning or failure checks returned. Validate the raw health contract above before declaring recovery.";return}checks.forEach(check=>root.append(card(check)))}catch(error){status.className="error";status.textContent=`L2 health read unavailable: ${error.message}. This is a visibility fault, not an all-clear.`}}
document.getElementById("refresh").onclick=refresh;refresh();
</script></main></body></html>"""
    )
