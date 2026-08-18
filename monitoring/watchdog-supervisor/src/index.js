const STATE_KEY = "alert-machine-state";
const SOURCE_DEFAULT = "cloudflare-watchdog-supervisor";
const FAILURE_CODE = /^[a-z0-9:/._-]{1,256}$/;

function machineUrl(env) {
  return `https://api.machines.dev/v1/apps/${encodeURIComponent(env.FLY_ALERT_APP)}/machines/${encodeURIComponent(env.FLY_ALERT_MACHINE_ID)}`;
}

function failure(code) {
  return { healthy: false, failures: [code], restartCount: null };
}

export async function observeAlertMachine(env, fetchImpl = fetch) {
  let response;
  try {
    response = await fetchImpl(machineUrl(env), {
      headers: { Authorization: `Bearer ${env.FLY_MACHINE_READ_TOKEN}` },
    });
  } catch {
    return failure("watchdog-supervisor:fly-unavailable");
  }
  if (!response.ok) return failure(`watchdog-supervisor:fly-http-${response.status}`);
  let machine;
  try {
    machine = await response.json();
  } catch {
    return failure("watchdog-supervisor:fly-response-invalid");
  }
  if (!machine || machine.id !== env.FLY_ALERT_MACHINE_ID || !Array.isArray(machine.events)) {
    return failure("watchdog-supervisor:fly-response-invalid");
  }
  const restartCount = machine.events.filter((event) => event?.type === "start" && event?.status === "started").length;
  if (machine.state !== "started") {
    const state = typeof machine.state === "string" && FAILURE_CODE.test(machine.state) ? machine.state : "invalid";
    return { healthy: false, failures: [`machine:${env.FLY_ALERT_APP}/${machine.id}:${state}`], restartCount };
  }
  return { healthy: true, failures: [], restartCount };
}

export async function transitionKey(source, kind, scheduledTime, failures) {
  const content = JSON.stringify({ source, kind, scheduledTime: new Date(scheduledTime).toISOString(), failures: [...failures].sort() });
  const bytes = new TextEncoder().encode(content);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((value) => value.toString(16).padStart(2, "0")).join("");
}

async function notify(env, kind, failures, scheduledTime, fetchImpl) {
  const source = env.RUNTIME_EVENT_SOURCE || SOURCE_DEFAULT;
  const key = await transitionKey(source, kind, scheduledTime, failures);
  const occurredAt = new Date(scheduledTime).toISOString();
  const writer = fetchImpl(`${env.RUNTIME_EVENT_WRITER_URL}/runtime-events`, {
    method: "POST",
    headers: { Authorization: `Bearer ${env.RUNTIME_EVENT_WRITER_TOKEN}`, "Content-Type": "application/json", "Idempotency-Key": key },
    body: JSON.stringify({ kind, failures, source, occurred_at: occurredAt }),
  });
  const telegram = fetchImpl(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text: `[M1][${kind}] ${source}: ${failures.join(", ")} (${occurredAt})` }),
  });
  const results = await Promise.allSettled([writer, telegram]);
  if (results.some((result) => result.status === "rejected" || !result.value.ok)) {
    throw new Error("watchdog-supervisor notification delivery failed");
  }
  return key;
}

function parseState(raw) {
  if (raw === null) return null;
  try {
    const state = JSON.parse(raw);
    return typeof state?.healthy === "boolean" ? state : null;
  } catch {
    return null;
  }
}

export async function runScheduled(controller, env, { fetchImpl = fetch } = {}) {
  let previous;
  try {
    previous = parseState(await env.WATCHDOG_STATE.get(STATE_KEY));
  } catch {
    const failures = ["watchdog-supervisor:kv-unavailable"];
    const key = await notify(env, "detected", failures, controller.scheduledTime, fetchImpl);
    return { healthy: false, kind: "detected", failures, key };
  }
  let observation = await observeAlertMachine(env, fetchImpl);
  if (observation.healthy && Number.isInteger(previous?.restartCount) && observation.restartCount > previous.restartCount) {
    observation = {
      healthy: false,
      failures: [`machine:${env.FLY_ALERT_APP}/${env.FLY_ALERT_MACHINE_ID}:restart-count:${previous.restartCount}-${observation.restartCount}`],
      restartCount: observation.restartCount,
    };
  }
  const kind = !observation.healthy && previous?.healthy !== false ? "detected"
    : observation.healthy && previous?.healthy === false ? "recovered" : null;
  let key;
  if (kind) key = await notify(env, kind, observation.failures, controller.scheduledTime, fetchImpl);
  try {
    await env.WATCHDOG_STATE.put(STATE_KEY, JSON.stringify({
      healthy: observation.healthy,
      restartCount: observation.restartCount,
      failures: observation.failures,
      observedAt: new Date(controller.scheduledTime).toISOString(),
    }));
  } catch {
    if (!kind) {
      const failures = ["watchdog-supervisor:kv-unavailable"];
      key = await notify(env, "detected", failures, controller.scheduledTime, fetchImpl);
      return { healthy: false, kind: "detected", failures, key };
    }
    throw new Error("watchdog-supervisor state persistence failed after notification");
  }
  return { ...observation, kind, key };
}

export default {
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(runScheduled(controller, env).catch((error) => {
      console.error("watchdog-supervisor scheduled run failed", error);
      throw error;
    }));
  },
  fetch() {
    return new Response("Not Found", { status: 404 });
  },
};
