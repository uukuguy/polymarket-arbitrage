import assert from "node:assert/strict";
import test from "node:test";

import { observeAlertMachine, runScheduled, transitionKey } from "../src/index.js";

const env = {
  FLY_ALERT_APP: "polyarb-control-alert",
  FLY_ALERT_MACHINE_ID: "machine-1",
  FLY_MACHINE_READ_TOKEN: "fly-token",
  RUNTIME_EVENT_WRITER_URL: "https://writer.example",
  RUNTIME_EVENT_WRITER_TOKEN: "writer-token",
  RUNTIME_EVENT_SOURCE: "cloudflare-watchdog-supervisor",
  TELEGRAM_BOT_TOKEN: "telegram-token",
  TELEGRAM_CHAT_ID: "chat-id",
};

class Kv {
  value = null;
  async get() { return this.value; }
  async put(_key, value) { this.value = value; }
}

function response(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

function flyMachine({ state = "started", startEvents = 1 } = {}) {
  return {
    id: "machine-1",
    state,
    events: Array.from({ length: startEvents }, () => ({ type: "start", status: "started" })),
  };
}

test("observeAlertMachine accepts only the exact started machine", async () => {
  const observation = await observeAlertMachine(env, () => Promise.resolve(response(flyMachine())));
  assert.deepEqual(observation, { healthy: true, failures: [], restartCount: 1 });

  const stopped = await observeAlertMachine(env, () => Promise.resolve(response(flyMachine({ state: "stopped" }))));
  assert.deepEqual(stopped, { healthy: false, failures: ["machine:polyarb-control-alert/machine-1:stopped"], restartCount: 1 });

  const wrongMachine = await observeAlertMachine(env, () => Promise.resolve(response({ ...flyMachine(), id: "other" })));
  assert.deepEqual(wrongMachine, { healthy: false, failures: ["watchdog-supervisor:fly-response-invalid"], restartCount: null });
});

test("transitionKey is deterministic and bounded", async () => {
  const first = await transitionKey("cloudflare-watchdog-supervisor", "detected", 1_787_000_000_000, ["machine:alert:stopped"]);
  const second = await transitionKey("cloudflare-watchdog-supervisor", "detected", 1_787_000_000_000, ["machine:alert:stopped"]);
  assert.equal(first, second);
  assert.match(first, /^[0-9a-f]{64}$/);
});

test("scheduled supervisor notifies only unhealthy and recovery transitions", async () => {
  const state = new Kv();
  const sent = [];
  const fetchImpl = async (url, options = {}) => {
    if (url.startsWith("https://api.machines.dev/")) return response(flyMachine());
    sent.push({ url, options });
    return response({ ok: true }, 201);
  };
  const first = await runScheduled({ scheduledTime: 1_787_000_000_000 }, { ...env, WATCHDOG_STATE: state }, { fetchImpl });
  assert.equal(first.healthy, true);
  assert.equal(sent.length, 0);

  const stoppedFetch = async (url, options = {}) => {
    if (url.startsWith("https://api.machines.dev/")) return response(flyMachine({ state: "stopped" }));
    sent.push({ url, options });
    return response({ ok: true }, 201);
  };
  const stopped = await runScheduled({ scheduledTime: 1_787_000_060_000 }, { ...env, WATCHDOG_STATE: state }, { fetchImpl: stoppedFetch });
  assert.equal(stopped.kind, "detected");
  assert.equal(sent.length, 2);
  assert.equal(JSON.parse(sent[0].options.body).source, "cloudflare-watchdog-supervisor");

  await runScheduled({ scheduledTime: 1_787_000_120_000 }, { ...env, WATCHDOG_STATE: state }, { fetchImpl: stoppedFetch });
  assert.equal(sent.length, 2);

  const recovered = await runScheduled({ scheduledTime: 1_787_000_180_000 }, { ...env, WATCHDOG_STATE: state }, { fetchImpl });
  assert.equal(recovered.kind, "recovered");
  assert.equal(sent.length, 4);
});

test("a newly observed extra start is reported as a restart", async () => {
  const state = new Kv();
  await runScheduled({ scheduledTime: 1_787_000_000_000 }, { ...env, WATCHDOG_STATE: state }, {
    fetchImpl: () => Promise.resolve(response(flyMachine({ startEvents: 1 }))),
  });
  const sent = [];
  const result = await runScheduled({ scheduledTime: 1_787_000_060_000 }, { ...env, WATCHDOG_STATE: state }, {
    fetchImpl: async (url, options = {}) => {
      if (url.startsWith("https://api.machines.dev/")) return response(flyMachine({ startEvents: 2 }));
      sent.push({ url, options });
      return response({ ok: true }, 201);
    },
  });
  assert.equal(result.kind, "detected");
  assert.match(result.failures[0], /restart-count:1-2$/);
  assert.equal(sent.length, 2);
});
