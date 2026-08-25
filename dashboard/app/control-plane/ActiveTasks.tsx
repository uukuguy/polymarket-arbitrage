import type { ActiveTask } from "@/lib/control-plane";

const panel = {
  padding: 16,
  border: "1px solid #4b5563",
  background: "#111",
  borderRadius: 8,
  marginBottom: 16,
} as const;

const muted = { color: "#aaa" } as const;
const danger = { color: "#fecaca" } as const;
const cell = { padding: "6px 8px", verticalAlign: "top" } as const;

function progressText(task: ActiveTask): string {
  return task.progress.total === null
    ? `${task.progress.current}/unknown`
    : `${task.progress.current}/${task.progress.total}`;
}

function overdueText(task: ActiveTask): string {
  return `lease ${task.lease_overdue_seconds}s / attempt ${task.attempt_overdue_seconds}s`;
}

export function ActiveTasks({
  tasks,
  total,
}: {
  tasks: ActiveTask[];
  total: number;
}) {
  return (
    <section style={panel}>
      <h2 style={{ marginTop: 0 }}>Active tasks</h2>
      {tasks.length === 0 ? (
        <p style={muted}>No active task rows returned by the bounded read model.</p>
      ) : (
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #333" }}>
              <th style={cell}>Job</th>
              <th style={cell}>Stage</th>
              <th style={cell}>Recovery state</th>
              <th style={cell}>Progress</th>
              <th style={cell}>Ages</th>
              <th style={cell}>Deadlines</th>
              <th style={cell}>Overdue</th>
            </tr>
          </thead>
          <tbody>
            {tasks.map((task) => (
              <tr key={`${task.job_key}:${task.attempt_id}`} style={{ borderBottom: "1px solid #222" }}>
                <td style={cell}>
                  <strong>{task.job_type}</strong>
                  <br />
                  <code>{task.job_key}</code>
                  <br />
                  <span style={muted}>worker {task.worker_id} · lease epoch {task.lease_epoch}</span>
                </td>
                <td style={cell}>{task.stage}</td>
                <td style={cell}>{task.recovery_state}</td>
                <td style={cell}>{progressText(task)}</td>
                <td style={cell}>
                  Heartbeat age {task.heartbeat_age_seconds}s
                  <br />
                  Progress age {task.progress_age_seconds}s
                </td>
                <td style={cell}>
                  Lease deadline {task.lease_deadline_at}
                  <br />
                  Heartbeat deadline {task.heartbeat_deadline_at}
                  <br />
                  Progress deadline {task.progress_deadline_at}
                  <br />
                  Attempt deadline {task.attempt_deadline_at}
                </td>
                <td style={task.lease_overdue_seconds || task.attempt_overdue_seconds ? { ...cell, ...danger } : cell}>
                  {overdueText(task)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <p style={muted}>Showing {tasks.length} of {total} active tasks.</p>
    </section>
  );
}
