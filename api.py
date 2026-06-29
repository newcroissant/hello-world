"""
FastAPI web server exposing the multi-agent harness with a live dashboard.
Run: uvicorn api:app --reload --port 8000
"""
import asyncio
import random
import string
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from harness import MessageBus, AgentRegistry, ResultAggregator, Orchestrator, Task
from harness.types import AgentStatus
from agents import EchoAgent, TransformAgent, FilterAgent

# ---------------------------------------------------------------------------
# Global harness state
# ---------------------------------------------------------------------------

bus        = MessageBus()
registry   = AgentRegistry()
aggregator = ResultAggregator()
orch       = Orchestrator(registry, bus, aggregator, concurrency=20)
_orch_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _orch_task
    for i in range(2):
        registry.register(EchoAgent(f"echo-{i}", bus))
        registry.register(TransformAgent(f"transform-{i}", bus))
    registry.register(FilterAgent("filter-0", bus))
    await registry.start_all()
    _orch_task = asyncio.create_task(orch.run())
    yield
    await orch.shutdown()
    if _orch_task:
        _orch_task.cancel()
    await registry.stop_all()


app = FastAPI(title="Multi-Agent Harness", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ---------------------------------------------------------------------------
# API schemas
# ---------------------------------------------------------------------------

class TaskRequest(BaseModel):
    task_type: str
    payload:   Any
    priority:  int = 0


class TaskResponse(BaseModel):
    task_id:   str
    task_type: str
    priority:  int


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------

@app.post("/tasks", response_model=TaskResponse)
async def submit_task(req: TaskRequest):
    task = Task(task_type=req.task_type, payload=req.payload, priority=req.priority)
    await orch.submit(task)
    return TaskResponse(task_id=task.task_id, task_type=task.task_type, priority=task.priority)


@app.get("/tasks/{task_id}/result")
async def get_result(task_id: str, timeout: float = 5.0):
    try:
        result = await aggregator.wait_for(task_id, timeout=timeout)
        return {
            "task_id":    result.task_id,
            "agent_id":   result.agent_id,
            "status":     result.status.name,
            "output":     result.output,
            "error":      result.error,
            "duration_s": result.duration_s,
        }
    except asyncio.TimeoutError:
        raise HTTPException(status_code=202, detail="Task not yet complete")


@app.get("/results")
async def list_results():
    results = aggregator.results()
    return [
        {
            "task_id":    r.task_id,
            "agent_id":   r.agent_id,
            "status":     r.status.name,
            "output":     r.output,
            "error":      r.error,
            "duration_s": round(r.duration_s, 4),
        }
        for r in reversed(results)
    ]


@app.get("/agents/health")
async def agents_health():
    reports = await registry.health_check_all()
    return [
        {
            "agent_id":    r.agent_id,
            "status":      r.status.name,
            "tasks_done":  r.tasks_done,
            "error_rate":  round(r.error_rate, 3),
            "queue_depth": r.queue_depth,
        }
        for r in reports
    ]


@app.get("/summary")
async def summary():
    return aggregator.summary()


# ---------------------------------------------------------------------------
# Dashboard (single-page HTML)
# ---------------------------------------------------------------------------

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Multi-Agent Harness Dashboard</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }
    header { background: #1e293b; border-bottom: 1px solid #334155; padding: 16px 24px; display: flex; align-items: center; gap: 12px; }
    header h1 { font-size: 1.25rem; font-weight: 600; color: #f8fafc; }
    .badge { background: #22c55e22; color: #22c55e; border: 1px solid #22c55e44; border-radius: 9999px; padding: 2px 10px; font-size: 0.75rem; }
    main { max-width: 1200px; margin: 0 auto; padding: 24px; display: grid; gap: 24px; }
    .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 20px; }
    .card h2 { font-size: 0.875rem; font-weight: 600; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 16px; }
    .stat-row { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 16px; }
    .stat { background: #0f172a; border-radius: 8px; padding: 12px 20px; flex: 1; min-width: 100px; }
    .stat .label { font-size: 0.75rem; color: #64748b; margin-bottom: 4px; }
    .stat .value { font-size: 1.5rem; font-weight: 700; color: #f8fafc; }
    .stat .value.green  { color: #22c55e; }
    .stat .value.red    { color: #ef4444; }
    .stat .value.yellow { color: #f59e0b; }
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    th { text-align: left; color: #64748b; font-weight: 500; padding: 8px 12px; border-bottom: 1px solid #334155; }
    td { padding: 8px 12px; border-bottom: 1px solid #1e293b; color: #cbd5e1; vertical-align: middle; }
    tr:hover td { background: #0f172a44; }
    .pill { display: inline-block; border-radius: 9999px; padding: 2px 10px; font-size: 0.75rem; font-weight: 500; }
    .pill.SUCCEEDED { background: #22c55e22; color: #22c55e; border: 1px solid #22c55e44; }
    .pill.FAILED    { background: #ef444422; color: #ef4444; border: 1px solid #ef444444; }
    .pill.HEALTHY   { background: #22c55e22; color: #22c55e; border: 1px solid #22c55e44; }
    .pill.DEGRADED  { background: #f59e0b22; color: #f59e0b; border: 1px solid #f59e0b44; }
    .pill.STOPPED   { background: #64748b22; color: #94a3b8; border: 1px solid #64748b44; }
    form { display: grid; gap: 12px; }
    .form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    label { font-size: 0.8rem; color: #94a3b8; display: block; margin-bottom: 4px; }
    select, input, textarea { width: 100%; background: #0f172a; border: 1px solid #334155; border-radius: 6px; color: #e2e8f0; padding: 8px 12px; font-size: 0.875rem; font-family: inherit; }
    textarea { font-family: 'Courier New', monospace; resize: vertical; min-height: 80px; }
    select:focus, input:focus, textarea:focus { outline: none; border-color: #6366f1; }
    button { background: #6366f1; color: #fff; border: none; border-radius: 6px; padding: 10px 20px; font-size: 0.875rem; font-weight: 600; cursor: pointer; transition: background 0.15s; }
    button:hover { background: #4f46e5; }
    button:disabled { background: #334155; color: #64748b; cursor: not-allowed; }
    .output { font-family: 'Courier New', monospace; font-size: 0.8rem; background: #0f172a; border-radius: 6px; padding: 12px; color: #a5f3fc; min-height: 48px; word-break: break-all; border: 1px solid #334155; }
    .refresh-hint { font-size: 0.75rem; color: #475569; margin-top: 8px; }
    .empty { color: #475569; font-style: italic; text-align: center; padding: 24px; }
    .truncate { max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  </style>
</head>
<body>
<header>
  <h1>Multi-Agent Harness</h1>
  <span class="badge" id="live-badge">● LIVE</span>
</header>
<main>
  <!-- Summary stats -->
  <div class="card">
    <h2>Summary</h2>
    <div class="stat-row" id="stat-row">
      <div class="stat"><div class="label">Succeeded</div><div class="value green" id="stat-ok">—</div></div>
      <div class="stat"><div class="label">Failed</div><div class="value red" id="stat-fail">—</div></div>
      <div class="stat"><div class="label">Total Tasks</div><div class="value" id="stat-total">—</div></div>
      <div class="stat"><div class="label">Healthy Agents</div><div class="value green" id="stat-agents">—</div></div>
    </div>
    <p class="refresh-hint">Auto-refreshes every 2 seconds</p>
  </div>

  <div class="grid2">
    <!-- Submit task -->
    <div class="card">
      <h2>Submit Task</h2>
      <form onsubmit="submitTask(event)">
        <div class="form-row">
          <div>
            <label>Task Type</label>
            <select id="task-type" onchange="updatePayloadHint()">
              <option value="echo">echo</option>
              <option value="transform">transform</option>
              <option value="filter">filter</option>
            </select>
          </div>
          <div>
            <label>Priority (0–10)</label>
            <input id="priority" type="number" value="0" min="0" max="10"/>
          </div>
        </div>
        <div>
          <label>Payload (JSON)</label>
          <textarea id="payload">{"hello": "world"}</textarea>
        </div>
        <button type="submit" id="submit-btn">Submit</button>
      </form>
      <div style="margin-top:16px">
        <label>Result</label>
        <div class="output" id="task-output">—</div>
      </div>
    </div>

    <!-- Agent health -->
    <div class="card">
      <h2>Agent Health</h2>
      <table>
        <thead><tr><th>Agent</th><th>Status</th><th>Done</th><th>Errors</th></tr></thead>
        <tbody id="agent-tbody"><tr><td colspan="4" class="empty">Loading…</td></tr></tbody>
      </table>
    </div>
  </div>

  <!-- Results log -->
  <div class="card">
    <h2>Task Results (most recent first)</h2>
    <table>
      <thead><tr><th>Task ID</th><th>Agent</th><th>Status</th><th>Output</th><th>Duration</th></tr></thead>
      <tbody id="results-tbody"><tr><td colspan="5" class="empty">No results yet</td></tr></tbody>
    </table>
  </div>
</main>

<script>
const PAYLOAD_HINTS = {
  echo:      '{"hello": "world"}',
  transform: '{"text": "hello world", "operation": "upper"}',
  filter:    '{"items": ["cat", "elephant", "ox"], "min_length": 3}',
};

function updatePayloadHint() {
  const type = document.getElementById('task-type').value;
  document.getElementById('payload').value = PAYLOAD_HINTS[type] || '{}';
}

async function submitTask(e) {
  e.preventDefault();
  const btn = document.getElementById('submit-btn');
  btn.disabled = true;
  btn.textContent = 'Submitting…';
  const out = document.getElementById('task-output');

  let payload;
  try { payload = JSON.parse(document.getElementById('payload').value); }
  catch { out.textContent = 'Invalid JSON payload'; btn.disabled = false; btn.textContent = 'Submit'; return; }

  try {
    const r = await fetch('/tasks', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        task_type: document.getElementById('task-type').value,
        payload,
        priority: parseInt(document.getElementById('priority').value) || 0,
      }),
    });
    const data = await r.json();
    out.textContent = `Submitted: ${data.task_id}\\nWaiting for result…`;

    // Poll for result
    const result = await fetch(`/tasks/${data.task_id}/result?timeout=8`);
    const res    = await result.json();
    out.textContent = JSON.stringify(res, null, 2);
  } catch (err) {
    out.textContent = 'Error: ' + err.message;
  }
  btn.disabled = false;
  btn.textContent = 'Submit';
}

async function refreshDashboard() {
  try {
    const [summary, health, results] = await Promise.all([
      fetch('/summary').then(r => r.json()),
      fetch('/agents/health').then(r => r.json()),
      fetch('/results').then(r => r.json()),
    ]);

    const ok    = summary.SUCCEEDED || 0;
    const fail  = summary.FAILED    || 0;
    const total = Object.values(summary).reduce((a, b) => a + b, 0);
    document.getElementById('stat-ok').textContent    = ok;
    document.getElementById('stat-fail').textContent  = fail;
    document.getElementById('stat-total').textContent = total;
    document.getElementById('stat-agents').textContent =
      health.filter(a => a.status === 'HEALTHY').length + ' / ' + health.length;

    // Agent table
    const atbody = document.getElementById('agent-tbody');
    atbody.innerHTML = health.map(a => `
      <tr>
        <td>${a.agent_id}</td>
        <td><span class="pill ${a.status}">${a.status}</span></td>
        <td>${a.tasks_done}</td>
        <td>${(a.error_rate * 100).toFixed(0)}%</td>
      </tr>`).join('');

    // Results table
    const rtbody = document.getElementById('results-tbody');
    if (!results.length) {
      rtbody.innerHTML = '<tr><td colspan="5" class="empty">No results yet</td></tr>';
    } else {
      rtbody.innerHTML = results.slice(0, 50).map(r => `
        <tr>
          <td><code style="font-size:0.75rem;color:#94a3b8">${r.task_id.slice(0,8)}…</code></td>
          <td>${r.agent_id}</td>
          <td><span class="pill ${r.status}">${r.status}</span></td>
          <td class="truncate" title="${JSON.stringify(r.output)}">${JSON.stringify(r.output)}</td>
          <td>${r.duration_s}s</td>
        </tr>`).join('');
    }
  } catch { /* server may be restarting */ }
}

refreshDashboard();
setInterval(refreshDashboard, 2000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML
