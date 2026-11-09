"use strict";

// Everything is rendered with textContent: job data comes from producers and is never
// interpreted as HTML.
const STORAGE_KEY = "conveyor.apiKey";
const $ = (id) => document.getElementById(id);
let timer;

function getKey() {
  try {
    return sessionStorage.getItem(STORAGE_KEY);
  } catch {
    return null;
  }
}

function setKey(value) {
  try {
    if (value) sessionStorage.setItem(STORAGE_KEY, value);
    else sessionStorage.removeItem(STORAGE_KEY);
  } catch {
    /* storage unavailable: the key lives only in memory for this page */
  }
}

let memoryKey = getKey();

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { Authorization: `Bearer ${memoryKey}`, "Content-Type": "application/json" },
    credentials: "omit",
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw Object.assign(new Error(body.error || `HTTP ${response.status}`), { status: response.status });
  return body;
}

function cell(text) {
  const td = document.createElement("td");
  td.textContent = text;
  return td;
}

function age(seconds) {
  if (!seconds) return "—";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
}

async function refresh() {
  try {
    const [{ queues }, { jobs }] = await Promise.all([api("/v1/stats"), api("/v1/jobs?status=dead&limit=50")]);

    const tbody = $("queues");
    tbody.replaceChildren(
      ...queues.map((q) => {
        const tr = document.createElement("tr");
        const c = q.counts;
        tr.append(cell(q.queue), cell(c.queued ?? 0), cell(c.running ?? 0), cell(c.succeeded ?? 0), cell(c.dead ?? 0), cell(c.cancelled ?? 0), cell(age(q.oldest_ready_seconds)));
        return tr;
      }),
    );
    $("empty").hidden = queues.length > 0;

    $("dead").replaceChildren(
      ...jobs.map((job) => {
        const tr = document.createElement("tr");
        const action = document.createElement("td");
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = "Retry";
        button.addEventListener("click", async () => {
          button.disabled = true;
          try {
            await api(`/v1/jobs/${job.id}/retry`, { method: "POST" });
            refresh();
          } catch (error) {
            button.disabled = false;
            $("status").textContent = error.message;
          }
        });
        action.append(button);
        tr.append(cell(job.id), cell(job.queue), cell(job.kind), cell(`${job.attempts}/${job.max_attempts}`), cell(job.last_error ?? ""), action);
        return tr;
      }),
    );
    $("status").textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (error) {
    if (error.status === 401 || error.status === 403) return signOut(error.message);
    $("status").textContent = `Error: ${error.message}`;
  }
}

function showApp() {
  $("login").hidden = true;
  $("app").hidden = false;
  $("signout").hidden = false;
  refresh();
  clearInterval(timer);
  timer = setInterval(refresh, 5000);
}

function signOut(message = "") {
  clearInterval(timer);
  memoryKey = null;
  setKey(null);
  $("app").hidden = true;
  $("signout").hidden = true;
  $("login").hidden = false;
  $("login-error").textContent = message;
  $("status").textContent = "";
}

$("login").addEventListener("submit", (event) => {
  event.preventDefault();
  memoryKey = $("key").value.trim();
  $("key").value = "";
  setKey(memoryKey);
  showApp();
});
$("signout").addEventListener("click", () => signOut());

if (memoryKey) showApp();
