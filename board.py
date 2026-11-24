#!/usr/bin/env python3
"""Read-only board UI for the shared actor-OS schema.

Renders actor_tasks as a kanban (rows = channels, columns = state),
plus the actor registry and recent channel messages. Because all three
frameworks (romancy/edda/shikibu) share one schema, a single board
observes a mixed-language swarm.

Usage:
    python board.py --db /path/to/app.db [--port 8321]

SQLite only (stdlib: sqlite3 + http.server). PostgreSQL/MySQL could be
added via SQLAlchemy if needed.
"""

import argparse
import json
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>actor board</title>
<style>
body{font:13px -apple-system,monospace;background:#0d1117;color:#c9d1d9;margin:0;padding:16px}
h1{font-size:15px;margin:0 0 12px}h2{font-size:12px;color:#8b949e;margin:18px 0 6px}
table{border-collapse:collapse;width:100%}
th{color:#8b949e;text-align:left;padding:4px 8px;font-weight:normal}
td{vertical-align:top;padding:4px 8px;border-top:1px solid #21262d}
.card{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:6px 8px;margin:4px 0;max-width:220px}
.id{color:#58a6ff;font-weight:bold}.meta{color:#8b949e;font-size:11px}
.data{color:#a5d6ff;white-space:pre-wrap;word-break:break-all;font-size:11px;margin-top:3px}
.open{border-left:3px solid #8b949e}.claimed{border-left:3px solid #d29922}
.done{border-left:3px solid #3fb950}.failed{border-left:3px solid #f85149}
.chan{color:#f0883e}.empty{color:#484f58}.ts{color:#8b949e;font-size:11px}
</style></head><body>
<h1>actor board <span class="ts" id="stamp"></span></h1>
<div id="actors"></div>
<div id="board"></div>
<div id="msgs"></div>
<script>
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const card=t=>`<div class="card ${t.state}"><span class="id">${esc(t.id)}</span>
 <span class="meta">${esc(t.claimed_by||'')}</span>
 <div class="data">${esc((t.data||'').slice(0,200))}${t.result?' ⇒ '+esc(String(t.result).slice(0,100)):''}</div>
 <div class="ts">${esc(t.updated_at||t.created_at||'')}</div></div>`;
async function tick(){
 const b=await (await fetch('/api/board')).json();
 document.getElementById('stamp').textContent=new Date().toLocaleTimeString();
 document.getElementById('actors').innerHTML=b.actors.length?
  '<h2>actors</h2>'+b.actors.map(a=>`<div>${esc(a.name)} → ${esc(a.instance_id)}</div>`).join(''):'';
 let h='';
 for(const ch of Object.keys(b.channels).sort()){
  h+=`<h2 class="chan">${esc(ch)}</h2><table><tr>`;
  for(const st of ['open','claimed','done','failed']){
   const ts=b.channels[ch].filter(t=>t.state===st);
   h+=`<td style="width:25%"><b class="${st}">${st} (${ts.length})</b>${
     ts.map(card).join('')||'<div class="empty">—</div>'}</td>`;
  }
  h+='</tr></table>';
 }
 document.getElementById('board').innerHTML=h||'<div class="empty">no tasks</div>';
 document.getElementById('msgs').innerHTML=b.messages.length?
  '<h2>recent channel messages</h2>'+b.messages.map(m=>
   `<div><span class="ts">${esc(m.published_at)}</span> <span class="chan">${esc(m.channel)}</span>
    <span class="data">${esc((m.data||'').slice(0,160))}</span></div>`).join(''):'';
}
tick();setInterval(tick,2000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    db_path = ""

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def do_GET(self):  # noqa: N802
        if self.path == "/":
            body = PAGE.encode()
            self._send(200, body, "text/html; charset=utf-8")
        elif self.path == "/api/board":
            self._send(200, json.dumps(self._board()).encode(), "application/json")
        else:
            self._send(404, b"not found", "text/plain")

    def _board(self):
        with self._conn() as conn:

            def rows(q):
                try:
                    return [dict(r) for r in conn.execute(q)]
                except sqlite3.OperationalError:
                    return []

            tasks = rows(
                "SELECT id, channel, data, state, claimed_by, result, "
                "created_at, updated_at FROM actor_tasks ORDER BY created_at"
            )
            channels = {}
            for t in tasks:
                channels.setdefault(t["channel"], []).append(t)
            return {
                "channels": channels,
                "actors": rows(
                    "SELECT name, instance_id FROM actor_names ORDER BY name"
                ),
                "messages": rows(
                    "SELECT channel, message_id, data, published_at "
                    "FROM channel_messages ORDER BY id DESC LIMIT 30"
                ),
            }

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True, help="SQLite database file")
    ap.add_argument("--port", type=int, default=8321)
    args = ap.parse_args()
    Handler.db_path = args.db
    print(f"board: http://localhost:{args.port}  (db={args.db})", flush=True)
    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
