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
h1{font-size:15px;margin:0 0 8px}h2{font-size:12px;color:#8b949e;margin:18px 0 6px}
table{border-collapse:collapse;width:100%}
th{color:#8b949e;text-align:left;padding:4px 8px;font-weight:normal}
td{vertical-align:top;padding:4px 8px;border-top:1px solid #21262d}
.card{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:6px 8px;margin:4px 0;max-width:220px}
.id{color:#58a6ff;font-weight:bold}.meta{color:#8b949e;font-size:11px}
.data{color:#a5d6ff;white-space:pre-wrap;word-break:break-all;font-size:11px;margin-top:3px}
.open{border-left:3px solid #8b949e}.claimed{border-left:3px solid #d29922}
.done{border-left:3px solid #3fb950}.failed{border-left:3px solid #f85149}
.chan{color:#f0883e}.empty{color:#484f58}.ts{color:#8b949e;font-size:11px}
.pill{display:inline-block;border:1px solid #30363d;border-radius:10px;padding:1px 10px;margin-right:8px;font-size:12px}
.pill.open{color:#8b949e}.pill.claimed{color:#d29922}.pill.done{color:#3fb950}.pill.failed{color:#f85149}
.pill.failed.hot{border-color:#f85149;background:#f8514922;animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 0 0 #f8514966}50%{box-shadow:0 0 0 4px #f8514900}}
@keyframes flash{0%{background:#1f6feb55}100%{background:#161b22}}
.card.flash{animation:flash 1.2s ease-out}
.age{border-radius:8px;padding:0 6px;font-size:10px;margin-left:4px}
.age.ok{background:#30363d;color:#8b949e}.age.warn{background:#d2992233;color:#d29922}
.age.stale{background:#f8514933;color:#f85149;font-weight:bold}
.ev{padding:2px 0;font-size:12px}.ev .icon{display:inline-block;width:16px;text-align:center}
.ev.posted .icon{color:#8b949e}.ev.claimed .icon{color:#d29922}.ev.done .icon{color:#3fb950}
.ev.failed .icon{color:#f85149}.ev.other .icon{color:#484f58}
</style></head><body>
<h1>actor board <span class="ts" id="stamp"></span></h1>
<div id="summary"></div>
<div id="actors"></div>
<div id="board"></div>
<div id="msgs"></div>
<script>
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const prev=new Map();   // task id -> serialized task (diff detection)
const changed=new Set();
const ageMin=u=>{const t=new Date((u||'').replace(' ','T')+'Z');return isNaN(t)?null:(Date.now()-t)/6e4};
const fmtAge=m=>m<1?'now':m<60?Math.floor(m)+'m':Math.floor(m/60)+'h'+Math.floor(m%60)+'m';
const ageCls=m=>m<5?'ok':m<15?'warn':'stale';
const card=t=>{
 const hot=changed.has(t.id)?' flash':'';
 let badge='';
 if(t.state==='claimed'){const a=ageMin(t.updated_at);
  if(a!==null)badge=` <span class="age ${ageCls(a)}">${fmtAge(a)}</span>`;}
 return `<div class="card ${t.state}${hot}"><span class="id">${esc(t.id)}</span>${badge}
 <span class="meta">${esc(t.claimed_by||'')}</span>
 <div class="data">${esc((t.data||'').slice(0,200))}${t.result?' ⇒ '+esc(String(t.result).slice(0,100)):''}</div>
 <div class="ts">${esc(t.updated_at||t.created_at||'')}</div></div>`};
const evLine=m=>{
 const time=(m.published_at||'').slice(11,19);
 let d={};try{d=JSON.parse(m.data||'{}')}catch(e){}
 let icon='·',cls='other',txt='';
 if(d.type==='task_posted'){icon='+';cls='posted';txt=`${esc(d.task_id)} posted to ${esc(d.channel||'')}`}
 else if(d.type==='task_claimed'){icon='→';cls='claimed';txt=`${esc(d.task_id)} claimed by ${esc(d.claimed_by||'')}`}
 else if(d.type==='task_state'){const ok=d.state==='done';icon=ok?'✓':'✗';cls=d.state||'failed';
  txt=`${esc(d.task_id)} ${esc(d.state||'')}`}
 else{txt=`<span class="chan">${esc(m.channel)}</span> ${esc((m.data||'').slice(0,120))}`}
 return `<div class="ev ${cls}"><span class="ts">${esc(time)}</span>
  <span class="icon">${icon}</span> ${txt}</div>`};
async function tick(){
 const b=await (await fetch('/api/board')).json();
 document.getElementById('stamp').textContent=new Date().toLocaleTimeString();
 // diff detection: mark tasks that are new or changed since last poll
 changed.clear();
 for(const ch of Object.values(b.channels))for(const t of ch){
  const s=JSON.stringify(t);
  if(prev.get(t.id)!==s){if(prev.size)changed.add(t.id);prev.set(t.id,s)}
 }
 const counts={open:0,claimed:0,done:0,failed:0};
 for(const ts of Object.values(b.channels))for(const t of ts)counts[t.state]=(counts[t.state]||0)+1;
 document.getElementById('summary').innerHTML=Object.entries(counts)
  .map(([s,n])=>`<span class="pill ${s}${s==='failed'&&n>0?' hot':''}">${s} ${n}</span>`).join('');
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
  '<h2>event stream</h2>'+b.messages.map(evLine).join(''):'';
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
