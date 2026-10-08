const express = require('express');
const Database = require('better-sqlite3');
const path = require('path');
const { Readable } = require('stream');

const PORT = process.env.PORT || 3000;
const IP_HOST= '0.0.0.0';
const OLLAMA_URL = (process.env.OLLAMA_URL || 'http://127.0.0.1:11434').replace(/\/$/, '');
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'chats.db');

// ---------- SQLite ----------
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');
db.exec(`
  CREATE TABLE IF NOT EXISTS chats (
    id      TEXT PRIMARY KEY,
    title   TEXT NOT NULL,
    model   TEXT,
    created INTEGER NOT NULL,
    updated INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS messages (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id  TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    role     TEXT NOT NULL,
    content  TEXT NOT NULL DEFAULT '',
    thinking TEXT NOT NULL DEFAULT '',
    model    TEXT,
    stats    TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id);
`);

const q = {
  list:    db.prepare('SELECT id, title, model, created FROM chats ORDER BY updated DESC'),
  get:     db.prepare('SELECT id, title, model, created FROM chats WHERE id = ?'),
  msgs:    db.prepare('SELECT role, content, thinking, model, stats FROM messages WHERE chat_id = ? ORDER BY id'),
  upsert:  db.prepare(`INSERT INTO chats (id, title, model, created, updated)
                       VALUES (@id, @title, @model, @created, @updated)
                       ON CONFLICT(id) DO UPDATE SET title = excluded.title, model = excluded.model, updated = excluded.updated`),
  delMsgs: db.prepare('DELETE FROM messages WHERE chat_id = ?'),
  insMsg:  db.prepare('INSERT INTO messages (chat_id, role, content, thinking, model, stats) VALUES (?, ?, ?, ?, ?, ?)'),
  delChat: db.prepare('DELETE FROM chats WHERE id = ?'),
};

// Replace a chat and all its messages atomically
const saveChat = db.transaction((id, c) => {
  const now = Date.now();
  q.upsert.run({ id, title: String(c.title || 'New Chat'), model: c.model || null, created: c.created || now, updated: now });
  q.delMsgs.run(id);
  for (const m of c.messages || []) {
    if (m.role !== 'user' && m.role !== 'assistant') continue;
    q.insMsg.run(id, m.role, String(m.content || ''), String(m.thinking || ''), m.model || null, m.stats ? JSON.stringify(m.stats) : null);
  }
});

// ---------- App ----------
const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/chats', (req, res) => res.json(q.list.all()));

app.get('/api/chats/:id', (req, res) => {
  const chat = q.get.get(req.params.id);
  if (!chat) return res.status(404).json({ error: 'Chat not found' });
  chat.messages = q.msgs.all(req.params.id).map(m => ({ ...m, stats: m.stats ? JSON.parse(m.stats) : undefined }));
  res.json(chat);
});

app.put('/api/chats/:id', (req, res) => {
  try { saveChat(req.params.id, req.body || {}); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/chats/:id', (req, res) => {
  q.delChat.run(req.params.id);
  res.json({ ok: true });
});

// Proxy to Ollama (streams through, so the browser needs no CORS setup)
app.use('/ollama', async (req, res) => {
  const ac = new AbortController();
  res.on('close', () => ac.abort()); // browser hit Stop or closed the tab
  try {
    const up = await fetch(OLLAMA_URL + req.url, {
      method: req.method,
      headers: { 'Content-Type': 'application/json' },
      body: ['GET', 'HEAD'].includes(req.method) ? undefined : JSON.stringify(req.body),
      signal: ac.signal,
    });
    res.status(up.status);
    res.set('Content-Type', up.headers.get('content-type') || 'application/json');
    if (!up.body) return res.end();
    Readable.fromWeb(up.body).on('error', () => res.destroy()).pipe(res);
  } catch (e) {
    if (!res.headersSent) res.status(502).json({ error: 'Cannot reach Ollama at ' + OLLAMA_URL + ': ' + e.message });
    else res.end();
  }
});

app.listen(PORT, IP_HOST, () => {
  console.log(`Ollama Chat running at http://${IP_HOST}:${PORT}`);
  console.log(`Ollama: ${OLLAMA_URL}  |  Database: ${DB_PATH}`);
});