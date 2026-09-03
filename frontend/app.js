const $ = (id) => document.getElementById(id);
const api = async (url, options = {}) => { const response = await fetch(url, options); const text = await response.text(); let body = {}; try { body = text ? JSON.parse(text) : {}; } catch (_) {} if (!response.ok) throw new Error(body.error || `请求失败 (${response.status})`); return body; };
const toast = (message, error = false) => { const node = $('toast'); node.textContent = message; node.style.background = error ? '#9b4242' : '#13222d'; node.classList.add('show'); setTimeout(() => node.classList.remove('show'), 2600); };
const escapeHtml = (value) => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const formatMs = (ms) => { const total = Math.floor(ms / 1000); return `${String(Math.floor(total / 60)).padStart(2,'0')}:${String(total % 60).padStart(2,'0')}`; };
const formatTime = (value) => { if (!value) return '—'; const date = new Date(value.endsWith('Z') || value.includes('+') ? value : `${value}Z`); return Number.isNaN(date.getTime()) ? value : date.toLocaleString('zh-CN', { hour12: false }); };
const statusLabel = (status) => ({ running: '进行中', ended: '已结束', completed: '已转写', transcribing: '转写中', uploaded: '待转写', failed: '失败' }[status] || status);
const requireMeeting = () => { const id = $('meetingId').value.trim(); if (!id) { toast('请先创建或加载会议', true); return null; } return id; };

/* ---------- 健康状态 ---------- */
async function refreshHealth() { try { const data = await api('/health'); $('health').innerHTML = `<span class="dot"></span><span>服务正常 · ${data.jobs.pending} 个待处理任务</span>`; } catch (_) { $('health').innerHTML = '<span class="dot" style="background:#c25353"></span><span>服务不可用</span>'; } }

/* ---------- 视图路由 ---------- */
let detailMeeting = null;
let detailEvents = null;
function showView(name, tab) {
  ['meetings', 'detail', 'console'].forEach(key => { $(`view-${key}`).hidden = key !== name; });
  document.querySelectorAll('.tabs [data-tab]').forEach(link => link.classList.toggle('active', link.dataset.tab === (tab || name)));
}
function route() {
  const hash = location.hash || '#/meetings';
  const detail = hash.match(/^#\/meetings\/([\w-]+)$/);
  if (detail) { openDetail(detail[1]); return; }
  stopDetailEvents(); detailMeeting = null;
  if (hash === '#/console') { showView('console', 'console'); return; }
  showView('meetings', 'meetings');
  loadMeetingList();
}

/* ---------- 会议列表 ---------- */
async function loadMeetingList() {
  const status = $('meetingStatusFilter').value;
  const node = $('meetingList');
  try {
    const meetings = await api(`/api/v1/meetings?limit=200${status ? `&status=${status}` : ''}`);
    if (!meetings.length) { node.className = 'meeting-list empty'; node.innerHTML = '暂无会议，可在操作台创建会议'; return; }
    node.className = 'meeting-list';
    node.innerHTML = meetings.map(m => `
      <a class="meeting-card" href="#/meetings/${encodeURIComponent(m.id)}">
        <div class="meeting-card-main">
          <h3>${escapeHtml(m.title)}</h3>
          <p class="muted">开始 ${formatTime(m.started_at || m.created_at)}${m.ended_at ? ` · 结束 ${formatTime(m.ended_at)}` : ''}</p>
        </div>
        <div class="meeting-card-meta">
          <span class="pill ${m.status === 'ended' ? 'ended' : 'running'}">${statusLabel(m.status)}</span>
          <span>分段 ${m.transcribed_count}/${m.segment_count}</span>
          <span>说话人 ${m.speaker_count}</span>
          <span>摘要 ${m.summary_count}</span>
          <span>Board v${m.board_version}</span>
        </div>
      </a>`).join('');
  } catch (error) { node.className = 'meeting-list empty'; node.textContent = error.message; }
}

/* ---------- 会议详情 ---------- */
async function openDetail(id) {
  showView('detail', 'meetings');
  try {
    const meeting = await api(`/api/v1/meetings/${id}`);
    detailMeeting = meeting;
    $('detailTitle').textContent = meeting.title;
    $('detailMeta').textContent = `开始 ${formatTime(meeting.started_at)} · 摘要窗口 ${Math.round(meeting.summary_window_ms / 1000)}s · ID ${meeting.id}`;
    $('detailStatus').innerHTML = `<span class="pill ${meeting.status === 'ended' ? 'ended' : 'running'}">${statusLabel(meeting.status)}</span>`;
    $('endDetailMeeting').style.display = meeting.status === 'ended' ? 'none' : '';
    await refreshDetailData(id);
    startDetailEvents(id);
  } catch (error) { toast(error.message, true); location.hash = '#/meetings'; }
}
async function refreshDetailData(id) {
  if (!detailMeeting || detailMeeting.id !== id) return;
  try {
    const [segments, summaries, board] = await Promise.all([
      api(`/api/v1/meetings/${id}/segments`),
      api(`/api/v1/meetings/${id}/summaries`),
      api(`/api/v1/meetings/${id}/board`),
    ]);
    renderTimeline(segments, summaries);
    renderBoardInto($('dBoard'), $('dBoardVersion'), board);
  } catch (error) { toast(error.message, true); }
}
/* 转写时间线与滚动摘要合并为一条时间轴：均按时间倒序（最新在上），
   摘要落在其覆盖窗口结束之后，便于与对应分段对照。 */
function renderTimeline(segments, summaries) {
  const node = $('dSegments');
  $('detailSegmentCount').textContent = `${segments.length} 个分段 · ${summaries.length} 条摘要`;
  if (!segments.length && !summaries.length) { node.className = 'feed empty'; node.innerHTML = '暂无分段，等待音频上传'; return; }
  node.className = 'feed';
  const items = [
    ...segments.map(seg => ({ kind: 'segment', t: seg.end_ms ?? seg.start_ms ?? 0, seg })),
    ...summaries.map(sum => ({ kind: 'summary', t: sum.window_end_ms ?? 0, sum })),
  ].sort((a, b) => (b.t - a.t) || (a.kind === 'summary' ? -1 : 1));
  node.innerHTML = items.map(item => item.kind === 'summary' ? `
    <article class="summary timeline-summary">
      <strong>滚动摘要 · ${formatMs(item.sum.window_start_ms)} — ${formatMs(item.sum.window_end_ms)}</strong>
      <span>${escapeHtml(summaryText(item.sum.content))}</span>
    </article>` : `
    <article class="segment">
      <header>
        <strong class="speaker">${escapeHtml(item.seg.speaker_name || '未知说话人')}</strong>
        <span class="time">${formatMs(item.seg.start_ms)} — ${formatMs(item.seg.end_ms)}</span>
        <span class="pill ${item.seg.status}">${statusLabel(item.seg.status)}</span>
      </header>
      <p class="segment-text">${escapeHtml(item.seg.transcript || (item.seg.status === 'failed' ? '转写失败' : '（等待转写）'))}</p>
      ${item.seg.has_audio && item.seg.audio_url ? `<audio controls preload="none" src="${encodeURI(item.seg.audio_url)}"></audio>` : ''}
    </article>`).join('');
}
function startDetailEvents(id) {
  stopDetailEvents();
  if (typeof EventSource === 'undefined') return;
  detailEvents = new EventSource(`/api/v1/meetings/${id}/events`);
  let timer = null;
  const refresh = () => { clearTimeout(timer); timer = setTimeout(() => refreshDetailData(id), 400); };
  ['segment.uploaded', 'segment.transcribed', 'segment.failed', 'segment.updated', 'summary.created', 'board.updated', 'meeting.ended'].forEach(kind => detailEvents.addEventListener(kind, refresh));
  detailEvents.onerror = () => { stopDetailEvents(); };
}
function stopDetailEvents() { if (detailEvents) { detailEvents.close(); detailEvents = null; } }
$('meetingStatusFilter').addEventListener('change', loadMeetingList);
$('refreshMeetings').addEventListener('click', loadMeetingList);
$('endDetailMeeting').addEventListener('click', async () => {
  if (!detailMeeting) return;
  try { await api(`/api/v1/meetings/${detailMeeting.id}/end`, { method: 'POST' }); toast('会议已结束'); await openDetail(detailMeeting.id); } catch (error) { toast(error.message, true); }
});

/* ---------- 操作台 ---------- */
let currentMeeting = null;
async function loadMeeting() { const id = requireMeeting(); if (!id) return; try { const meeting = await api(`/api/v1/meetings/${id}`); currentMeeting = meeting; $('meetingStatus').textContent = meeting.status === 'ended' ? '已结束' : '进行中'; await Promise.all([loadSpeakers(), refreshData(), loadJobs()]); } catch (error) { toast(error.message, true); } }
async function loadSpeakers() { const id = requireMeeting(); if (!id) return; const speakers = await api(`/api/v1/meetings/${id}/speakers`); $('speakers').className = `list${speakers.length ? '' : ' empty'}`; $('speakers').innerHTML = speakers.length ? speakers.map(s => `<div><span>${escapeHtml(s.name)}</span><code>${s.id.slice(0,8)}</code></div>`).join('') : '还没有说话人'; $('speakerId').innerHTML = '<option value="">未知说话人</option>' + speakers.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join(''); }
async function refreshData() { const id = requireMeeting(); if (!id) return; const [summaries, board] = await Promise.all([api(`/api/v1/meetings/${id}/summaries`), api(`/api/v1/meetings/${id}/board`)]); renderSummariesInto($('summaries'), summaries); renderBoardInto($('board'), $('boardVersion'), board); }
async function loadJobs() { const id = requireMeeting(); if (!id) return; const jobs = await api(`/api/v1/jobs?meeting_id=${encodeURIComponent(id)}`); const node = $('jobs'); node.className = `jobs-table${jobs.length ? '' : ' empty'}`; node.innerHTML = jobs.length ? `<table><thead><tr><th>类型</th><th>状态</th><th>重试</th><th>时间</th></tr></thead><tbody>${jobs.map(j => `<tr><td>${j.job_type}</td><td><span class="pill ${j.status === 'failed' ? 'failed' : ''}">${j.status}</span></td><td>${j.retry_count}</td><td>${j.available_at}</td></tr>`).join('')}</tbody></table>` : '暂无任务'; }
function summaryText(content) { if (content.summary) return content.summary; const parts = []; if ((content.topics || []).length) parts.push(`主题：${content.topics.join('、')}`); if ((content.key_points || []).length) parts.push(`要点：${content.key_points.join('；')}`); if ((content.decisions || []).length) parts.push(`决策：${content.decisions.join('；')}`); if ((content.action_items || []).length) parts.push(`行动项：${content.action_items.map(a => `${a.content}(${a.owner || '未指派'})`).join('；')}`); return parts.join('\n') || '暂无内容'; }
function renderSummariesInto(node, items) { node.className = `feed${items.length ? '' : ' empty'}`; node.innerHTML = items.length ? items.slice().reverse().map(item => `<article class="summary"><strong>${formatMs(item.window_start_ms)} — ${formatMs(item.window_end_ms)}</strong><span>${escapeHtml(summaryText(item.content))}</span></article>`).join('') : '暂无 Summary'; }
function renderBoardInto(node, versionNode, data) { versionNode.textContent = `v${data.version || 0}`; const board = data.content || {}; const groups = [['topics','主题'],['decisions','决策'],['key_points','关键点'],['open_questions','待确认问题'],['risks','风险']]; const parts = groups.filter(([key]) => (board[key] || []).length).map(([key, title]) => `<div class="board-group"><h3>${title}</h3><ul>${board[key].map(v => `<li>${escapeHtml(v)}</li>`).join('')}</ul></div>`); if ((board.action_items || []).length) parts.push(`<div class="board-group"><h3>行动项</h3>${board.action_items.map(v => `<div class="action">${escapeHtml(v.content)}<small>${escapeHtml(v.owner || '未指派')} · ${escapeHtml(v.status || 'open')}</small></div>`).join('')}</div>`); node.className = `board${parts.length ? '' : ' empty'}`; node.innerHTML = parts.join('') || '暂无 Board 内容'; }
$('meetingForm').addEventListener('submit', async (event) => { event.preventDefault(); try { const result = await api('/api/v1/meetings', { method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify({title:$('title').value}) }); $('meetingId').value = result.id; toast('会议已创建'); await loadMeeting(); } catch (error) { toast(error.message, true); } });
$('loadMeeting').addEventListener('click', loadMeeting); $('refresh').addEventListener('click', () => loadMeeting());
$('speakerForm').addEventListener('submit', async (event) => { event.preventDefault(); const id = requireMeeting(); if (!id) return; try { await api(`/api/v1/meetings/${id}/speakers`, { method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify({name:$('speakerName').value}) }); $('speakerName').value = ''; toast('说话人已添加'); await loadSpeakers(); } catch (error) { toast(error.message, true); } });
$('endMeeting').addEventListener('click', async () => { const id = requireMeeting(); if (!id) return; try { await api(`/api/v1/meetings/${id}/end`, {method:'POST'}); toast('会议已结束'); await loadMeeting(); } catch (error) { toast(error.message, true); } });
$('segmentForm').addEventListener('submit', async (event) => { event.preventDefault(); const id = requireMeeting(); if (!id) return; const file = $('audio').files[0]; if (!file) return toast('请选择音频文件', true); const form = new FormData(); [['speaker_id','speakerId'],['sequence_no','sequence'],['start_ms','startMs'],['end_ms','endMs'],['transcript','transcript']].forEach(([key, input]) => { if ($(input).value) form.append(key, $(input).value); }); form.append('audio', file); $('uploadStatus').textContent = '上传中...'; try { await api(`/api/v1/meetings/${id}/segments`, {method:'POST', body:form}); toast('音频分段已上传'); $('uploadStatus').textContent = '已加入处理队列'; $('sequence').value = Number($('sequence').value) + 1; await loadJobs(); } catch (error) { $('uploadStatus').textContent = '上传失败'; toast(error.message, true); } });

/* ---------- 启动 ---------- */
refreshHealth(); setInterval(refreshHealth, 15000);
window.addEventListener('hashchange', route);
route();
