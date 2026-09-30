const $ = (id) => document.getElementById(id);
const api = async (url, options = {}) => { const response = await fetch(url, options); const text = await response.text(); let body = {}; try { body = text ? JSON.parse(text) : {}; } catch (_) {} if (!response.ok) throw new Error(body.error || `请求失败 (${response.status})`); return body; };
const toast = (message, error = false) => { const node = $('toast'); node.textContent = message; node.style.background = error ? '#9b4242' : '#13222d'; node.classList.add('show'); setTimeout(() => node.classList.remove('show'), 2600); };
const escapeHtml = (value) => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const formatMs = (ms) => { const total = Math.floor(ms / 1000); return `${String(Math.floor(total / 60)).padStart(2,'0')}:${String(total % 60).padStart(2,'0')}`; };
const formatTime = (value) => { if (!value) return '—'; const date = new Date(value.endsWith('Z') || value.includes('+') ? value : `${value}Z`); return Number.isNaN(date.getTime()) ? value : date.toLocaleString('zh-CN', { hour12: false }); };
const statusLabel = (status) => ({ running: '进行中', ending: '正在收尾', partial: '识别中…', ended: '已结束', completed: '已转写', transcribing: '转写中', uploaded: '待转写', failed: '失败' }[status] || status);
const requireMeeting = () => { const id = $('meetingId').value.trim(); if (!id) { toast('请先创建或加载会议', true); return null; } return id; };

/* ---------- 健康状态 ---------- */
async function refreshHealth() { try { const data = await api('/health'); $('health').innerHTML = `<span class="dot"></span><span>服务正常 · ${data.jobs.pending} 个待处理任务</span>`; } catch (_) { $('health').innerHTML = '<span class="dot" style="background:#c25353"></span><span>服务不可用</span>'; } }

/* ---------- 视图路由 ---------- */
let detailMeeting = null;
let detailEvents = null;
let detailEpoch = 0;
let detailRequest = 0;
function showView(name, tab) {
  ['meetings', 'detail', 'console'].forEach(key => { $(`view-${key}`).hidden = key !== name; });
  document.querySelectorAll('.tabs [data-tab]').forEach(link => link.classList.toggle('active', link.dataset.tab === (tab || name)));
}
function route() {
  const hash = location.hash || '#/meetings';
  const detail = hash.match(/^#\/meetings\/([\w-]+)$/);
  if (detail) { openDetail(detail[1]); return; }
  stopDetailEvents(); ++detailEpoch; detailMeeting = null;
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
/* 时间线分页：每页条数，第 1 页为最新（时间线按时间倒序，最新在上）。 */
const DETAIL_PAGE_SIZE = 50;
let detailPage = 1;
let detailTimeline = null;
async function openDetail(id) {
  stopDetailEvents();
  const epoch = ++detailEpoch;
  showView('detail', 'meetings');
  detailPage = 1;
  detailTimeline = null;
  try {
    const meeting = await api(`/api/v1/meetings/${id}`);
    if (epoch !== detailEpoch) return;
    detailMeeting = meeting;
    $('detailTitle').textContent = meeting.title;
    $('detailMeta').textContent = `开始 ${formatTime(meeting.started_at)} · 摘要窗口 ${Math.round(meeting.summary_window_ms / 1000)}s · ID ${meeting.id}`;
    $('detailStatus').innerHTML = `<span class="pill ${meeting.status === 'ended' ? 'ended' : 'running'}">${statusLabel(meeting.status)}</span>`;
    $('endDetailMeeting').style.display = meeting.status === 'ended' ? 'none' : '';
    // Establish the subscription first; its open handler loads a consistent snapshot.
    startDetailEvents(id);
  } catch (error) { if (epoch !== detailEpoch) return; toast(error.message, true); location.hash = '#/meetings'; }
}
async function loadDetailSegments(id) {
  const rows = [];
  let cursor = -1;
  while (true) {
    const page = await api(`/api/v1/meetings/${id}/segments?after_sequence=${cursor}&limit=500`);
    if (!page.length) break;
    const next = Math.max(...page.map(row => Number(row.sequence_no)));
    if (next <= cursor) break;
    rows.push(...page);
    cursor = next;
    if (page.length < 500) break;
  }
  return rows;
}
async function refreshDetailData(id) {
  if (!detailMeeting || detailMeeting.id !== id) return;
  const epoch = detailEpoch;
  const request = ++detailRequest;
  try {
    const [segments, summaries, board] = await Promise.all([
      loadDetailSegments(id),
      api(`/api/v1/meetings/${id}/summaries`),
      api(`/api/v1/meetings/${id}/board`),
    ]);
    if (epoch !== detailEpoch || request !== detailRequest || detailMeeting?.id !== id) return;
    const current = detailTimeline?.segments || [];
    // Events can arrive while the snapshot is in flight; retain newer revisions.
    const merged = new Map(current.map(seg => [seg.id, seg]));
    segments.forEach(seg => merged.set(seg.id, mergeSegment(merged.get(seg.id), seg)));
    detailTimeline = { segments: [...merged.values()], summaries };
    renderTimeline(detailTimeline.segments, summaries);
    renderBoardInto($('dBoard'), $('dBoardVersion'), board);
  } catch (error) { toast(error.message, true); }
}
// Preview text is transient: keep it through uploads/resync, but never treat it as a final result.
function mergeSegment(previous = {}, incoming) {
  if (Number(incoming.revision || 0) < Number(previous.revision || 0) ||
      (incoming.status === 'partial' && ['completed', 'failed'].includes(previous.status))) return previous;
  const segment = { ...previous, ...incoming };
  if (incoming.status === 'partial') {
    segment.preview_transcript = incoming.transcript?.trim() ? incoming.transcript : previous.preview_transcript;
    segment.transcript = previous.transcript ?? null;
    if (['uploaded', 'transcribing'].includes(previous.status)) segment.status = previous.status;
  }
  if (segment.status === 'completed') delete segment.preview_transcript;
  return segment;
}
function segmentText(segment) {
  if (segment.transcript?.trim()) return segment.transcript;
  if (segment.status === 'completed') return '（未识别到语音）';
  if (segment.preview_transcript) {
    return `${segment.preview_transcript}（临时字幕，${segment.status === 'failed' ? '转写失败' : '等待最终转写'}）`;
  }
  return segment.status === 'failed' ? '转写失败' : '（等待转写）';
}
/* 同一说话人的连续分段（首尾间隔不超过 MERGE_GAP_MS）合并为一条发言展示。
   合并只发生在渲染层：存储、接口与 SSE 事件仍按分段组织。 */
const MERGE_GAP_MS = 5000;
function groupSegments(segments) {
  const sorted = segments.slice().sort((a, b) => ((a.start_ms ?? 0) - (b.start_ms ?? 0)) || ((a.sequence_no ?? 0) - (b.sequence_no ?? 0)));
  const groups = [];
  for (const seg of sorted) {
    const last = groups[groups.length - 1];
    const sameSpeaker = last && String(last.speaker_id) === String(seg.speaker_id ?? '');
    const gap = last ? (seg.start_ms ?? 0) - last.end_ms : Infinity;
    if (sameSpeaker && gap <= MERGE_GAP_MS) {
      last.segments.push(seg);
      last.end_ms = seg.end_ms ?? seg.start_ms ?? last.end_ms;
    } else {
      // key 取组内首个分段 ID：后续分段追加进来时 key 不变，已播放的音频不中断。
      groups.push({ key: seg.id, speaker_id: seg.speaker_id ?? '', speaker_name: seg.speaker_name, start_ms: seg.start_ms ?? 0, end_ms: seg.end_ms ?? seg.start_ms ?? 0, segments: [seg] });
    }
  }
  return groups;
}
/* 组合状态：最后一个分段仍在处理中则跟随它；否则任一失败即显示失败。 */
function groupStatus(segments) {
  const last = segments[segments.length - 1] || {};
  if (['partial', 'transcribing', 'uploaded'].includes(last.status)) return last.status;
  if (segments.some(seg => seg.status === 'failed')) return 'failed';
  return last.status || 'uploaded';
}
function segmentHtml(group) {
  const status = groupStatus(group.segments);
  // 同一发言内的分段文本行内连续排列，不逐段换行；音频各自保留。
  return `
    <article class="segment" data-key="group-${escapeHtml(group.key)}">
      <header>
        <strong class="speaker">${escapeHtml(group.speaker_name || '未知说话人')}</strong>
        <span class="time">${formatMs(group.start_ms)} — ${formatMs(group.end_ms)}</span>
        <span class="pill ${status}">${statusLabel(status)}</span>
      </header>
      <p class="segment-text">${group.segments.map(seg => `<span data-seg="${escapeHtml(seg.id)}">${escapeHtml(segmentText(seg))}</span>`).join('')}</p>
      ${group.segments.filter(seg => seg.has_audio && seg.audio_url).map(seg => `<audio controls preload="none" data-seg="${escapeHtml(seg.id)}" src="${encodeURI(seg.audio_url)}"></audio>`).join('')}
    </article>`;
}
/* 转写时间线与滚动摘要合并为一条时间轴：均按时间倒序（最新在上），
   摘要落在其覆盖窗口结束之后，便于与对应分段对照。 */
function renderTimeline(segments, summaries) {
  const node = $('dSegments');
  const groups = groupSegments(segments);
  $('detailSegmentCount').textContent = `${groups.length} 条发言 · ${segments.length} 个分段 · ${summaries.length} 条摘要`;
  if (!groups.length && !summaries.length) { node.className = 'feed empty'; node.innerHTML = '暂无分段，等待音频上传'; updateDetailPager(0); return; }
  node.className = 'feed';
  const items = [
    ...groups.map(group => ({ kind: 'segment', t: group.end_ms, group })),
    ...summaries.map(sum => ({ kind: 'summary', t: sum.window_end_ms ?? 0, sum })),
  ].sort((a, b) => (b.t - a.t) || (a.kind === 'summary' ? -1 : 1));
  const pages = Math.ceil(items.length / DETAIL_PAGE_SIZE);
  detailPage = Math.min(Math.max(detailPage, 1), pages);
  const pageItems = items.slice((detailPage - 1) * DETAIL_PAGE_SIZE, detailPage * DETAIL_PAGE_SIZE);
  const html = pageItems.map(item => item.kind === 'summary' ? `
    <article class="summary timeline-summary" data-key="summary-${escapeHtml(item.sum.id || item.sum.summary_id || item.t)}">
      <strong>滚动摘要 · ${formatMs(item.sum.window_start_ms)} — ${formatMs(item.sum.window_end_ms)}</strong>
      <span>${escapeHtml(summaryText(item.sum.content))}</span>
    </article>` : segmentHtml(item.group)).join('');
  reconcileTimeline(node, html);
  updateDetailPager(items.length);
}

// Keep existing audio elements connected and playing when a caption changes.
function reconcileTimeline(node, html) {
  [...node.childNodes].filter(child => child.nodeType === 3).forEach(child => child.remove());
  const template = document.createElement('template');
  template.innerHTML = html;
  const existing = new Map([...node.children].map(el => [el.dataset.key, el]));
  let cursor = node.firstElementChild;
  for (const fresh of [...template.content.children]) {
    const key = fresh.dataset.key;
    let element = existing.get(key);
    existing.delete(key);
    if (element && element.matches('.segment')) {
      reconcileSegment(element, fresh);
    } else if (!element || element.innerHTML !== fresh.innerHTML) {
      if (element === cursor) cursor = cursor.nextElementSibling;
      element?.remove();
      element = fresh;
    }
    if (element !== cursor) node.insertBefore(element, cursor);
    cursor = element.nextElementSibling;
  }
  existing.forEach(element => element.remove());
}
/* 就地更新一条发言：文本片段按 data-seg 对齐更新（行内连续排列），已有 audio
   元素原样保留（避免打断播放），新增分段的文本追加到段落末尾、音频追加到尾部。 */
function reconcileSegment(element, fresh) {
  const header = element.querySelector('header');
  const newHeader = fresh.querySelector('header');
  if (header.innerHTML !== newHeader.innerHTML) header.innerHTML = newHeader.innerHTML;
  // Text and audio share segment IDs; reconcile each kind independently.
  for (const selector of ['span[data-seg]', 'audio[data-seg]']) {
    const parent = selector.startsWith('span') ? element.querySelector('.segment-text') : element;
    const existing = new Map([...element.querySelectorAll(selector)].map(el => [el.dataset.seg, el]));
    let cursor = element.querySelector(selector);
    for (const part of fresh.querySelectorAll(selector)) {
      const el = existing.get(part.dataset.seg) || part;
      existing.delete(part.dataset.seg);
      if (el.matches('span') && el.textContent !== part.textContent) el.textContent = part.textContent;
      if (el !== cursor) parent.insertBefore(el, cursor);
      cursor = el.nextElementSibling;
    }
    existing.forEach(el => el.remove());
  }
}
function updateDetailPager(totalItems) {
  const pager = $('dPager');
  const pages = Math.ceil(totalItems / DETAIL_PAGE_SIZE);
  pager.hidden = pages <= 1;
  if (pager.hidden) return;
  $('dPagerInfo').textContent = `第 ${detailPage} / ${pages} 页 · 共 ${totalItems} 条`;
  $('dPagerPrev').disabled = detailPage <= 1;
  $('dPagerNext').disabled = detailPage >= pages;
}
$('dPagerPrev').addEventListener('click', () => { if (detailPage > 1 && detailTimeline) { detailPage -= 1; renderTimeline(detailTimeline.segments, detailTimeline.summaries); } });
$('dPagerNext').addEventListener('click', () => { if (detailTimeline) { detailPage += 1; renderTimeline(detailTimeline.segments, detailTimeline.summaries); } });
function startDetailEvents(id) {
  stopDetailEvents();
  if (typeof EventSource === 'undefined') { refreshDetailData(id); return; }
  const source = new EventSource(`/api/v1/meetings/${id}/events`);
  detailEvents = source;
  let queued = [];
  let syncing = false;
  const apply = (kind, data) => {
    if (detailEvents !== source || detailMeeting?.id !== id) return;
    detailTimeline ||= { segments: [], summaries: [] };
    if (kind.startsWith('segment.')) {
      const segmentId = data.segment_id || data.id;
      if (!segmentId) return;
      const rows = detailTimeline.segments;
      const index = rows.findIndex(row => row.id === segmentId);
      const previous = index >= 0 ? rows[index] : {};
      const segment = mergeSegment(previous, { ...data, id: segmentId,
        ...(kind === 'segment.failed' ? { status: 'failed' } : {}) });
      if (index >= 0) rows[index] = segment; else rows.push(segment);
      renderTimeline(rows, detailTimeline.summaries);
    } else if (kind === 'summary.created') {
      const row = { ...data, id: data.summary_id || data.id };
      detailTimeline.summaries = detailTimeline.summaries.filter(item => item.id !== row.id);
      detailTimeline.summaries.push(row);
      renderTimeline(detailTimeline.segments, detailTimeline.summaries);
    } else if (kind === 'board.updated' && data.content) {
      renderBoardInto($('dBoard'), $('dBoardVersion'), data);
    } else if (kind === 'meeting.ended') {
      $('detailStatus').textContent = '已结束 · 整理中';
      $('endDetailMeeting').style.display = 'none';
    } else if (kind === 'ingest.failed') {
      toast('音频采集失败：' + (data.error || '请检查连接'), true);
    } else if (kind === 'resync.required' || kind === 'board.updated') {
      resync();
    }
  };
  const resync = async () => {
    if (syncing) return;
    syncing = true;
    try {
      await refreshDetailData(id);
    } finally {
      syncing = false;
      const buffered = queued; queued = [];
      buffered.forEach(([kind, data]) => apply(kind, data));
    }
  };
  source.onopen = resync;
  ['segment.partial', 'segment.uploaded', 'segment.transcribed', 'segment.failed', 'segment.updated', 'summary.created', 'board.updated', 'meeting.ended', 'ingest.failed', 'resync.required'].forEach(kind => {
    source.addEventListener(kind, event => {
      try {
        const data = JSON.parse(event.data);
        if (syncing) queued.push([kind, data]); else apply(kind, data);
      } catch (error) { console.warn('Invalid meeting event', error); }
    });
  });
  // EventSource owns reconnect; closing here would permanently disable updates.
  source.onerror = () => { if (detailEvents === source) $('detailStatus').textContent = '连接中断，正在重连…'; };
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
