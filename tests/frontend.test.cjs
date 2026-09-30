const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const code = fs.readFileSync('frontend/app.js','utf8');

function detailHarness() {
  let snapshot = [];
  const nodes = new Map();
  class EventSource {
    constructor() { this.handlers = {}; }
    addEventListener(kind, handler) { this.handlers[kind] = handler; }
    close() {}
  }
  const context = { EventSource, console, document: { getElementById(id) {
    if (!nodes.has(id)) nodes.set(id, { addEventListener() {} });
    return nodes.get(id);
  } }, fetch: async url => ({ ok: true, text: async () => JSON.stringify(
    url.includes('/segments?') ? snapshot : url.endsWith('/summaries') ? [] : {}
  ) }) };
  vm.createContext(context);
  vm.runInContext(code.slice(0, code.indexOf("$('meetingStatusFilter').addEventListener")), context);
  vm.runInContext('renderTimeline = () => {}; renderBoardInto = () => {}; detailMeeting = {id:"m"}; startDetailEvents("m");', context);
  return {
    emit(kind, data) { vm.runInContext('detailEvents', context).handlers[kind]({data: JSON.stringify(data)}); },
    async refresh(rows) { snapshot = rows; await vm.runInContext('detailEvents.onopen()', context); },
    row() { return vm.runInContext('detailTimeline.segments[0]', context); },
    html() { return vm.runInContext('segmentHtml(groupSegments(detailTimeline.segments)[0])', context); },
    context,
  };
}

test('preview survives empty upload and reconnect snapshot until final transcription arrives', async () => {
  const h = detailHarness();
  h.emit('segment.partial', {id:'s', transcript:'先看到的字幕', status:'partial', revision:0});
  h.emit('segment.uploaded', {id:'s', transcript:null, status:'uploaded', revision:0, sequence_no:1, has_audio:true, audio_url:'/audio/s'});
  assert.match(h.html(), /先看到的字幕/);
  assert.match(h.html(), /临时字幕/);
  assert.equal(h.row().status, 'uploaded');
  assert.equal(h.row().has_audio, true);
  await h.refresh([{id:'s', transcript:null, status:'transcribing', revision:0, sequence_no:1}]);
  assert.match(h.html(), /先看到的字幕/);
  h.emit('segment.transcribed', {id:'s', transcript:'最终转写', status:'completed', revision:1});
  assert.match(h.html(), /最终转写/);
  assert.doesNotMatch(h.html(), /先看到的字幕|临时字幕/);
  h.emit('segment.partial', {id:'s', transcript:'迟到的预览', status:'partial', revision:0});
  h.emit('segment.uploaded', {id:'s', transcript:null, status:'uploaded', revision:0});
  await h.refresh([{id:'s', transcript:null, status:'uploaded', revision:0, sequence_no:1}]);
  assert.equal(h.row().transcript, '最终转写');
});

test('empty completed result clears preview and does not claim to be waiting', () => {
  const h = detailHarness();
  h.emit('segment.partial', {id:'s', transcript:'临时识别', status:'partial', revision:0});
  h.emit('segment.transcribed', {id:'s', transcript:'', status:'completed', revision:1});
  assert.match(h.html(), /未识别到语音/);
  assert.doesNotMatch(h.html(), /临时识别|等待转写/);
});

test('failed transcription retains clearly marked preview', () => {
  const h = detailHarness();
  h.emit('segment.partial', {id:'s', transcript:'临时识别', status:'partial', revision:0});
  h.emit('segment.failed', {segment_id:'s'});
  assert.match(h.html(), /临时识别/);
  assert.match(h.html(), /转写失败/);
});

test('DOM refresh updates caption spans and preserves audio sharing the same segment ID', () => {
  // Minimal DOM fixture: only operations used by reconcileSegment are needed.
  class Element {
    constructor(tag, id, text = '') { this.tagName = tag.toUpperCase(); this.dataset = {seg:id}; this.textContent = text; this.innerHTML = text; this.children = []; }
    matches(selector) { return selector === '.segment-text' ? this.tagName === 'P' : selector.endsWith('[data-seg]') ? !!this.dataset.seg && this.tagName === selector.split('[')[0].toUpperCase() : selector.toUpperCase() === this.tagName; }
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
    querySelectorAll(selector) { return this.children.flatMap(el => [...(el.matches(selector) ? [el] : []), ...el.querySelectorAll(selector)]); }
    get nextElementSibling() { return this.parent?.children[this.parent.children.indexOf(this) + 1] || null; }
    remove() { if (this.parent) this.parent.children.splice(this.parent.children.indexOf(this), 1); this.parent = null; }
    append(el) { this.insertBefore(el, null); }
    insertBefore(el, next) { el.remove(); const index = next ? this.children.indexOf(next) : this.children.length; this.children.splice(index, 0, el); el.parent = this; }
  }
  const article = (text, extra = false) => {
    const el = new Element('article');
    el.append(new Element('header', undefined, 'header'));
    const paragraph = new Element('p');
    paragraph.append(new Element('span', 's', text));
    if (extra) paragraph.append(new Element('span', 's2', '第二段'));
    el.append(paragraph);
    el.append(new Element('audio', 's'));
    return el;
  };
  const existing = article('（等待转写）');
  const audio = existing.querySelector('audio');
  const h = detailHarness();
  h.context.reconcileSegment(existing, article('已经转写', true));
  assert.equal(existing.querySelector('span').textContent, '已经转写');
  assert.equal(existing.querySelector('audio'), audio);
  assert.deepEqual(existing.querySelectorAll('span').map(el => el.textContent), ['已经转写', '第二段']);
  h.context.reconcileSegment(existing, article('修订文本'));
  assert.equal(existing.querySelector('span').textContent, '修订文本');
  assert.equal(existing.querySelectorAll('span').length, 1);
  assert.equal(existing.querySelector('audio'), audio);
  assert.equal(existing.children.length, 3);
});

test('SSE errors keep the subscription open and reconnect reloads the snapshot', async () => {
  const begin=code.indexOf('function startDetailEvents(id)');
  const end=code.indexOf("$('meetingStatusFilter')",begin);
  const nodes={detailStatus:{textContent:''}};
  let refreshes=0;
  class EventSource {
    constructor(){this.handlers={};this.closed=false;}
    addEventListener(kind,handler){this.handlers[kind]=handler;}
    close(){this.closed=true;}
  }
  const context={EventSource,console,detailEvents:null,detailMeeting:{id:'m'},detailTimeline:null,
    refreshDetailData:async()=>{refreshes++;},$:id=>nodes[id], renderTimeline(){},renderBoardInto(){},toast(){}};
  vm.createContext(context);vm.runInContext(code.slice(begin,end),context);
  vm.runInContext(code.slice(code.indexOf('function mergeSegment('), code.indexOf('function segmentText(')), context);
  context.startDetailEvents('m'); const stream=context.detailEvents;
  stream.onerror();assert.equal(stream.closed,false);
  await stream.onopen();await stream.onopen();assert.equal(refreshes,2);
  stream.handlers['segment.transcribed']({data:JSON.stringify({segment_id:'s',transcript:'edited',revision:2,status:'completed'})});
  stream.handlers['segment.partial']({data:JSON.stringify({segment_id:'s',transcript:'old preview',revision:0,status:'partial'})});
  assert.equal(context.detailTimeline.segments[0].transcript,'edited');
  context.stopDetailEvents();assert.equal(stream.closed,true);
});

test('consecutive segments from the same speaker merge into one group', () => {
  const begin=code.indexOf('const MERGE_GAP_MS');
  const end=code.indexOf('/* 转写时间线与滚动摘要合并为一条时间轴');
  const context={};
  vm.createContext(context);vm.runInContext(code.slice(begin,end),context);
  const seg=(id,speaker,start,end)=>({id,speaker_id:speaker,start_ms:start,end_ms:end,sequence_no:start});
  // 同一说话人、间隔 ≤5s 的分段合并；换人、超过 5s 间隔都会开新的一组
  const groups=context.groupSegments([
    seg('c','sp1',12000,17000),
    seg('a','sp1',0,5000),
    seg('b','sp1',5000,12000),
    seg('d','sp2',18000,20000),
    seg('e','sp2',30000,31000),
  ]);
  const ids=g=>g.segments.map(s=>s.id).join(',');
  assert.equal(groups.length,3);
  assert.equal(ids(groups[0]),'a,b,c');
  assert.equal(groups[0].start_ms,0);assert.equal(groups[0].end_ms,17000);
  assert.equal(groups[0].key,'a');
  assert.equal(ids(groups[1]),'d');
  assert.equal(ids(groups[2]),'e');
  // 未指认说话人（speaker_id 为空）的连续分段也合并为一组
  const anon=context.groupSegments([seg('x',null,0,1000),seg('y',undefined,1500,2000),seg('z',null,9000,10000)]);
  assert.equal(anon.length,2);
  assert.equal(ids(anon[0]),'x,y');
});
