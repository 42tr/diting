const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const code = fs.readFileSync('frontend/app.js','utf8');

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
