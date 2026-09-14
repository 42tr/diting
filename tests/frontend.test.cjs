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
