const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

// Run the actual embedded panel script against a minimal DOM. No browser
// storage, management credentials or upstream network calls are involved.
class Element {
  constructor(tag='div') {
    this.tagName=tag; this.children=[]; this.style={setProperty(name,value){this[name]=value;},removeProperty(name){delete this[name];}}; this.attributes={}; this.className=''; this.events={}; this.text='';
    this.classList={
      contains:name=>this.className.split(' ').includes(name),
      add:name=>{if(!this.classList.contains(name))this.className+=' '+name;},
      remove:name=>{this.className=this.className.split(' ').filter(item=>item!==name).join(' ');},
      toggle:(name,force)=>{const on=force??!this.classList.contains(name);this.classList[on?'add':'remove'](name);return on;}
    };
  }
  set textContent(value){this.text=String(value??'');this.children=[];}
  get textContent(){return this.text+this.children.map(child=>child.textContent).join('');}
  append(...items){for(const item of items){if(item.tagName==='fragment')this.children.push(...item.children);else this.children.push(item);}}
  replaceChildren(...items){this.text='';this.children=[];this.append(...items);}
  addEventListener(name,fn){this.events[name]=fn;}
  setAttribute(name,value){this.attributes[name]=value;}
  getAttribute(name){return this.attributes[name]??null;}
  scrollIntoView(){}
  focus(){this.focused=true;}
  animate(frames,options){this.animation={frames,options};}
}

function panel(options={}) {
  const nodes=new Map();
  const get=id=>{if(!nodes.has(id))nodes.set(id,new Element());return nodes.get(id);};
  const html=fs.readFileSync(__dirname+'/index.html','utf8');
  const staticNodes=[];
  for(const match of html.split('<script>')[0].matchAll(/<[^>]+data-i18n(?:-[\w-]+)?="[^"]+"[^>]*>/g)){
    const attrs=Object.fromEntries(Array.from(match[0].matchAll(/([\w-]+)="([^"]*)"/g),item=>[item[1],item[2]]));
    const node=attrs.id?get(attrs.id):new Element();
    for(const [key,value] of Object.entries(attrs))node.setAttribute(key,value);
    staticNodes.push(node);
  }
  const root=new Element('html'),host=new Element('html'),windowEvents={},mediaEvents={};
  if(options.hostTheme)host.setAttribute('data-theme',options.hostTheme);
  if(options.hostLanguage)host.setAttribute('lang',options.hostLanguage);
  let storedTheme=options.storedTheme,storedLanguage=options.rawLanguage??(options.storedLanguage?JSON.stringify({state:{language:options.storedLanguage}}):null);
  const media={matches:!!options.darkSystem,addEventListener:(name,fn)=>{mediaEvents[name]=fn;}};
  const observers=[];
  const notify=attr=>{for(const observer of observers)if(observer.filter.includes(attr))observer.fn();};
  const win={addEventListener:(name,fn)=>{(windowEvents[name]??=[]).push(fn);},matchMedia:query=>query.includes('prefers-reduced-motion')?{matches:!!options.reducedMotion}:media};
  win.parent=options.embedded?{document:{documentElement:host},getComputedStyle:()=>({getPropertyValue:name=>options.hostTokens?.[name]||''})}:win;
  if(options.crossOrigin)Object.defineProperty(win,'parent',{get(){throw new Error('cross origin');}});
  const context=vm.createContext({
    document:{documentElement:root,querySelectorAll:()=>staticNodes,getElementById:get,createElement:tag=>new Element(tag),createElementNS:(namespace,tag)=>new Element(tag),createDocumentFragment:()=>new Element('fragment')},
    location:{pathname:options.pathname||'/management.html',host:'localhost',origin:'http://localhost'},
    navigator:{userAgent:'panel-test',language:options.browserLanguage},localStorage:{getItem:name=>{if(options.storageUnavailable)throw new Error('storage unavailable');return name==='cli-proxy-theme'?JSON.stringify({state:{theme:storedTheme}}):name==='cli-proxy-language'?storedLanguage:null;}},
    window:win,MutationObserver:class{constructor(fn){this.fn=fn;}observe(target,config){observers.push({fn:this.fn,filter:config.attributeFilter});}disconnect(){}},
    setInterval(){},setTimeout(){},URL,TextEncoder,TextDecoder,
  });
  const script=html.match(/<script>([\s\S]*?)<\/script>/)[1];
  vm.runInContext(script,context);
  vm.runInContext('globalThis.translateTest=t;globalThis.messagesTest=messages;globalThis.renderTest=renderProbe;globalThis.renderPoolTest=renderPool;globalThis.openExitTest=openExitEditor;globalThis.renderDataTest=render;globalThis.actions=[];globalThis.controlResult=false;probeControl=async payload=>{actions.push(payload);return controlResult;}',context);
  vm.runInContext('globalThis.evalRenderTest=renderEval;globalThis.evalDetailTest=value=>{evalDetail=value;renderEvalDetail(value);};globalThis.evalActions=[];globalThis.evalReply={started:1};evalRequest=async(action,payload)=>{evalActions.push({action,payload});return evalReply;};',context);
  vm.runInContext('globalThis.evalLogoutTest=requireLogin;globalThis.evalErrorTest=evalError;',context);
  vm.runInContext('globalThis.concurrencyRenderTest=renderConcurrency;',context);
return {renderConcurrency:context.concurrencyRenderTest,get,root,host,staticNodes,translate:context.translateTest,messages:context.messagesTest,changeLanguage:language=>{storedLanguage=JSON.stringify({state:{language}});for(const fn of windowEvents.storage||[])fn({key:'cli-proxy-language'});},changeHostLanguage:language=>{host.setAttribute('lang',language);notify('lang');},changeHost:theme=>{host.setAttribute('data-theme',theme);notify('data-theme');},changeStored:theme=>{storedTheme=theme;for(const fn of windowEvents.storage||[])fn({key:'cli-proxy-theme'});},changeSystem:dark=>{media.matches=dark;mediaEvents.change();},render:context.renderTest,renderPool:context.renderPoolTest,openExit:context.openExitTest,renderData:context.renderDataTest,actions:context.actions,setResult:value=>{context.controlResult=value;},logoutEval:context.evalLogoutTest,renderEval:context.evalRenderTest,renderEvalDetail:context.evalDetailTest,evalError:context.evalErrorTest,evalActions:context.evalActions,setEvalReply:value=>{context.evalReply=value;}};
}

function evalFixture(overrides={}){return {can_run:true,accounts:[{scope:'synthetic-a',account_email:'a@example.invalid'},{scope:'synthetic-b',account_email:'b@example.invalid'}],models:['gpt-6-astra'],jobs:[],results:[],...overrides};}
function selectEval(p,index=0){const input=p.get('eval-accounts').children[index].children[0];input.checked=true;input.events.change();}

function concurrencyFixture(overrides={}){const data={accounts:[{account:'safe-a',file_name:'a@example.invalid.json',account_email:'a@example.invalid',limit:8,in_flight:8},{account:'safe-b',file_name:'b.json',limit:2,in_flight:0}],active:[{account:'safe-a',request_id:'request-safe',model:'gpt-6-astra',kind:'business',status:'running',started_at:new Date().toISOString()}],recent:[],...overrides};data.accounts=data.accounts.map(a=>({weekly_threshold:0,...a}));return data;}
function concurrencyInput(p,index=0){return p.get('concurrency-accounts').children[index].children[0].children[3].children[0].children[1];}
function concurrencySave(p,index=0){return p.get('concurrency-accounts').children[index].children[0].children[3].children[2];}
function weeklyInput(p,index=0){return p.get('concurrency-accounts').children[index].children[0].children[3].children[1].children[1];}
function weeklySave(p,index=0){return concurrencySave(p,index);}

test('account settings omit the side description and retain quota evidence in the status tooltip',()=>{
  const p=panel(),account={account:'safe-a',file_name:'a@example.invalid.json',account_email:'a@example.invalid',limit:0,in_flight:2,weekly_threshold:90,weekly_quota:{state:'paused',used_percent:91,reset_at:'2026-10-08T00:00:00Z',updated_at:'2026-10-06T00:00:00Z'}};
  p.renderConcurrency(concurrencyFixture({accounts:[account]}));
  const input=weeklyInput(p);input.value='85.5';input.events.input();
  for(const lang of ['zh-CN','en','zh-TW','ru']){p.changeLanguage(lang);assert.equal(weeklyInput(p),input);assert.equal(input.value,'85.5');assert.match(p.get('concurrency-accounts').children[0].children[0].children[2].title,/91%/);assert.equal(p.get('concurrency-accounts').children[0].children.length,1);assert.doesNotMatch(p.get('concurrency-accounts').textContent,/91%/);assert.ok(p.get('concurrency-accounts').textContent.includes(p.translate('7d 阈值暂停')));assert.doesNotMatch(p.get('concurrency-accounts').textContent,/a@example/);}
  p.renderConcurrency(concurrencyFixture({accounts:[{...account,weekly_quota:{state:'ready',used_percent:0}}]}));assert.match(p.get('concurrency-accounts').children[0].children[0].children[2].title,/0%/);assert.equal(concurrencyInput(p).value,'0');
  p.renderConcurrency(concurrencyFixture({accounts:[{...account,weekly_quota:{state:'unknown',error:'weekly_quota_query_failed'}}]}));assert.ok(p.get('concurrency-accounts').textContent.includes(p.translate('等待额度确认')));assert.ok(p.get('concurrency-accounts').children[0].children[0].children[2].title.includes(p.translate('额度查询失败，将自动重试；未确认刷新')));
});

test('one account save submits both fields, preserves both drafts on failure and accepts blank threshold as off',async()=>{
  const p=panel();p.renderConcurrency(concurrencyFixture());
  for(const value of ['-1','100.1','NaN','Infinity','bad']){weeklyInput(p).value=value;await weeklySave(p).events.click();assert.equal(p.evalActions.length,0);}
  concurrencyInput(p).value='6';concurrencyInput(p).events.input();weeklyInput(p).value='90.5';weeklyInput(p).events.input();p.setEvalReply(concurrencyFixture({accounts:[{account:'safe-a',limit:6,weekly_threshold:90.5,weekly_quota:{state:'unknown'}}]}));await weeklySave(p).events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.evalActions[0])),{action:'account-concurrency',payload:{account:'safe-a',limit:6,weekly_threshold:90.5}});assert.equal(concurrencyInput(p).value,'6');assert.equal(weeklyInput(p).value,'90.5');assert.equal(concurrencySave(p),weeklySave(p));
  p.setEvalReply(null);concurrencyInput(p).value='3';concurrencyInput(p).events.input();weeklyInput(p).value='75';weeklyInput(p).events.input();await weeklySave(p).events.click();assert.equal(weeklyInput(p).value,'75');assert.equal(concurrencyInput(p).value,'3');
  weeklyInput(p).value='';p.setEvalReply(concurrencyFixture({accounts:[{account:'safe-a',limit:8,weekly_threshold:0}]}));await weeklySave(p).events.click();assert.equal(p.evalActions.at(-1).payload.weekly_threshold,0);assert.equal(weeklyInput(p).value,'0');
});

test('late weekly saves cannot resurrect a signed-out session or overwrite unrelated settings',async()=>{
  const p=panel();p.renderConcurrency(concurrencyFixture());let finish;p.setEvalReply(new Promise(resolve=>finish=resolve));weeklyInput(p).value='90';const pending=weeklySave(p).events.click();p.logoutEval();p.renderConcurrency(concurrencyFixture({accounts:[{account:'safe-a',limit:3,weekly_threshold:70}]}));finish(concurrencyFixture({accounts:[{account:'safe-a',limit:8,weekly_threshold:90}]}));await pending;assert.equal(weeklyInput(p).value,'70');assert.equal(concurrencyInput(p).value,'3');assert.equal(weeklySave(p).disabled,false);
});

test('concurrency settings show only enabled credentials and restore their saved limits on re-enable',()=>{
  const p=panel(),off={account:'off',file_name:'DISABLED_FILE_CANARY.json',account_email:'off@example.invalid',limit:9,in_flight:0,disabled:true},on={account:'on',file_name:'enabled.json',limit:3,in_flight:1,disabled:false,unavailable:true};
  const data=concurrencyFixture({accounts:[off,on],active:[{account:'off',request_id:'old-active',model:'gpt-6-astra',kind:'business',status:'running',started_at:new Date().toISOString()}],recent:[{account:'off',request_id:'old-finished',model:'gpt-6-astra',kind:'business',status:'succeeded',started_at:new Date().toISOString(),completed_at:new Date().toISOString()}]});
  p.renderConcurrency(data);assert.equal(p.get('concurrency-accounts').children.length,1);assert.equal(concurrencyInput(p).value,'3');assert.match(p.get('concurrency-accounts').textContent,/凭据 1/);assert.match(p.get('concurrency-active').textContent,/已停用凭据/);assert.match(p.get('concurrency-recent').textContent,/old-finished/);
  p.get('account-email-toggle').events.click();assert.doesNotMatch(p.get('concurrency-accounts').textContent,/DISABLED_FILE_CANARY|off@example/);
  assert.equal(data.accounts.length,2);assert.equal(data.accounts[0].limit,9);
  p.renderConcurrency({...data,accounts:[{...off,disabled:false},on]});assert.equal(p.get('concurrency-accounts').children.length,2);assert.equal(concurrencyInput(p).value,'9');
  p.renderConcurrency(data);assert.equal(p.get('concurrency-accounts').children.length,1);assert.equal(p.evalActions.length,0);
});

test('all disabled concurrency credentials produce a localized empty setting list without deleting history',()=>{
  const p=panel(),data=concurrencyFixture({accounts:[{account:'safe-a',limit:8,disabled:true}],recent:[{account:'safe-a',request_id:'preserved-history',model:'gpt-6-astra',kind:'business',status:'succeeded',started_at:new Date().toISOString(),completed_at:new Date().toISOString()}]});p.renderConcurrency(data);
  for(const lang of ['zh-CN','en','zh-TW','ru']){p.changeLanguage(lang);assert.equal(p.get('concurrency-accounts').children.length,0);assert.equal(p.get('concurrency-accounts').textContent,p.translate('没有已启用的原生 Codex 凭据'));assert.match(p.get('concurrency-recent').textContent,/preserved-history/);}
});

test('account concurrency preserves drafts, localizes states and protects credential filenames',()=>{
  const p=panel();p.renderConcurrency(concurrencyFixture());const input=concurrencyInput(p);input.value='3';input.events.input();
  p.get('concurrency-active-panel').open=false;
  for(const lang of ['zh-CN','en','zh-TW','ru']){p.changeLanguage(lang);p.renderConcurrency(concurrencyFixture());assert.equal(concurrencyInput(p).value,'3');assert.equal(p.get('concurrency-active-panel').open,false);assert.doesNotMatch(p.get('concurrency-accounts').textContent,/a@example/);assert.ok(p.get('concurrency-accounts').textContent.includes(p.translate('并发已满')));}
  assert.equal(concurrencyInput(p),input,'polling must retain the input and save controls');
  p.get('account-email-toggle').events.click();assert.match(p.get('concurrency-accounts').textContent,/a@example.invalid.json/);
  p.get('account-email-toggle').events.click();assert.doesNotMatch(p.get('concurrency-accounts').textContent,/a@example/);
});

test('account limits save explicitly and failed saves keep the draft and old limit',async()=>{
  const p=panel();p.renderConcurrency(concurrencyFixture());
  for(const value of ['-1','1.5','10001','junk']){concurrencyInput(p).value=value;await concurrencySave(p).events.click();assert.equal(p.evalActions.length,0);}
  concurrencyInput(p).value='0';concurrencyInput(p).events.input();p.setEvalReply(concurrencyFixture({accounts:[{account:'safe-a',limit:0,in_flight:8}]}));await concurrencySave(p).events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.evalActions[0])),{action:'account-concurrency',payload:{account:'safe-a',limit:0,weekly_threshold:0}});assert.equal(concurrencyInput(p).value,'0');assert.match(p.get('concurrency-accounts').textContent,/8 \/ 不限/);
  p.setEvalReply(null);concurrencyInput(p).value='2';concurrencyInput(p).events.input();await concurrencySave(p).events.click();assert.equal(concurrencyInput(p).value,'2');assert.match(p.get('concurrency-accounts').textContent,/8 \/ 不限/);
  p.renderConcurrency(concurrencyFixture({storage_error:'concurrency_settings_unavailable'}));assert.equal(concurrencySave(p).disabled,true);
});

test('concurrency sign-out clears sensitive state and late save results do not restore it',async()=>{
  const p=panel();p.renderConcurrency(concurrencyFixture());p.get('account-email-toggle').events.click();let finish;p.setEvalReply(new Promise(resolve=>finish=resolve));concurrencyInput(p).value='2';const pending=concurrencySave(p).events.click();p.logoutEval();finish(concurrencyFixture());await pending;
  for(const lang of ['zh-CN','en','ru']){p.changeLanguage(lang);assert.equal(p.get('concurrency-accounts').children.length,0);assert.equal(p.get('concurrency-active').children.length,0);}
  p.renderData({records:[{time:new Date().toISOString(),request_id:'running',model:'gpt-6-astra',concurrency_state:'running',concurrency_rerouted:true,original:[]}],turn_state_override:{probe:fixture()}});
  p.changeLanguage('zh-CN');assert.match(p.get('rows').textContent,/执行中/);assert.match(p.get('rows').textContent,/请求已换号/);
});

test('an old concurrency save cannot overwrite a subsequent login with the same account',async()=>{
  const p=panel();p.renderConcurrency(concurrencyFixture());let finish;p.setEvalReply(new Promise(resolve=>finish=resolve));concurrencyInput(p).value='9';const pending=concurrencySave(p).events.click();p.logoutEval();p.renderConcurrency(concurrencyFixture({accounts:[{account:'safe-a',limit:2,in_flight:0}]}));finish(concurrencyFixture({accounts:[{account:'safe-a',limit:9,in_flight:0}]}));await pending;assert.equal(concurrencyInput(p).value,'2');assert.equal(concurrencySave(p).disabled,false);assert.equal(p.get('concurrency-feedback').textContent,'');
});

test('concurrent saves for different credentials retain both limits when responses arrive out of order',async()=>{
  const p=panel();p.renderConcurrency(concurrencyFixture());let finishA,finishB;
  p.setEvalReply(new Promise(resolve=>finishA=resolve));concurrencyInput(p,0).value='3';const a=concurrencySave(p,0).events.click();
  p.setEvalReply(new Promise(resolve=>finishB=resolve));concurrencyInput(p,1).value='6';const b=concurrencySave(p,1).events.click();
  finishB(concurrencyFixture({accounts:[{account:'safe-a',limit:8,in_flight:0},{account:'safe-b',limit:6,in_flight:0}]}));await b;
  finishA(concurrencyFixture({accounts:[{account:'safe-a',limit:3,in_flight:0},{account:'safe-b',limit:2,in_flight:0}]}));await a;
  assert.equal(concurrencyInput(p,0).value,'3');assert.equal(concurrencyInput(p,1).value,'6');
});

test('evaluation visuals distinguish scoring from execution and request errors',()=>{
  const p=panel(),base={scope:'synthetic-a',kind:'candy',status:'completed',done:2,graded:2,correct:2};
  p.renderEval(evalFixture({results:[base,{...base,correct:1},{...base,status:'failed',graded:0,correct:0},{...base,graded:0,correct:0},{...base,kind:'modeltrace'}]}));
  const badges=p.get('eval-results').children.map(row=>row.children[4].children[0]);
  assert.deepEqual(badges.map(b=>b.getAttribute('data-tone')),['ok','danger','warn','warn','info']);
  assert.match(badges[1].textContent,/含错误答案/);assert.match(badges[2].textContent,/请求失败/);
  assert.doesNotMatch(badges[2].textContent,/答案错误/);assert.doesNotMatch(badges[4].textContent,/答案正确/);
  assert.match(p.get('eval-results').children[2].textContent,/—/);assert.doesNotMatch(p.get('eval-results').children[2].textContent,/0%/);
  for(const badge of badges){const icon=badge.children[0];assert.equal(icon.tagName,'svg');assert.equal(icon.getAttribute('aria-hidden'),'true');assert.equal(icon.getAttribute('focusable'),'false');assert.equal(icon.getAttribute('stroke'),'currentColor');}
});

test('sample badges keep distinct icons and translated text across all locales',()=>{
  const p=panel();p.renderEval(evalFixture());
  p.renderEvalDetail({id:'visual',samples:[{status:'correct'},{status:'incorrect'},{status:'error',error:'invalid_response_event'},{status:'cancelled'}]});
  for(const lang of ['zh-CN','en','zh-TW','ru']){
    p.changeLanguage(lang);const badges=p.get('eval-detail-body').children.slice(1).map(item=>item.children[0].children[1]);
    assert.deepEqual(badges.map(b=>b.getAttribute('data-tone')),['ok','danger','warn','neutral']);
    assert.equal(badges[0].textContent,p.translate('答案正确'));assert.equal(badges[2].textContent,p.translate('请求失败'));
    assert.equal(new Set(badges.map(b=>b.children[0].children[1].getAttribute('d'))).size,4);
    assert.equal(p.get('eval-legend').children.length,6);
  }
  const html=fs.readFileSync(__dirname+'/index.html','utf8');
  assert.match(html,/<button id="eval-run"[^>]*><svg[^>]*aria-hidden="true"[^>]*>[\s\S]*?<span data-i18n="开始测试">/);
  assert.doesNotMatch(html,/<button id="eval-run"[^>]*data-i18n=/);
  assert.match(html,/@media\(forced-colors:active\)\{\.status-pill/);
});

test('ability tests require selected accounts and an explicit quota confirmation',async()=>{
  const p=panel();p.renderEval(evalFixture());assert.equal(p.get('eval-run').disabled,true);
  await p.get('eval-run').events.click();assert.equal(p.evalActions.length,0);
  selectEval(p);assert.equal(p.get('eval-run').disabled,true);
  p.get('eval-confirm').checked=true;p.get('eval-confirm').events.change();assert.equal(p.get('eval-run').disabled,false);
  await p.get('eval-run').events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.evalActions)),[{action:'eval-run',payload:{accounts:['synthetic-a'],kind:'candy',model:'gpt-6-astra',runs:1,effort:'none',confirmed:true}}]);
  assert.equal(p.get('eval-confirm').checked,false);
});

test('ability drafts and account selections survive refresh and locale changes',()=>{
  const p=panel();p.renderEval(evalFixture());selectEval(p);p.get('eval-runs').value='7';p.get('eval-model').value='gpt-6.1-sol';p.get('eval-effort').value='xhigh';
  for(const language of ['en','zh-TW','ru','zh-CN']){p.changeLanguage(language);p.renderEval(evalFixture());assert.equal(p.get('eval-runs').value,'7');assert.equal(p.get('eval-model').value,'gpt-6.1-sol');assert.equal(p.get('eval-effort').value,'xhigh');assert.equal(p.get('eval-accounts').children[0].children[0].checked,true);}
  assert.match(p.get('eval-budget').textContent,/共 7 次请求/);
  p.renderEval(evalFixture({accounts:[{scope:'synthetic-b'}]}));assert.equal(p.get('eval-accounts').children[0].children[0].checked,false);
});

test('ModelTrace fixes sampling parameters and request budget without overwriting candy drafts',async()=>{
  const p=panel();p.renderEval(evalFixture());selectEval(p);p.get('eval-runs').value='10';p.get('eval-effort').value='xhigh';
  p.get('eval-kind').value='modeltrace';p.get('eval-kind').events.change();assert.equal(p.get('eval-runs').disabled,true);assert.match(p.get('eval-budget').textContent,/共 3 次请求/);
  p.get('eval-confirm').checked=true;p.get('eval-confirm').events.change();await p.get('eval-run').events.click();
  assert.equal(p.evalActions[0].payload.runs,1);assert.equal(p.evalActions[0].payload.effort,'none');
  p.get('eval-kind').value='candy';p.get('eval-kind').events.change();assert.equal(p.get('eval-runs').value,'10');assert.equal(p.get('eval-effort').value,'xhigh');
});

test('ability validation blocks invalid budgets, models, busy accounts and corrupt storage',()=>{
  const p=panel();p.renderEval(evalFixture());selectEval(p);p.get('eval-confirm').checked=true;
  for(const value of ['0','11','1.5','not-a-number']){p.get('eval-runs').value=value;p.get('eval-runs').events.input();assert.equal(p.get('eval-run').disabled,true);}
  p.get('eval-runs').value='1';p.get('eval-model').value='<script>';p.get('eval-model').events.input();assert.equal(p.get('eval-run').disabled,true);
  p.get('eval-model').value='gpt-6-astra';p.renderEval(evalFixture({jobs:[{id:'j',scope:'synthetic-a',phase:'running',kind:'candy',done:0,total:1}]}));assert.equal(p.get('eval-run').disabled,true);
  p.renderEval(evalFixture({storage_error:'eval_storage_invalid'}));assert.equal(p.get('eval-run').disabled,true);assert.match(p.get('eval-feedback').textContent,/不可写/);
});

test('manual ability controls do not resume paused health probes',async()=>{
  const p=panel();p.renderData({records:[],turn_state_override:{probe:fixture({halted:true,paused:['gpt-6-astra']})}});
  p.renderEval(evalFixture());selectEval(p);p.get('eval-effort').value='medium';
  p.get('eval-confirm').checked=true;p.get('eval-confirm').events.change();
  assert.equal(p.get('eval-run').disabled,false);
  await p.get('eval-run').events.click();
  assert.equal(p.evalActions[0].action,'eval-run');assert.equal(p.evalActions[0].payload.effort,'medium');
  assert.equal(p.actions.length,0);
  const html=fs.readFileSync(__dirname+'/index.html','utf8');
  assert.match(html,/<option value="medium">medium<\/option>/);
  const notices=p.staticNodes.filter(node=>node.getAttribute('data-i18n')?.startsWith('糖果题沿用')||node.getAttribute('data-i18n')?.startsWith('CPA 当前账号路由'));
  for(const lang of ['zh-CN','en','zh-TW','ru']){
    p.changeLanguage(lang);assert.match(notices.map(node=>node.textContent).join(' '),/21/);
    assert.doesNotMatch(notices.map(node=>node.textContent).join(' '),/29|FINAL_ANSWER/);
  }
});

test('ability errors distinguish request, parameters, health pause, storage and busy accounts in every locale',()=>{
  const p=panel(),codes=['invalid_eval_request','invalid_eval_parameters','model_paused','eval_storage_write_failed','engine_unavailable','account_inventory_unavailable','account_unavailable_or_changed','account_eval_busy'];
  for(const lang of ['zh-CN','en','zh-TW','ru']){
    p.changeLanguage(lang);const messages=codes.map(code=>p.evalError(code));
    assert.equal(new Set(messages).size,codes.length);
    if(lang!=='zh-CN')for(const message of messages)assert.doesNotMatch(message,/测试|账号|暂停|存储/);
  }
  p.changeLanguage('zh-CN');assert.match(p.evalError('invalid_eval_parameters'),/次数和推理强度/);
  assert.doesNotMatch(p.evalError('invalid_eval_request'),/暂停/);
  assert.match(p.evalError('model_paused'),/更新插件/);
});

test('legacy candy scores remain labelled and separate from the corrected benchmark',()=>{
  const p=panel(),old={id:'old',scope:'synthetic-a',kind:'candy',revision:'candy-shapes-v2-final-29',status:'completed',correct:1,graded:1};
  const current={...old,id:'new',revision:'candy-shapes-v3-original-21'};
  p.renderEval(evalFixture({results:[old,current]}));
  p.renderEvalDetail({...old,samples:[{status:'correct',text:'FINAL_ANSWER=29'}]});
  for(const lang of ['zh-CN','en','zh-TW','ru']){
    p.changeLanguage(lang);assert.match(p.get('eval-results').children[0].textContent,/29/);
    assert.doesNotMatch(p.get('eval-results').children[1].textContent,/29/);
    assert.match(p.get('eval-detail-body').children[1].textContent,/29/);
  }
  p.renderEvalDetail({...current,samples:[{status:'correct',text:'21'}]});
  assert.doesNotMatch(p.get('eval-detail-body').textContent,/29/);
});

test('evaluation requests share the request table with timezone results but no health verdicts',()=>{
  const p=panel(),record={time:new Date().toISOString(),request_id:'eval-safe-1',request_kind:'evaluation',evaluation_id:'job-safe',evaluation_kind:'candy',evaluation_sample:1,model:'gpt-6-astra',upstream_model:'gpt-5.6-luna',account:'safe',account_email:'a@example.invalid',request_outcome:'succeeded',timing_source:'evaluation-callback',request_latency_ms:2000,first_output_latency_ms:500,response_tokens_output:20,response_tokens_reasoning:5,response_tokens_input:12,response_tokens_complete:true,output_tps:10,target:'America/Los_Angeles',original:['UTC'],action:'replaced'};
  const data={records:[record],total:1,turn_state_override:{probe:fixture()}};
  p.renderData(data);
  for(const lang of ['zh-CN','en','zh-TW','ru']){
    p.changeLanguage(lang);const row=p.get('rows').children[0];
    assert.equal(row.children.length,8);assert.match(row.textContent,/gpt-5\.6-luna/);assert.match(row.textContent,/10\.0 t\/s/);
    assert.doesNotMatch(row.textContent,/a@example|模型不一致|降智已拦截|undefined/);
    assert.match(row.title,/eval-safe-1/);assert.match(row.children[7].children[1].title,lang==='zh-CN'?/不含排队/:lang==='en'?/excluding queue/:lang==='zh-TW'?/不含排隊/:/без ожидания/);
  }
  p.changeLanguage('zh-CN');assert.match(p.get('rows').textContent,/糖果测试/);assert.match(p.get('rows').textContent,/UTC/);assert.match(p.get('rows').textContent,/America\/Los_Angeles/);assert.match(p.get('rows').textContent,/已替换/);assert.doesNotMatch(p.get('rows').textContent,/不做时区转换/);
  p.get('account-email-toggle').events.click();assert.match(p.get('rows').textContent,/a@example.invalid/);
  p.renderData({...data,records:[{...record,evaluation_kind:'modeltrace',request_outcome:'failed',evaluation_error:'invalid_response_event',response_tokens_complete:false}]});
  assert.match(p.get('rows').textContent,/ModelTrace|响应流解析失败|invalid_response_event|请求已失败/);
  assert.doesNotMatch(p.get('rows').textContent,/10\.0 t\/s/);
});

test('evaluation timezone history uses measured target rather than current settings',()=>{
  const p=panel(),base={time:new Date().toISOString(),request_kind:'evaluation',evaluation_kind:'modeltrace',model:'gpt-6-astra'};
  const data={target:'Asia/Tokyo',records:[{...base,target:'America/Los_Angeles',original:[],action:'inserted'},{...base}],turn_state_override:{probe:fixture()}};
  p.renderData(data);
  for(const lang of ['zh-CN','en','zh-TW','ru']){
    p.changeLanguage(lang);const rows=p.get('rows').children;
    assert.equal(rows[0].children[4].textContent,'America/Los_Angeles');assert.notEqual(rows[0].children[4].textContent,'Asia/Tokyo');
    assert.equal(rows[1].children[4].textContent,'—');assert.ok(rows[1].children[3].textContent.includes(p.translate('旧版评测：未记录时区')));
  }
  p.changeLanguage('zh-CN');assert.match(p.get('rows').children[0].textContent,/已补入/);
});

test('evaluation parsing failures are labelled as errors rather than model answers',()=>{
  const p=panel();p.renderEval(evalFixture());
  p.renderEvalDetail({id:'failed',kind:'candy',samples:[{status:'error',error:'invalid_response_event',prompt:'synthetic prompt',request_id:'eval-fixture-1'}]});
  for(const lang of ['zh-CN','en','zh-TW','ru']){
    p.changeLanguage(lang);const sample=p.get('eval-detail-body').children[1];
    assert.match(sample.textContent,/invalid_response_event|eval-fixture-1/);
    assert.ok(sample.children.some(n=>n.textContent===p.translate('错误')));
    assert.ok(!sample.children.some(n=>n.textContent===p.translate('回答')));
  }
  p.changeLanguage('zh-CN');assert.match(p.get('eval-detail-body').textContent,/不是模型答错/);
});

test('ability histories keep email private and render untrusted text literally',()=>{
  const p=panel();p.renderEval(evalFixture({results:[{id:'r',scope:'synthetic-a',kind:'modeltrace',model:'<img src=x>',status:'partial',attribution:{prediction:'<script>',probability:.6,used_outputs:2}}]}));
  assert.doesNotMatch(p.get('eval-results').textContent,/a@example/);assert.match(p.get('eval-results').textContent,/<script>/);assert.match(p.get('eval-results').textContent,/有效样本 2\/3/);
  p.get('account-email-toggle').events.click();assert.match(p.get('eval-results').textContent,/a@example.invalid/);
  p.get('account-email-toggle').events.click();assert.doesNotMatch(p.get('eval-results').textContent,/a@example/);
});

test('ability detail samples stay open across translation and display real usage',()=>{
  const p=panel();p.renderEval(evalFixture());p.renderEvalDetail({id:'d',revision:'synthetic-revision',samples:[{status:'correct',prompt:'<prompt>',text:'FINAL_ANSWER=29',usage:{input_tokens:12,output_tokens:5,reasoning_tokens:2}}]});
  const sample=p.get('eval-detail-body').children[1];sample.open=true;sample.events.toggle();p.changeLanguage('en');
  assert.equal(p.get('eval-detail-body').children[1].open,true);assert.match(p.get('eval-detail-body').textContent,/input 12 \/ output 5 \/ reasoning 2/);assert.match(p.get('eval-detail-body').textContent,/<prompt>/);
  p.get('eval-detail-close').events.click();assert.equal(p.get('eval-detail').classList.contains('hidden'),true);
});

test('sign-out removes ability records and details without later locale resurrection',()=>{
  const p=panel();p.renderEval(evalFixture({results:[{scope:'synthetic-a',kind:'candy',model:'PRIVATE_HISTORY_CANARY',status:'completed'}]}));
  p.renderEvalDetail({id:'secret-detail',samples:[{text:'PRIVATE_ANSWER_CANARY',status:'correct'}]});
  p.logoutEval();p.changeLanguage('ru');assert.equal(p.get('eval-results').children.length,0);assert.equal(p.get('eval-detail-body').children.length,0);assert.equal(p.get('eval-run').disabled,true);
});

test('native ability API pins new namespace and checks the session after JSON parsing',async()=>{
  const html=fs.readFileSync(__dirname+'/index.html','utf8');
  const source=html.slice(html.indexOf('  async function evalRequest('),html.indexOf('  async function refreshEval('));
  let key='synthetic-key',logouts=0;const calls=[];
  const c=vm.createContext({prefix:'/cpa',rejectedKey:'',readManagementKey:()=>key,requireLogin:()=>logouts++,fetch:async(url,options)=>{calls.push({url,options});return {ok:true,status:202,json:async()=>({started:1})};}});
  vm.runInContext(source+'\nglobalThis.request=evalRequest;',c);
  const result=await c.request('eval-run',{confirmed:true});assert.equal(result.started,1);assert.equal(calls[0].url,'/cpa/v0/management/oai-adversarial-plugin/eval-run');assert.equal(calls[0].options.headers.Authorization,'Bearer synthetic-key');assert.equal(calls[0].options.method,'POST');
  c.fetch=async()=>({ok:true,status:200,json:async()=>{key='changed-session';return {private_data:'should-not-render'};}});
  assert.equal(await c.request('eval'),null);assert.equal(logouts,1);
});

test('native ability API does not call the host without login and clears rejected sessions',async()=>{
  const html=fs.readFileSync(__dirname+'/index.html','utf8');
  const source=html.slice(html.indexOf('  async function evalRequest('),html.indexOf('  async function refreshEval('));
  let key='',calls=0,logouts=0;
  const c=vm.createContext({prefix:'',rejectedKey:'',readManagementKey:()=>key,requireLogin:()=>logouts++,fetch:async()=>{calls++;return {status:401};}});
  vm.runInContext(source+'\nglobalThis.request=evalRequest;',c);
  assert.equal(await c.request('eval'),null);assert.equal(calls,0);
  key='synthetic-key';assert.equal(await c.request('eval'),null);assert.equal(c.rejectedKey,key);assert.equal(logouts,2);
});

function fixture(overrides={}) {
  return {enabled:true,running:false,halted:false,prefetch_minutes:3,paused:[],active_models:[],queue_models:[],
    values:[{model:'gpt-6-astra',valid:true,value_length:292,source:'probe',remaining_seconds:180,
      issued_at:new Date(Date.now()-52*60000).toISOString(),expires_at:new Date(Date.now()+3*60000).toISOString()}],...overrides};
}

test('renamed panel links keep the CPA reverse-proxy prefix',()=>{
  for(const prefix of ['', '/cpa']){
    const p=panel({pathname:prefix+'/v0/resource/plugins/oai-adversarial-plugin/status'});
    assert.equal(p.get('management-login').href,prefix+'/management.html#/plugin-pages/oai-adversarial-plugin/0');
  }
});

test('renamed panel posts to the new API namespace',async()=>{
  const html=fs.readFileSync(__dirname+'/index.html','utf8');
  const source=html.slice(html.indexOf('  async function probeControl('),html.indexOf('  function remainingText('));
  for(const prefix of ['', '/cpa']){
    const calls=[];
    const c=vm.createContext({prefix,controlRevision:0,readManagementKey:()=> 'synthetic-key',
      fetch:async(url,options)=>{calls.push({url,options});return {ok:true,status:200};}});
    vm.runInContext(source+'\nglobalThis.control=probeControl;',c);
    assert.equal(await c.control({action:'rename-test-unknown'}),true);
    assert.equal(calls[0].url,prefix+'/v0/management/oai-adversarial-plugin/probe-control');
    assert.equal(calls[0].options.method,'POST');
    assert.equal(calls[0].options.headers.Authorization,'Bearer synthetic-key');
    assert.deepEqual(JSON.parse(calls[0].options.body),{action:'rename-test-unknown'});
  }
});

test('full stop and zero prefetch window do not advertise automatic takeover',()=>{
  const p=panel();p.render(fixture({halted:true}));
  assert.equal(p.get('probe-status').textContent,'全部停止');
  assert.match(p.get('probe-times').textContent,/自动预备已停止/);
  assert.doesNotMatch(p.get('probe-times').textContent,/到期前/);
  p.render(fixture({prefetch_minutes:0,pool_attempts:0}));
  assert.equal(p.get('probe-status').textContent,'手动待命');
  assert.match(p.get('probe-times').textContent,/窗口为 0/);
  assert.match(p.get('probe-times').textContent,/池预算 0 次/);
});

test('idle participation, pause and one-off probing remain distinct',()=>{
  const p=panel();p.render(fixture());
  let row=p.get('probe-values').children[0];
  assert.equal(row.classList.contains('paused-row'),false);
  assert.match(row.children[0].textContent,/自动预备待命/);
  assert.equal(row.children[6].children[0].textContent,'暂停');
  p.render(fixture({paused:['gpt-6-astra']}));
  row=p.get('probe-values').children[0];
  assert.equal(row.classList.contains('paused-row'),true);
  assert.equal(row.children[6].children[0].textContent,'恢复');
  assert.equal(row.children[6].children[1].disabled,true);
  p.render(fixture({halted:true,running:true,active_models:['gpt-6-astra']}));
  assert.equal(p.get('probe-status').textContent,'运行中');
  assert.match(p.get('probe-times').textContent,/自动预备已停止/);
  assert.equal(p.get('round-start').disabled,false);
});

test('stopping remains visible until drain and keeps the prefetch mode clear',()=>{
  const p=panel();p.render(fixture({running:true,stopping:true,active_models:['gpt-6-astra']}));
  assert.equal(p.get('probe-status').textContent,'正在停止');
  assert.match(p.get('probe-times').textContent,/本轮结束后继续自动预备/);
  assert.equal(p.get('round-start').disabled,true);
  assert.match(p.get('probe-values').textContent,/正在停止/);
});

test('business evidence takes precedence and rejection control is restored after refresh',()=>{
  const p=panel();p.get('reject-toggle').disabled=true;
  p.render(fixture({failures:[{model:'gpt-6-astra',attempts:3}],business:[{model:'gpt-6-astra',reason:'业务模型不一致'}]}));
  assert.equal(p.get('reject-toggle').disabled,false);
  assert.match(p.get('probe-values').textContent,/业务确认/);
  assert.doesNotMatch(p.get('probe-values').textContent,/上次探测/);
  assert.match(p.get('probe-values').textContent,/剩余 3m 00s/);
});

test('turn-state injection control reflects state and sends the dashboard action',async()=>{
const p=panel();
p.render(fixture());
const button=p.get('turn-state-inject-toggle');
assert.equal(button.disabled,false);
assert.equal(button.className.includes('on'),true);
assert.match(button.textContent,/注入 X-Codex-Turn-State：已开启/);
p.render(fixture({turn_state_inject_enabled:false}));
assert.equal(button.className.includes('on'),false);
assert.match(button.textContent,/注入 X-Codex-Turn-State：已关闭/);
p.setResult(true);
await button.events.click();
assert.deepEqual(JSON.parse(JSON.stringify(p.actions)),[{action:'turn-state-inject',enabled:true}]);
});

test('suspected degraded tier badge and token usage follow the accepted lengths policy',()=>{
const p=panel();
const record={time:new Date().toISOString(),model:'gpt-6-astra',upstream_model:'gpt-5.6-luna',
  model_checked:true,model_mismatch:true,detection_exempt:false,original:[],target:'America/Los_Angeles',
  action:'unchanged',turn_state_override:'applied',turn_state_original_length:0,turn_state_injected_length:292,
  turn_state_response_original_length:312,turn_state_response_injected_length:292,turn_state_response_status:'invalid-length',
  response_tokens_input:120,response_tokens_output:45,response_tokens_reasoning:30};
p.renderData({total:1,replaced:0,inserted:0,records:[record],turn_state_override:{enabled:true,probe:fixture()}});
let row=p.get('rows').children[0];
assert.match(row.textContent,/疑似降级档（312 字节）/);
assert.match(row.textContent,/Token 输入 120 · 输出 45 · 推理 30/);
// A response length inside the policy renders no degraded-tier badge.
record.turn_state_response_original_length=292;record.turn_state_response_status='updated-active';
p.renderData({total:1,replaced:0,inserted:0,records:[record],turn_state_override:{enabled:true,probe:fixture()}});
row=p.get('rows').children[0];
assert.doesNotMatch(row.textContent,/疑似降级档/);
// Missing token fields stay hidden instead of faking zeros.
record.response_tokens_input=null;record.response_tokens_output=null;record.response_tokens_reasoning=null;
p.renderData({total:1,replaced:0,inserted:0,records:[record],turn_state_override:{enabled:true,probe:fixture()}});
row=p.get('rows').children[0];
assert.doesNotMatch(row.textContent,/Token 输入/);
});

test('probe failure preserves healthy TTL and expired rows have no stray class text',()=>{
  const p=panel();p.render(fixture({failures:[{model:'gpt-6-astra',attempts:3}]}));
  let ttl=p.get('probe-values').children[0].children[5];
  assert.equal(ttl.className,'ttl-cell');
  assert.match(ttl.textContent,/现有基线继续使用/);
  assert.match(ttl.textContent,/剩余 3m 00s/);
  p.render(fixture({values:[{model:'gpt-6-astra',expired:true,remaining_seconds:-1,value_length:292}]}));
  ttl=p.get('probe-values').children[0].children[5];
  assert.equal(ttl.textContent,'已到期');
});

test('stop buttons send different actions and pause acts on participation',async()=>{
  const p=panel();p.render(fixture({running:true}));
  await p.get('round-stop').events.click();
  await p.get('all-stop').events.click();
  await p.get('probe-values').children[0].children[6].children[0].events.click();
  assert.deepEqual(Array.from(p.actions,item=>item.action),['stop-current','stop-all','pause']);
});

test('unchecked models show policy without stale failure, expiry or controls',()=>{
  const p=panel();
  const models=['gpt-5.6-luna','gpt-5.6-terra'];
  p.render(fixture({models,detection_models:[],prefetch_enabled:false,
    values:models.map(model=>({model,detection_enabled:false,value_length:312,expired:true,candidate:{remaining_seconds:120}})),
    paused:models,active_models:models,failures:models.map(model=>({model,attempts:99})),
    business:models.map(model=>({model,reason:'旧模型不一致'}))}));
  assert.equal(p.get('probe-status').textContent,'不检测');
  assert.equal(p.get('round-start').disabled,true);
  assert.equal(p.get('probe-priority').textContent,'—');
  assert.match(p.get('probe-times').textContent,/当前模型均不检测/);
  for(const row of p.get('probe-values').children){
    assert.equal(row.children.length,7);
    assert.equal(row.children[5].textContent,'不检测');
    assert.equal(row.children[6].children.length,0);
    assert.doesNotMatch(row.textContent,/失败|已到期|暂停|312|预备就绪/);
  }
});

test('unchecked request records retain raw observations without mismatch badges',()=>{
  const p=panel();
  p.renderData({total:1,replaced:0,inserted:0,records:[{
    time:new Date().toISOString(),model:'gpt-5.6-luna',upstream_model:'gpt-6-astra',
    detection_exempt:true,model_checked:true,model_mismatch:true,turn_state_length:312,action:'unchanged'
  }]});
  const row=p.get('rows').children[0];
  assert.match(row.children[2].textContent,/gpt-6-astra/);
  assert.match(row.children[2].textContent,/不检测/);
  assert.doesNotMatch(row.textContent,/模型不一致|降智已拦截/);
  assert.equal(row.children[6].className,'turnstate');
  assert.match(row.children[6].textContent,/312 字节/);
});

test('prefetch draft survives refresh and requires explicit valid confirmation',async()=>{
  const p=panel();p.render(fixture({ttl_minutes:55}));
  assert.equal(p.get('prefetch-minutes').value,'3');
  p.get('prefetch-minutes').value='10';p.get('prefetch-minutes').events.input();
  p.render(fixture({prefetch_minutes:5}));
  assert.equal(p.get('prefetch-minutes').value,'10');
  assert.equal(p.actions.length,0);
  for(const invalid of ['','-1','1.5','55','1e2']){
    p.get('prefetch-minutes').value=invalid;await p.get('prefetch-save').events.click();
    assert.equal(p.actions.length,0);
  }
  p.get('prefetch-minutes').value='10';await p.get('prefetch-save').events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.actions)),[{action:'prefetch-window',minutes:10}]);
  assert.match(p.get('prefetch-feedback').textContent,/保存失败/);
  assert.equal(p.get('prefetch-minutes').value,'10');
});

test('prefetch confirmation remains stable during polling and permits zero',async()=>{
  const p=panel();p.render(fixture({halted:true}));
  let finish;const pending=new Promise(resolve=>{finish=resolve;});p.setResult(pending);
  p.get('prefetch-minutes').value='0';p.get('prefetch-minutes').events.input();
  const saving=p.get('prefetch-save').events.click();
  p.render(fixture({halted:true,prefetch_minutes:3}));
  assert.equal(p.get('prefetch-minutes').value,'0');
  assert.equal(p.get('prefetch-save').disabled,true);
  finish(true);await saving;
  assert.match(p.get('prefetch-feedback').textContent,/已保存：0/);
  assert.equal(p.get('prefetch-save').disabled,false);
  assert.equal(p.actions[0].action,'prefetch-window');
  assert.equal(p.actions.length,1);
});

test('proxy editing retains authentication by omission and uses stable IDs',async()=>{
  const p=panel();
  const item={id:'exit-test',proxy:'socks5://192.0.2.1:1080',label:'备用出口',has_auth:true,pool:false,attempts:0,multiplier:1,budget:3,disabled:true};
  p.renderPool({proxies_state:[item]});
  const row=p.get('pool-rows').children[0];
  assert.match(row.children[0].children[2].textContent,/3 × 1 = 3/);
  await row.children[0].children[4].children[1].events.click();
  assert.equal(p.actions[0].id,'exit-test');assert.equal(p.actions[0].proxy,undefined);
  row.children[0].children[4].children[0].events.click();
  assert.equal(p.get('exit-password').value,'');assert.equal(p.get('exit-username').value,'');
  assert.match(p.get('exit-auth-note').textContent,/已配置认证/);
  p.get('exit-multiplier').value='2';await p.get('exit-save').events.click();
  const sent=p.actions[1];
  assert.equal(sent.action,'save-exit');assert.equal(sent.exit.id,'exit-test');
  assert.equal(sent.exit.multiplier,2);assert.equal(sent.exit.password,undefined);assert.equal(sent.exit.username,undefined);
  assert.match(p.get('exit-feedback').textContent,/保存失败/);
  assert.equal(p.get('exit-form').classList.contains('hidden'),false);
});

test('new pool is confirmed once and sensitive drafts are cleared after success',async()=>{
  const p=panel();p.get('pool-add').events.click();
  p.get('exit-label').value='聚合出口';p.get('exit-url').value='http://192.0.2.1:8080';
  p.get('exit-kind').value='pool';p.get('exit-attempts').value='100';p.get('exit-multiplier').value='2';
  p.get('exit-username').value='test-user';p.get('exit-password').value='TEST_PASSWORD_CANARY';
  assert.equal(p.actions.length,0);
  p.setResult(true);await p.get('exit-save').events.click();
  assert.equal(p.actions.length,1);assert.equal(p.actions[0].exit.pool,true);
  assert.equal(p.actions[0].exit.attempts,100);assert.equal(p.actions[0].exit.multiplier,2);
  assert.equal(p.get('exit-form').classList.contains('hidden'),true);
  assert.equal(p.get('exit-url').value,'');assert.equal(p.get('exit-password').value,'');
});

test('proxy form validates budgets and never sends retained credentials when clearing auth',async()=>{
  const p=panel();p.openExit({id:'exit-test',proxy:'http://192.0.2.1:8080',has_auth:true});
  for(const invalid of ['','0','-1','101','NaN']){
    p.get('exit-multiplier').value=invalid;await p.get('exit-save').events.click();
    assert.equal(p.actions.length,0);
  }
  p.get('exit-multiplier').value='0.5';p.get('exit-clear-auth').checked=true;
  p.get('exit-username').value='test-user';p.get('exit-password').value='TEST_PASSWORD_CANARY';
  await p.get('exit-save').events.click();
  assert.equal(p.actions[0].exit.clear_auth,true);assert.equal(p.actions[0].exit.password,undefined);
});

test('serial interval draft survives refresh validates bounds and saves once',async()=>{
  const p=panel();p.render(fixture({interval_seconds:2}));
  assert.equal(p.get('probe-interval-seconds').value,'2');
  assert.match(p.get('probe-interval-feedback').textContent,/当前生效：2 秒（配置默认）/);
  p.get('probe-interval-seconds').value='30';p.get('probe-interval-seconds').events.input();
  p.render(fixture({interval_seconds:5}));
  assert.equal(p.get('probe-interval-seconds').value,'30');
  assert.equal(p.actions.length,0);
  for(const invalid of ['','-1','0','3601','1.5','1e2']){
    p.get('probe-interval-seconds').value=invalid;await p.get('probe-interval-save').events.click();
    assert.equal(p.actions.length,0);
  }
  assert.match(p.get('probe-interval-feedback').textContent,/请输入 1–3600 的整数秒/);
  p.setResult(true);
  p.get('probe-interval-seconds').value='3600';await p.get('probe-interval-save').events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.actions)),[{action:'probe-interval',seconds:3600}]);
  assert.match(p.get('probe-interval-feedback').textContent,/已保存：3600 秒；下一次等待起生效/);
});

test('serial interval confirmation stays stable during polling and reports failure',async()=>{
  const p=panel();p.render(fixture());
  let finish;const pending=new Promise(resolve=>{finish=resolve;});p.setResult(pending);
  p.get('probe-interval-seconds').value='45';p.get('probe-interval-seconds').events.input();
  const saving=p.get('probe-interval-save').events.click();
  p.render(fixture({interval_seconds:2,interval_override:false}));
  assert.equal(p.get('probe-interval-seconds').value,'45');
  assert.equal(p.get('probe-interval-save').disabled,true);
  finish(false);await saving;
  assert.match(p.get('probe-interval-feedback').textContent,/保存失败，原设置未更改/);
  assert.equal(p.get('probe-interval-save').disabled,false);
  assert.equal(p.actions[0].action,'probe-interval');
  assert.equal(p.actions.length,1);
  p.setResult(true);
  p.get('probe-interval-save').events.click();
  assert.equal(p.get('probe-interval-seconds').value,'45');
});


test('embedded CPA theme follows white, unmarked paper and dark without OS overriding it',()=>{
  const tokens={'--bg-secondary':'#ffffff','--text-secondary':'#6d6760'};
  const p=panel({embedded:true,hostTheme:'white',darkSystem:true,hostTokens:tokens});
  assert.equal(p.root.getAttribute('data-theme'),'white');
  assert.equal(p.root.style['--bg'],'#ffffff');
  tokens['--bg-secondary']='#faf9f5';p.changeHost('');
  assert.equal(p.root.getAttribute('data-theme'),'light');
  assert.equal(p.root.style['--bg'],'#faf9f5');
  tokens['--bg-secondary']='#151412';p.changeHost('dark');
  assert.equal(p.root.getAttribute('data-theme'),'dark');
  assert.equal(p.root.style['--bg'],'#151412');
  p.changeSystem(false);assert.equal(p.root.getAttribute('data-theme'),'dark');
  delete tokens['--bg-secondary'];p.changeHost('white');
  assert.equal(p.root.style['--bg'],undefined);
});

test('standalone and inaccessible parent use CPA storage with live system fallback',()=>{
  for(const crossOrigin of [false,true]){
    const p=panel({crossOrigin,storedTheme:'white',darkSystem:true});
    assert.equal(p.root.getAttribute('data-theme'),'white');
    p.changeStored('light');assert.equal(p.root.getAttribute('data-theme'),'light');
    p.changeStored('dark');assert.equal(p.root.getAttribute('data-theme'),'dark');
    p.changeSystem(false);assert.equal(p.root.getAttribute('data-theme'),'dark');
    p.changeStored('auto');assert.equal(p.root.getAttribute('data-theme'),'white');
    p.changeSystem(true);assert.equal(p.root.getAttribute('data-theme'),'dark');
  }
});

test('collapsed settings summary shows effective values without replacing open drafts',()=>{
  const p=panel();p.render(fixture({prefetch_minutes:3,interval_seconds:2}));
  p.get('probe-settings').open=true;
  p.get('prefetch-minutes').value='12';p.get('prefetch-minutes').events.input();
  p.render(fixture({prefetch_minutes:5,interval_seconds:8}));
  assert.equal(p.get('probe-settings').open,true);
  assert.equal(p.get('prefetch-minutes').value,'12');
  assert.equal(p.get('settings-summary').textContent,'提前预备 5 分钟 · 串行间隔 8 秒 · 不休眠');
  p.render(fixture({settings_error:'设置不可读'}));
  assert.equal(p.get('settings-summary').textContent,'设置异常 · 展开查看');
  assert.equal(p.get('prefetch-save').disabled,true);
});

test('proxy list preserves long details, visible actions and disabled versus cooling counts',async()=>{
  const p=panel();const error='出口连接失败 '.repeat(40);
  p.renderPool({pool_total:3,pool_active:1,pool_disabled:1,proxies_state:[
    {id:'disabled',proxy:'http://192.0.2.1:8080',disabled:true,active:false,last_error:error},
    {id:'cooling',proxy:'http://192.0.2.2:8080',active:false,rest:true,remaining_seconds:300,until:new Date().toISOString()},
    {id:'ready',proxy:'direct',active:true}
  ]});
  assert.equal(p.get('pool-cooling').textContent,'1');
  const first=p.get('pool-rows').children[0];
  assert.match(first.textContent,new RegExp(error));
  const ops=first.children[0].children[4];
  assert.deepEqual(ops.children.map(button=>button.textContent),['编辑','启用','重置']);
  ops.children[0].events.click();assert.equal(p.get('exit-label').focused,true);
  await ops.children[2].events.click();
  assert.equal(p.actions[0].action,'reset-exit');assert.equal(p.actions[0].id,'disabled');
  assert.match(p.get('pool-rows').children[1].textContent,/轮休至.*剩余 5m 00s/);
});

test('all CPA locales translate static text, states and validation while retaining drafts',async()=>{
  const p=panel({embedded:true,hostLanguage:'zh-CN',storedLanguage:'en'});
  const data={total:1,replaced:0,inserted:0,records:[],turn_state_override:{enabled:true,probe:fixture({interval_seconds:2})}};
  p.renderData(data);
  const heading=p.staticNodes.find(node=>node.getAttribute('data-i18n')==='探测控制台');
  assert.equal(heading.textContent,'Probe console');
  p.get('probe-settings').open=true;
  p.get('prefetch-minutes').value='12';p.get('prefetch-minutes').events.input();
  p.openExit({id:'named',proxy:'http://192.0.2.1:8080',label:'用户命名',has_auth:true});
  p.get('exit-label').value='未保存的名称';p.get('exit-password').value='TEST_DRAFT_CANARY';
  p.changeLanguage('ru');
  assert.equal(p.root.getAttribute('lang'),'ru');
  assert.equal(heading.textContent,'Консоль проверок');
  assert.equal(p.get('probe-status').textContent,'Ожидание автоподготовки');
  assert.equal(p.get('prefetch-feedback').textContent,'Есть несохранённые изменения');
  assert.equal(p.get('probe-settings').open,true);
  assert.equal(p.get('prefetch-minutes').value,'12');
  assert.equal(p.get('exit-label').value,'未保存的名称');
  assert.equal(p.get('exit-password').value,'TEST_DRAFT_CANARY');
  assert.equal(p.get('exit-form-title').textContent,'Изменить выход');
  p.changeLanguage('zh-TW');
  assert.equal(heading.textContent,'探測控制台');
  p.get('probe-interval-seconds').value='0';p.get('probe-interval-seconds').events.input();await p.get('probe-interval-save').events.click();
  assert.equal(p.get('probe-interval-feedback').textContent,'請輸入 1–3600 的整數秒');
  p.changeLanguage('en');
  assert.equal(p.get('probe-interval-feedback').textContent,'Enter whole seconds from 1 to 3600');
  assert.equal(p.actions.length,0);
  p.changeLanguage('zh-CN');assert.equal(heading.textContent,'探测控制台');
});

test('locale fallback accepts CPA storage formats and handles unavailable parent or storage',()=>{
  for(const rawLanguage of ['en','"en"','{"language":"en"}','{"state":{"language":"en"}}']){
    assert.equal(panel({rawLanguage,hostLanguage:'zh-CN',embedded:true}).root.getAttribute('lang'),'en');
  }
  const host=panel({embedded:true,hostLanguage:'ru',browserLanguage:'en'});
  assert.equal(host.root.getAttribute('lang'),'ru');
  host.changeHostLanguage('zh-TW');assert.equal(host.root.getAttribute('lang'),'zh-TW');
  assert.equal(panel({crossOrigin:true,storedLanguage:'ru'}).root.getAttribute('lang'),'ru');
  assert.equal(panel({storageUnavailable:true,crossOrigin:true,browserLanguage:'zh-HK'}).root.getAttribute('lang'),'zh-TW');
  assert.equal(panel({rawLanguage:'not-json',browserLanguage:'de-DE'}).root.getAttribute('lang'),'en');
});

test('language changes during a pending save preserve its payload and translate completion',async()=>{
  const p=panel();p.renderData({total:0,replaced:0,inserted:0,records:[],turn_state_override:{probe:fixture()}});
  let finish;p.setResult(new Promise(resolve=>{finish=resolve;}));
  p.get('probe-interval-seconds').value='18';p.get('probe-interval-seconds').events.input();
  const saving=p.get('probe-interval-save').events.click();
  p.changeLanguage('en');
  assert.equal(p.get('probe-interval-seconds').value,'18');
  assert.equal(p.get('probe-interval-save').disabled,true);
  finish(true);await saving;
  assert.match(p.get('probe-interval-feedback').textContent,/Saved: 18 seconds/);
  assert.deepEqual(JSON.parse(JSON.stringify(p.actions)),[{action:'probe-interval',seconds:18}]);
});

test('translation catalogs cover static UI and retain every interpolation parameter',()=>{
  const p=panel();
  for(const node of p.staticNodes){
    for(const attr of ['data-i18n','data-i18n-title','data-i18n-placeholder','data-i18n-aria-label']){
      const key=node.getAttribute(attr);if(key!==null)assert.ok(Object.hasOwn(p.messages,key),key);
    }
  }
  for(const [key,translations] of Object.entries(p.messages)){
    assert.equal(translations.length,3,key);
    const params=Array.from(key.matchAll(/\{(\w+)\}/g),match=>match[1]).sort();
    for(const translation of translations){
      assert.ok(translation,key);
      assert.deepEqual(Array.from(translation.matchAll(/\{(\w+)\}/g),match=>match[1]).sort(),params,key);
    }
  }
  p.changeLanguage('en');
  assert.equal(p.translate('最近错误：{error}',{error:'raw {count} <script>'}),'Last error: raw {count} <script>');
});

test('probe history uses supplied exit names as text and keeps safe address fallback',()=>{
  const p=panel({storedLanguage:'en'});
  p.render(fixture({history:[
    {proxy:'socks5h://192.0.2.1:1080',proxy_label:'自定义出口 <img src=x>',egress_addr:'2001:db8::1',success:true,state_length:292},
    {proxy:'socks5h://192.0.2.1:1080',proxy_label:'另一个账号',success:false},
    {proxy:'direct',success:true},
    {proxy:'http://test-user:test-password@192.0.2.2:8080',success:false}
  ]}));
  const rows=p.get('probe-history').children;
  assert.match(rows[0].children[2].textContent,/自定义出口 <img src=x>/);
  assert.equal(rows[0].children[2].title,'socks5h://192.0.2.1:1080');
  assert.equal(rows[0].children.length,6);
  assert.doesNotMatch(JSON.stringify(rows),/2001:db8::1/);
  assert.equal(rows[1].children[2].textContent,'另一个账号');
  assert.equal(rows[2].children[2].textContent,'Direct');
  assert.equal(rows[3].children[2].textContent,'http://192.0.2.2:8080');
  assert.doesNotMatch(rows[3].children[2].title,/test-password/);
});

test('switching language after sign-out does not restore previously rendered requests',async()=>{
  const p=panel();
  p.renderData({total:42,replaced:0,inserted:0,records:[],turn_state_override:{probe:fixture()}});
  // The test has no stored login. Refresh enters the real login-required path.
  await p.get('refresh').events.click();
  p.changeLanguage('en');
  assert.equal(p.get('total').textContent,'—');
  assert.equal(p.get('auth-notice').classList.contains('hidden'),false);
  assert.equal(p.get('results').classList.contains('hidden'),true);
  assert.match(p.get('auth-message').textContent,/No saved CPA session/);
});

test('successful probes use independent history and respect its explicit empty state',()=>{
  const p=panel();
  const success={model:'saved-success',proxy:'direct',proxy_label:'用户名称',success:true,state_length:292};
  const history=Array.from({length:60},(_,i)=>({model:'failure-'+i,success:false}));
  p.render(fixture({history,success_history:[success]}));
  assert.equal(p.get('probe-history').children.length,50);
  assert.equal(p.get('probe-success-history').children.length,1);
  assert.match(p.get('probe-success-history').textContent,/saved-success.*用户名称/);
  p.render(fixture({history:[success],success_history:[]}));
  assert.equal(p.get('probe-success-history').textContent,'暂无成功记录。');
  p.render(fixture({history:[...history,success]}));
  assert.match(p.get('probe-success-history').textContent,/saved-success/);
  p.render(fixture({success_history:[{success:false},...Array.from({length:60},(_,i)=>({...success,model:'success-'+i}))]}));
  assert.equal(p.get('probe-success-history').children.length,50);
  assert.match(p.get('probe-success-history').children[49].textContent,/success-49/);
});

test('recent requests display the newest 50 without truncating source data or statistics',()=>{
  const p=panel();
  const records=Array.from({length:65},(_,i)=>({time:new Date(1700000000000-i*1000).toISOString(),model:'request-'+i,original:[],turn_state_length:292}));
  const data={total:1000,records,turn_state_override:{probe:fixture()}};
  p.renderData(data);
  const details=p.get('request-history-panel');
  for(const open of [true,false]){
    details.open=open;
    for(const language of ['zh-CN','en','zh-TW','ru']){
      p.changeLanguage(language);p.renderData(data);
      assert.equal(details.open,open);
      const rows=p.get('rows').children;
      assert.equal(rows.length,50);
      assert.equal(rows[0].children[1].textContent,'request-0');
      assert.equal(rows[49].children[1].textContent,'request-49');
      assert.equal(p.get('turnstates').textContent,'65');
      assert.equal(records.length,65);
    }
  }
  p.renderData({...data,records:[]});
  assert.equal(p.get('rows').children[0].children[0].colSpan,8);
});

function performanceFixture(overrides={}){
  return {time:'2026-10-03T00:00:00Z',model:'gpt-6-astra',original:[],request_outcome:'succeeded',response_tokens_complete:true,
    response_tokens_output:1420,response_tokens_reasoning:1034,request_latency_ms:75120,first_output_latency_ms:63460,output_tps:1420*1000/75120,...overrides};
}
function performancePanel(record){const p=panel();p.renderData({total:1,records:[record],turn_state_override:{probe:fixture()}});return p;}

test('request performance shows full-output TPS and names first-output versus total latency',()=>{
  const p=performancePanel(performanceFixture()),column=p.get('rows').children[0].children[7];
  assert.equal(column.children[0].textContent,'18.9 t/s');
  assert.equal(column.children[1].textContent,'首输出 63.46 s · 总耗时 75.12 s');
  assert.match(column.children[0].title,/含推理.*总耗时/);assert.match(column.children[1].title,/不是宿主 TTFT/);
  assert.doesNotMatch(column.className,/warn|danger/);
});

test('request performance never fabricates old unknown pending or nonstream first latency',()=>{
  for(const record of [{time:'2026-10-03T00:00:00Z',model:'legacy',original:[]},performanceFixture({request_outcome:'',output_tps:undefined,request_latency_ms:undefined,first_output_latency_ms:undefined}),performanceFixture({response_tokens_complete:false,output_tps:undefined})]){
    const column=performancePanel(record).get('rows').children[0].children[7];assert.equal(column.children[0].textContent,'—');
  }
  const column=performancePanel(performanceFixture({first_output_latency_ms:undefined})).get('rows').children[0].children[7];
  assert.equal(column.children[1].textContent,'首输出 — · 总耗时 75.12 s');
});

test('request performance preserves measured zero and rejects invalid numbers',()=>{
  let column=performancePanel(performanceFixture({output_tps:0,request_latency_ms:.5,first_output_latency_ms:0})).get('rows').children[0].children[7];
  assert.equal(column.children[0].textContent,'0.0 t/s');assert.equal(column.children[1].textContent,'首输出 0.00 ms · 总耗时 0.50 ms');
  for(const invalid of [-1,NaN,Infinity,'12',null,undefined]){
    column=performancePanel(performanceFixture({output_tps:invalid,request_latency_ms:invalid,first_output_latency_ms:invalid})).get('rows').children[0].children[7];
    assert.equal(column.children[0].textContent,'—');assert.equal(column.children[1].textContent,'首输出 — · 总耗时 —');
  }
});

test('request performance localizes failure without displaying raw error text',()=>{
  for(const [outcome,label] of [['failed','请求已失败'],['rejected','请求已拒绝'],['canceled','请求已取消']]){
    const p=performancePanel(performanceFixture({request_outcome:outcome,error:'PRIVATE_RAW_ERROR'}));
    const column=p.get('rows').children[0].children[7];assert.equal(column.children[0].textContent,'—');assert.equal(column.children[2].textContent,label);assert.doesNotMatch(column.textContent,/PRIVATE_RAW_ERROR/);
    for(const language of ['en','zh-TW','ru']){p.changeLanguage(language);const value=p.get('rows').children[0].children[7].children[2].textContent;assert.equal(value,p.translate(label));}
  }
});

test('request performance labels and tooltips follow all CPA locales without changing numeric values',()=>{
  const p=performancePanel(performanceFixture());
  for(const language of ['en','zh-TW','ru','zh-CN']){
    p.changeLanguage(language);const column=p.get('rows').children[0].children[7];assert.equal(column.children[0].textContent,'18.9 t/s');
    assert.equal(column.children[1].textContent,p.translate('首输出 {first} · 总耗时 {total}',{first:'63.46 s',total:'75.12 s'}));
    assert.equal(column.children[1].title,p.translate('首输出延迟由插件实际收到的文本、推理或工具参数增量计时；不是宿主 TTFT。总耗时来自 CPA 完成回调起止时间，包含路由和重试；缺失数据不补造。'));
  }
});

test('history panels preserve independent open states through refresh and all locales',()=>{
  const p=panel();
  const data={total:0,records:[],turn_state_override:{probe:fixture()}};
  p.renderData(data);
  p.get('probe-history-panel').open=true;
  p.get('probe-success-history-panel').open=false;
  const empty=['No successful probes yet.','尚無成功記錄。','Успешных проверок пока нет.','暂无成功记录。'];
  for(const [i,language] of ['en','zh-TW','ru','zh-CN'].entries()){
    p.changeLanguage(language);p.renderData(data);
    assert.equal(p.get('probe-success-history').textContent,empty[i]);
    assert.equal(p.get('probe-history-panel').open,true);
    assert.equal(p.get('probe-success-history-panel').open,false);
  }
  p.get('probe-history-panel').open=false;p.get('probe-success-history-panel').open=true;
  p.renderData(data);
  assert.equal(p.get('probe-history-panel').open,false);
  assert.equal(p.get('probe-success-history-panel').open,true);
});


test('both histories omit reported addresses and visibility controls in every language',()=>{
  const p=panel();
  const record={proxy:'direct',proxy_label:'自定义出口',egress_addr:'203.0.113.42',success:true,state_length:292};
  const data={records:[],turn_state_override:{probe:fixture({history:[record],success_history:[record]})}};
  p.renderData(data);
  for(const locale of ['zh-CN','en','zh-TW','ru']){
    p.changeLanguage(locale);p.renderData(data);
    for(const id of ['probe-history','probe-success-history']){
      const row=p.get(id).children[0];
      assert.equal(row.children.length,6);
      assert.equal(row.children[2].textContent,'自定义出口');
      assert.doesNotMatch(JSON.stringify(row),/203\.0\.113\.42/);
    }
  }
  assert.doesNotMatch(fs.readFileSync(__dirname+'/index.html','utf8'),/egressVisible|ip-toggle|egress-ip|代理回报地址/);
  p.render(fixture());
  for(const id of ['probe-history','probe-success-history'])assert.equal(p.get(id).children[0].children[0].colSpan,6);
});

test('pool shows independent cumulative counts and a localized counting start',()=>{
  const p=panel();
  const probe=fixture({exit_success_since:'2026-09-20T01:00:00Z',proxies_state:[
    {id:'a',proxy:'direct',success_count:'0'},
    {id:'b',proxy:'http://192.0.2.1:80',success_count:'9007199254740993',disabled:true},
    {id:'legacy',proxy:'http://192.0.2.2:80'}
  ],success_history:[]});
  const data={records:[],turn_state_override:{probe}};
  p.renderData(data);
  for(const [locale,label] of [['zh-CN','累计成功'],['en','Cumulative successes'],['zh-TW','累計成功'],['ru','Успешных проверок']]){
    p.changeLanguage(locale);p.renderData(data);
    const rows=p.get('pool-rows').children;
    assert.equal(rows[0].children[0].children[3].children[0].textContent,label);
    assert.match(rows[0].children[0].children[3].textContent,/0/);
    assert.equal(rows[0].children[0].children[3].children[1].className,'badge state-ok');
    assert.match(rows[1].children[0].children[3].textContent,/9007199254740993/);
    assert.equal(rows[1].children[0].children[3].children[1].className,'badge state-ok');
    assert.equal(rows[2].children[0].children[3].children[1].textContent,'—');
    assert.equal(rows[2].children[0].children[3].children[1].className,'badge state-off');
    assert.match(p.get('pool-note').textContent,/09:00:00/);
    assert.match(p.get('pool-note').textContent,/2026/);
    assert.equal(rows[0].children[0].children[4].children.length,3);
  }
});

test('sleep settings preserve drafts, validate hours and keep failed saves editable',async()=>{
  const p=panel();p.render(fixture());
  assert.equal(p.get('sleep-start-hour').value,'0');
  assert.equal(p.get('sleep-end-hour').value,'0');
  p.get('sleep-start-hour').value='23';p.get('sleep-start-hour').events.input();
  p.get('sleep-end-hour').value='7';p.get('sleep-end-hour').events.input();
  p.render(fixture({sleep_start_hour:2,sleep_end_hour:8}));
  assert.equal(p.get('sleep-start-hour').value,'23');
  assert.equal(p.get('sleep-end-hour').value,'7');
  for(const value of ['','-1','24','2.5','abc']){
    p.get('sleep-start-hour').value=value;await p.get('sleep-save').events.click();
    assert.equal(p.actions.length,0);
  }
  p.get('sleep-start-hour').value='23';await p.get('sleep-save').events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.actions)),[{action:'sleep-hours',start_hour:23,end_hour:7}]);
  assert.match(p.get('sleep-feedback').textContent,/保存失败/);
  p.render(fixture());assert.equal(p.get('sleep-start-hour').value,'23');
});

test('sleep save is single flight, follows language changes and permits disabling',async()=>{
  const p=panel(),data={records:[],turn_state_override:{probe:fixture()}};
  p.renderData(data);p.get('sleep-start-hour').value='2';p.get('sleep-end-hour').value='7';p.get('sleep-start-hour').events.input();
  let finish;p.setResult(new Promise(resolve=>{finish=resolve;}));
  const saving=p.get('sleep-save').events.click();
  await p.get('sleep-save').events.click();p.renderData(data);p.changeLanguage('en');
  assert.equal(p.actions.length,1);assert.equal(p.get('sleep-save').disabled,true);
  assert.equal(p.get('sleep-start-hour').value,'2');
  finish(true);await saving;
  assert.equal(p.get('sleep-feedback').textContent,'Sleep schedule saved');
  p.get('sleep-start-hour').value='0';p.get('sleep-end-hour').value='0';p.setResult(true);
  await p.get('sleep-save').events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.actions[1])),{action:'sleep-hours',start_hour:0,end_hour:0});
});

test('sleep overrides probe activity while preserving baseline and explicit stopped modes',()=>{
  const p=panel();
  const sleeping=fixture({sleeping:true,sleep_start_hour:2,sleep_end_hour:7,sleep_until:'2026-01-01T23:00:00Z',running:true,active_models:['gpt-6-astra']});
  p.render(sleeping);
  assert.equal(p.get('probe-status').textContent,'休眠中');
  assert.match(p.get('settings-summary').textContent,/02:00～07:00/);
  assert.match(p.get('probe-times').textContent,/在途请求完成后等待/);
  const row=p.get('probe-values').children[0];
  assert.match(row.children[0].textContent,/休眠中/);
  assert.match(row.children[5].textContent,/剩余 3m 00s/);
  assert.doesNotMatch(row.children[5].textContent,/探测中/);
  assert.equal(row.children[6].children[1].disabled,true);
  p.render({...sleeping,running:false,active_models:[],halted:true,paused:['gpt-6-astra']});
  assert.equal(p.get('probe-status').textContent,'全部停止');
  assert.match(p.get('probe-values').children[0].children[0].textContent,/已暂停/);
  p.render({...sleeping,sleeping:false,running:false,active_models:[],prefetch_minutes:0});
  assert.equal(p.get('probe-status').textContent,'手动待命');
});

test('proxy list retains management actions without independent public sampling',()=>{
  const p=panel();p.renderPool({proxies_state:[{id:'direct',proxy:'direct',active:true}]});
  const row=p.get('pool-rows').children[0];
  assert.equal(row.children.length,2);
  assert.match(row.textContent,/编辑/);assert.match(row.textContent,/禁用/);assert.match(row.textContent,/重置/);
  assert.doesNotMatch(row.textContent,/公网|采样|ipify/);
  assert.equal(p.actions.length,0);
});


// Synthetic login fixtures only. Format matches the public CPA/Manager Plus
// secureStorage v1/v2 encoders; never read a real browser profile or credential.
{
  const host='review.example:8317',origin='https://'+host;
  function encodeStoredFixture(version,value,ua='review-ua') {
    const salt='cli-proxy-api-webui::secure-storage';
    const mask=Buffer.from(version==='v1'?`${salt}|${host}|${ua}`:`${salt}|v2|${host}`);
    const bytes=Buffer.from(value);
    return `enc::${version}::`+Buffer.from(bytes.map((v,i)=>v^mask[i%mask.length])).toString('base64');
  }
  function storagePanel(values,ua='review-ua',prefix='') {
    const html=fs.readFileSync(__dirname+'/index.html','utf8');
    const source=html.slice(html.indexOf('  function storedValue('),html.indexOf('  function requireLogin('));
    const c={localStorage:{getItem:n=>values[n]??null},location:{host,origin},navigator:{userAgent:ua},prefix,URL,TextEncoder,TextDecoder,atob};
    vm.createContext(c);vm.runInContext(source+'\nglobalThis.read=storedValue;globalThis.key=readManagementKey;',c);
    return c;
  }
  for(const version of ['v1','v2'])test(`${version} storage decodes a persisted login object`,()=>{
    const value={state:{managementKey:'synthetic-review-key',apiBase:origin}};
    const p=storagePanel({v:encodeStoredFixture(version,JSON.stringify(value))});
    assert.equal(JSON.stringify(p.read('v')),JSON.stringify(value));
  });
  test('v2 storage supports Unicode',()=>assert.equal(storagePanel({v:encodeStoredFixture('v2',JSON.stringify('简体中文 / русский'))}).read('v'),'简体中文 / русский'));
  test('v2 storage survives a changed user agent',()=>assert.equal(storagePanel({v:encodeStoredFixture('v2',JSON.stringify('synthetic-review-key'),'ua-before')},'ua-after').read('v'),'synthetic-review-key'));
  test('plaintext JSON login storage remains readable',()=>assert.equal(storagePanel({v:'"synthetic-review-key"'}).read('v'),'synthetic-review-key'));
  test('legacy raw login storage remains readable',()=>assert.equal(storagePanel({v:'legacy-review-value'}).read('v'),'legacy-review-value'));
  test('missing login storage returns null',()=>assert.equal(storagePanel({}).read('v'),null));
  function login(apiBase=origin){return {isLoggedIn:'true','cli-proxy-auth':encodeStoredFixture('v2',JSON.stringify({state:{managementKey:'synthetic-review-key',apiBase}}))};}
  test('same-origin v2 session reuses login',()=>assert.equal(storagePanel(login()).key(),'synthetic-review-key'));
  test('foreign origin cannot reuse v2 login',()=>assert.equal(storagePanel(login('https://other.example')).key(),''));
  test('foreign base path cannot reuse v2 login',()=>assert.equal(storagePanel(login(origin+'/other')).key(),''));
  test('malformed v2 login fails closed',()=>assert.equal(storagePanel({isLoggedIn:'true','cli-proxy-auth':'enc::v2::!!!'}).key(),''));
  test('logged-out session cannot reuse v2 login',()=>assert.equal(storagePanel({...login(),isLoggedIn:'false'}).key(),''));
}

test('target timezone follows API changes and remains literal text',()=>{
  const p=panel();
  const data={records:[],turn_state_override:{probe:fixture()}};
  p.renderData({...data,target:'Asia/Singapore'});
  assert.equal(p.get('target-timezone').textContent,'Asia/Singapore');
  p.changeLanguage('en');
  assert.equal(p.get('target-timezone').textContent,'Asia/Singapore');
  p.renderData({...data,target:'America/New_York'});
  assert.equal(p.get('target-timezone').textContent,'America/New_York');
  p.renderData({...data,target:'<img src=x onerror=alert(1)>'});
  assert.equal(p.get('target-timezone').children.length,0);
  p.renderData(data);assert.equal(p.get('target-timezone').textContent,'—');
});


test('configured lengths drive baseline and history badges',()=>{
  const p=panel();
  const probe=fixture({accepted_state_lengths:[308],values:[{model:'gpt-6-astra',valid:true,value_length:308,remaining_seconds:180}],
    history:[{state_length:308,success:true},{state_length:292,success:true}]});
  p.render(probe);
  assert.equal(p.get('probe-values').children[0].children[1].children[0].className,'badge state-ok');
  const rows=p.get('probe-history').children;
  assert.equal(rows[0].children[5].children[0].className,'badge state-ok');
  assert.equal(rows[1].children[5].children[0].className,'badge state-warn');
  assert.match(p.get('probe-values').textContent,/剩余/);
});

test('same-model account rows retain separate activity, evidence and actions',async()=>{
  const p=panel();p.setResult(false);
  const value=fixture().values[0];
  p.render(fixture({account_binding_ready:true,values:[
    {...value,target:'account-A/astra',account:'auth-fixture-A',account_available:true},
    {...value,target:'account-B/astra',account:'auth-fixture-B',account_available:true}
  ],paused:['account-A/astra'],business:[{model:'account-A/astra',reason:'fixture-only'}]}));
  const [a,b]=p.get('probe-values').children.filter(row=>row.classList.contains('account-detail'));
  assert.match(a.children[0].textContent,/\*\*\*/);
  assert.doesNotMatch(b.children[0].textContent,/auth-fixture-B/);
  assert.equal(a.classList.contains('paused-row'),true);
  assert.equal(b.classList.contains('paused-row'),false);
  assert.match(a.textContent,/业务确认/);
  assert.doesNotMatch(b.textContent,/业务确认/);
  await b.children[6].children[1].events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.actions.pop())),{model:value.model,target:'account-B/astra',action:'probe-model'});
  await a.children[6].children[0].events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.actions.pop())),{model:value.model,target:'account-A/astra',action:'resume'});
});

test('missing account binding and removed account rows cannot start probes',()=>{
  const p=panel();
  p.render(fixture({account_binding_ready:false}));
  assert.equal(p.get('round-start').disabled,true);
  assert.match(p.get('probe-times').textContent,/账号绑定不可用/);
  p.render(fixture({account_binding_ready:true,values:[{...fixture().values[0],target:'removed/astra',account:'removed',account_available:false}]}));
  assert.equal(p.get('probe-values').children.find(row=>row.classList.contains('account-detail')).children[6].children[1].disabled,true);
});

test('model groups aggregate availability and keep independent expansion across refresh and language',()=>{
  const p=panel(),base=fixture().values[0];
  const values=[
    {...base,model:'astra',target:'A/astra',account:'auth-A',account_email:'a@example.test',account_available:true},
    {...base,model:'astra',target:'B/astra',account:'auth-B',account_email:'b@example.test',valid:false,account_available:true},
    {...base,model:'sol',target:'A/sol',account:'auth-A',valid:false,account_available:true}
  ];
  const probe=fixture({models:['astra','sol'],values});
  p.render(probe);
  let rows=p.get('probe-values').children;
  assert.equal(rows[0].children[0].children[0].children[1].children[0].textContent,'可用');
  assert.equal(rows[3].children[0].children[0].children[1].children[0].textContent,'不可用');
  assert.equal(rows[1].classList.contains('hidden'),true);
  rows[0].children[0].children[0].children[0].events.click();
  p.render(probe);
  rows=p.get('probe-values').children;
  assert.equal(rows[1].classList.contains('hidden'),false);
  assert.equal(rows[4].classList.contains('hidden'),true);
  assert.equal(rows[0].children[0].children[0].children[0].getAttribute('aria-expanded'),'true');
  p.renderData({records:[],turn_state_override:{probe}});p.changeLanguage('en');
  rows=p.get('probe-values').children;
  assert.equal(rows[0].children[0].children[0].children[1].children[0].textContent,'Available');
  assert.equal(rows[1].classList.contains('hidden'),false);
  p.render({...probe,values:values.map(e=>({...e,remaining_seconds:-1,expired:true}))});
  assert.equal(p.get('probe-values').children[0].children[0].children[0].children[1].children[0].textContent,'Unavailable');
});

test('email visibility applies to baseline and history without exposing auth hashes',()=>{
  const p=panel(),entry={...fixture().values[0],target:'A/astra',account:'auth-private',account_email:'a@example.test'};
  const data={records:[{time:new Date().toISOString(),model:'gpt-6-astra',account:'auth-private',account_email:'a@example.test'}],turn_state_override:{probe:fixture({values:[entry],history:[{model:'gpt-6-astra',account:'auth-private',account_email:'a@example.test',success:true,state_length:292}]})}};
  p.renderData(data);
  assert.equal(p.get('account-email-toggle').textContent,'邮箱显示：已关闭');
  assert.equal(p.get('account-email-toggle').classList.contains('on'),false);
  for(const id of ['probe-values','rows','probe-history']){assert.doesNotMatch(p.get(id).textContent,/a@example|auth-private/);assert.match(p.get(id).textContent,/\*\*\*/);}
  p.get('account-email-toggle').events.click();
  assert.equal(p.get('account-email-toggle').textContent,'邮箱显示：已开启');
  assert.equal(p.get('account-email-toggle').classList.contains('on'),true);
  for(const id of ['probe-values','rows','probe-history'])assert.match(p.get(id).textContent,/a@example\.test/);
  p.changeLanguage('ru');p.renderData(data);
  assert.match(p.get('probe-values').textContent,/a@example\.test/);
  p.get('account-email-toggle').events.click();
  assert.doesNotMatch(p.get('probe-values').textContent,/a@example/);
  assert.equal(p.get('account-email-toggle').getAttribute('aria-pressed'),'false');
  const html=fs.readFileSync(__dirname+'/index.html','utf8');
  assert.doesNotMatch(html,/id="account-routing-panel"|id="cooldown-toggle"/);
});

test('collapsed model shows one newest ticket with its own account metadata',()=>{
  const p=panel(),base=fixture().values[0];
  const older=new Date(Date.now()-600000).toISOString(),newer=new Date(Date.now()-60000).toISOString();
  const entries=[{...base,issued_at:older,target:'A/astra',account:'a',account_email:'a@example.test'},
    {...base,issued_at:older,target:'B/astra',account:'b',account_email:'b@example.test',candidate:{value_length:332,issued_at:newer,expires_at:base.expires_at,remaining_seconds:200,source:'probe'}}];
  const data={records:[],turn_state_override:{probe:fixture({values:entries})}};
  p.renderData(data);
  const row=p.get('probe-values').children[0];
  assert.equal(row.children.length,7);
  assert.equal(row.children[1].textContent,'332 字节');
  assert.match(row.children[2].textContent,/最新票据 · 预备/);
  assert.doesNotMatch(JSON.stringify(row),/b@example/);
  assert.equal(p.get('probe-values').children[1].classList.contains('hidden'),true);
  p.get('account-email-toggle').events.click();
  assert.match(p.get('probe-values').children[0].children[2].textContent,/b@example/);
  assert.doesNotMatch(p.get('probe-values').children[0].children[2].textContent,/a@example/);
});

test('missing request headers show only injection while known lengths retain comparison',()=>{
  const p=panel();
  const records=[356,0,undefined].map(turn_state_original_length=>({time:new Date().toISOString(),turn_state_original_length,turn_state_length:332,turn_state_injected_length:292,turn_state_override:'applied'}));
  p.renderData({records,turn_state_override:{probe:fixture()}});
  const rows=p.get('rows').children;
  assert.match(rows[0].textContent,/请求：收到 356 字节 → 注入 292 字节/);
  assert.match(rows[1].textContent,/注入 292 字节/);
  assert.doesNotMatch(rows[1].textContent,/请求：|未携带|→/);
  assert.match(rows[2].textContent,/原始长度未知 → 注入 292 字节/);
  assert.doesNotMatch(rows[0].textContent,/332/);
  p.changeLanguage('en');assert.match(p.get('rows').children[0].textContent,/Request: Received 356 bytes → Injected 292 bytes/);
});

test('request and response injection lengths remain separate in all locales',()=>{
  const p=panel(),base={time:new Date().toISOString(),turn_state_original_length:0,turn_state_injected_length:292,turn_state_override:'applied'};
  const records=[{...base,turn_state_response_original_length:312,turn_state_response_injected_length:292,turn_state_length:292,turn_state_source:'stream'},
    {...base,turn_state_length:332,turn_state_source:'stream'}, {...base,turn_state_length:356,turn_state_source:'request'}];
  p.renderData({records,turn_state_override:{probe:fixture()}});
  for(const lang of ['zh-CN','en','zh-TW','ru']){
    p.changeLanguage(lang);
    const rows=p.get('rows').children;
    assert.equal((rows[0].textContent.match(/→/g)||[]).length,1);
    assert.match(rows[0].textContent,/312/);
    assert.equal((rows[1].textContent.match(/→/g)||[]).length,0);
    assert.match(rows[1].textContent,/332/);
    assert.doesNotMatch(rows[2].textContent,/356/);
  }
  p.changeLanguage('zh-CN');
  assert.match(p.get('rows').children[0].textContent,/响应：收到 312 字节 → 注入 292 字节/);
  assert.match(p.get('rows').children[2].textContent,/响应：未观测/);
  assert.doesNotMatch(p.get('rows').textContent,/回灌/);
});

test('missing request tickets are not labelled overwritten and response evidence remains visible',()=>{
  const p=panel();
  const records=[0,312].map(turn_state_length=>({time:new Date().toISOString(),turn_state_original_length:0,turn_state_override:'skipped-missing',turn_state_length,turn_state_source:turn_state_length?'response':''}));
  const data={records,turn_state_override:{probe:fixture()}};
  p.renderData(data);
  for(const [locale,label] of [['zh-CN','请求未携带票据，未覆写'],['en','No request ticket; not overwritten'],['zh-TW','請求未攜帶票據，未覆寫'],['ru','В запросе нет билета; без перезаписи']]){
    p.changeLanguage(locale);p.renderData(data);
    const rows=p.get('rows').children;
    for(const row of rows){assert.ok(row.textContent.includes(label));assert.doesNotMatch(row.textContent,/→/);}
    assert.match(rows[1].textContent,/312/);
  }
});

test('response learning reports actual outcomes without inventing updates for legacy records',()=>{
  const p=panel(),base={time:new Date().toISOString(),turn_state_original_length:0,turn_state_injected_length:292,turn_state_override:'applied'};
  const states=['missing','same-active','same-candidate','updated-active','updated-candidate','awaiting-model','older','expired','invalid-length','model-mismatch','account-unavailable','exempt'];
  const records=states.map(turn_state_response_status=>({...base,turn_state_response_status,turn_state_response_original_length:turn_state_response_status==='missing'?undefined:332}));
  records.push({...base});
  records.push({...base,turn_state_response_status:'missing',turn_state_source:'stream',turn_state_length:332});
  records.push({...base,turn_state_response_status:'headers-unavailable'});
  p.renderData({records,turn_state_override:{probe:fixture()}});
  for(const lang of ['zh-CN','en','zh-TW','ru']){
    p.changeLanguage(lang);
    const rows=p.get('rows').children;
    for(let i=0;i<states.length;i++){
      assert.doesNotMatch(rows[i].textContent,/请求：未携带|undefined|回灌/);
      if(lang!=='zh-CN')assert.doesNotMatch(rows[i].textContent,/已更新预备票据|未收到票据|长度不符合策略/);
    }
    assert.match(rows[4].textContent,lang==='en'?/Standby ticket updated/:lang==='ru'?/Резервный билет обновлён/:lang==='zh-TW'?/已更新預備票據/:/已更新预备票据/);
  }
  p.changeLanguage('zh-CN');
  const rows=p.get('rows').children;
  assert.match(rows[0].textContent,/响应：未收到票据/);
  assert.match(rows[1].textContent,/票据相同，未更新/);
  assert.match(rows[2].textContent,/与预备票据相同，未更新/);
  assert.match(rows[3].textContent,/已更新使用中票据/);
  assert.match(rows[8].textContent,/长度不符合策略，未更新/);
  assert.match(rows[9].textContent,/模型不一致，未更新/);
  assert.match(rows[12].textContent,/响应：未观测/);
  assert.doesNotMatch(rows[12].textContent,/已更新|未收到票据/);
  assert.match(rows[13].textContent,/响应：未收到票据/);
  assert.doesNotMatch(rows[13].textContent,/332|收到 332/);
  assert.match(rows[14].textContent,/响应：宿主未提供响应票据头/);
});

test('timezone editing keeps drafts through polling and locale changes and confirms explicitly',async()=>{
  const p=panel(),data={target:'America/Los_Angeles',records:[],turn_state_override:{probe:fixture()}};
  p.renderData(data);assert.equal(p.get('timezone-input').value,data.target);
  p.get('timezone-input').value='Asia/Tokyo';p.get('timezone-input').events.input();
  p.changeLanguage('en');p.renderData(data);
  assert.equal(p.get('timezone-input').value,'Asia/Tokyo');assert.equal(p.actions.length,0);
  await p.get('timezone-save').events.click();
  assert.deepEqual(JSON.parse(JSON.stringify(p.actions[0])),{action:'target-timezone',timezone:'Asia/Tokyo'});
  assert.equal(p.get('timezone-input').value,'Asia/Tokyo');assert.match(p.get('timezone-feedback').textContent,/Save failed/);
  p.get('timezone-input').value='';await p.get('timezone-save').events.click();assert.equal(p.actions.length,1);
  p.get('timezone-input').value='UTC';p.setResult(true);await p.get('timezone-save').events.click();
  assert.match(p.get('timezone-feedback').textContent,/Timezone saved/);
  p.renderData({...data,target:'UTC'});assert.equal(p.get('timezone-input').value,'UTC');
});

test('model disclosure hides outer ticket details and restores policy-colored length when collapsed',()=>{
  const p=panel(),base=fixture().values[0];
  const probe=fixture({accepted_state_lengths:[308],values:[{...base,value_length:308,target:'A/astra',account:'A'}]});
  p.render(probe);
  const rows=()=>p.get('probe-values').children;
  assert.equal(rows()[0].children[1].children[0].className,'badge state-ok');
  assert.match(rows()[0].children[0].textContent,/1\/1 账号可用/);
  rows()[0].children[0].children[0].children[0].events.click();
  assert.ok(rows()[0].children.slice(1).every(cell=>cell.textContent===''));
  assert.equal(rows()[1].classList.contains('hidden'),false);
  assert.equal(rows()[1].animation.options.duration,160);
  assert.equal(rows()[0].children[0].children[0].children[0].focused,true);
  p.render(probe);
  assert.ok(rows()[0].children.slice(1).every(cell=>cell.textContent===''));
  assert.equal(rows()[1].animation,undefined); // Polling does not replay motion.
  rows()[0].children[0].children[0].children[0].events.click();
  assert.equal(rows()[0].children[1].children[0].className,'badge state-ok');
  assert.equal(rows()[0].animation.options.duration,160);
  assert.equal(rows()[1].classList.contains('hidden'),true);
  p.render({...probe,accepted_state_lengths:[292]});
  assert.equal(rows()[0].children[1].children[0].className,'badge state-warn');
});

test('reduced motion skips model disclosure animations without changing expansion',()=>{
  const p=panel({reducedMotion:true});
  p.render(fixture({values:[{...fixture().values[0],target:'A/astra',account:'A'}]}));
  p.get('probe-values').children[0].children[0].children[0].children[0].events.click();
  const rows=p.get('probe-values').children;
  assert.equal(rows[1].classList.contains('hidden'),false);
  assert.equal(rows[1].animation,undefined);
});
