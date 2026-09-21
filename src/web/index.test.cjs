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
    document:{documentElement:root,querySelectorAll:()=>staticNodes,getElementById:get,createElement:tag=>new Element(tag),createDocumentFragment:()=>new Element('fragment')},
    location:{pathname:'/management.html',host:'localhost',origin:'http://localhost'},
    navigator:{userAgent:'panel-test',language:options.browserLanguage},localStorage:{getItem:name=>{if(options.storageUnavailable)throw new Error('storage unavailable');return name==='cli-proxy-theme'?JSON.stringify({state:{theme:storedTheme}}):name==='cli-proxy-language'?storedLanguage:null;}},
    window:win,MutationObserver:class{constructor(fn){this.fn=fn;}observe(target,config){observers.push({fn:this.fn,filter:config.attributeFilter});}disconnect(){}},
    setInterval(){},setTimeout(){},URL,TextEncoder,TextDecoder,
  });
  const script=html.match(/<script>([\s\S]*?)<\/script>/)[1];
  vm.runInContext(script,context);
  vm.runInContext('globalThis.translateTest=t;globalThis.messagesTest=messages;globalThis.renderTest=renderProbe;globalThis.renderPoolTest=renderPool;globalThis.openExitTest=openExitEditor;globalThis.renderDataTest=render;globalThis.actions=[];globalThis.controlResult=false;probeControl=async payload=>{actions.push(payload);return controlResult;}',context);
  return {get,root,host,staticNodes,translate:context.translateTest,messages:context.messagesTest,changeLanguage:language=>{storedLanguage=JSON.stringify({state:{language}});for(const fn of windowEvents.storage||[])fn({key:'cli-proxy-language'});},changeHostLanguage:language=>{host.setAttribute('lang',language);notify('lang');},changeHost:theme=>{host.setAttribute('data-theme',theme);notify('data-theme');},changeStored:theme=>{storedTheme=theme;for(const fn of windowEvents.storage||[])fn({key:'cli-proxy-theme'});},changeSystem:dark=>{media.matches=dark;mediaEvents.change();},render:context.renderTest,renderPool:context.renderPoolTest,openExit:context.openExitTest,renderData:context.renderDataTest,actions:context.actions,setResult:value=>{context.controlResult=value;}};
}

function fixture(overrides={}) {
  return {enabled:true,running:false,halted:false,prefetch_minutes:3,paused:[],active_models:[],queue_models:[],
    values:[{model:'gpt-6-astra',valid:true,value_length:292,source:'probe',remaining_seconds:180,
      issued_at:new Date(Date.now()-52*60000).toISOString(),expires_at:new Date(Date.now()+3*60000).toISOString()}],...overrides};
}

test('account routing renders empty, expired, pending and business failure separately',()=>{
  const p=panel();
  const data={records:[],turn_state_override:{probe:fixture()},account_routing:{enabled:true,accounts:[]}};
  p.renderData(data);
  assert.match(p.get('account-routing-rows').textContent,/尚无账号级使用记录/);
  data.account_routing.accounts=[
    {account:'auth-a',model:'astra',state:'expired'},
    {account:'auth-b',model:'astra',state:'probe_pending',probe_reason:'probe_model_mismatch'},
    {account:'auth-c',model:'sol',state:'degraded',cooldown_until:new Date(Date.now()+60000).toISOString()},
    {account:'auth-d',model:'astra',state:'healthy',healthy_until:new Date(Date.now()+60000).toISOString(),last_state_length:332},
    {account:'auth-e',model:'astra',state:'expired',renewal_attempted:true},
  ];
  p.renderData(data);
  const rows=p.get('account-routing-rows').children;
  assert.match(rows[0].children[2].textContent,/票据到期 · 待重探/);
  assert.match(rows[0].children[2].children[0].className,/state-warn/);
  assert.match(rows[1].children[2].textContent,/待重新探测/);
  assert.match(rows[1].children[6].textContent,/探测记录：probe_model_mismatch/);
  assert.match(rows[2].children[2].children[0].className,/state-fail/);
  assert.match(rows[2].children[6].textContent,/冷却/);
  assert.match(rows[3].children[2].children[0].className,/state-ok/);
  assert.equal(rows[3].children[3].textContent,'332 B');
  assert.match(rows[4].children[2].textContent,/续期已尝试 · 可手动重探/);
});

test('account guard counts, errors and all account locales update during refresh',()=>{
  const p=panel();
  const data={records:[],turn_state_override:{probe:fixture(),session_guard:{enabled:true,mode:'enforce',foreign_observed:5,foreign_replaced:2,foreign_stripped:3,last_decision:{kind:'foreign-account',account:'auth-a',owner:'auth-b',fingerprint:'123abc'}}},account_routing:{enabled:true,accounts:[{account:'auth-a',model:'astra',state:'expired'}]}};
  p.renderData(data);
  assert.match(p.get('account-routing-note').textContent,/异账号 5（替换 2 \/ 剥离 3）/);
  assert.match(p.get('account-routing-note').title,/指纹 123abc/);
  for(const [locale,label] of [['en','State expired'],['zh-TW','票據到期'],['ru','Срок истёк']]){
    p.changeLanguage(locale);
    assert.ok(p.get('account-routing-rows').textContent.includes(label));
    assert.doesNotMatch(p.get('account-routing-note').textContent,/会话守卫|路由已开启/);
  }
  p.changeLanguage('en');
  assert.match(p.get('account-routing-note').textContent,/Foreign states 5/);
  data.turn_state_override.session_guard={mode:'off'};data.account_routing.error='invalid_setting';
  p.renderData(data);
  assert.match(p.get('account-routing-note').textContent,/Configuration error: invalid_setting/);
  assert.match(p.get('account-routing-note').className,/settings-error/);
  assert.equal(p.get('account-routing-note').title,'');
});

test('account provenance request badges are translated with their tooltips',()=>{
  const p=panel({storedLanguage:'en'});
  p.renderData({records:[{time:new Date().toISOString(),model:'gpt-6-astra',original:[],paths:[],turn_state_override:'session-foreign-stripped',turn_state_provenance:'foreign-account',turn_state_fingerprint:'123abc',turn_state_owner:'auth-owner'}],turn_state_override:{probe:fixture()}});
  assert.match(p.get('rows').textContent,/Foreign state stripped/);
  assert.match(p.get('rows').textContent,/Other-account source/);
  assert.doesNotMatch(p.get('rows').textContent,/异账号/);
});

test('quota and authentication failures are not labelled as degradation',()=>{
  const p=panel({storedLanguage:'en'});
  const records=['quota_exhausted','rate_limited','account_auth_error','account_temporarily_unavailable'].map(kind=>({time:new Date().toISOString(),model:'gpt-6-astra',original:[],paths:[],rejection_kind:kind}));
  p.renderData({records,account_routing:{enabled:true,accounts:[{account:'quota',model:'astra',state:'quota_exhausted',cooldown_until:new Date(Date.now()+120000).toISOString()},{account:'rate',model:'astra',state:'rate_limited'}]},turn_state_override:{probe:fixture()}});
  assert.match(p.get('rows').textContent,/Quota exhausted/);
  assert.match(p.get('rows').textContent,/Rate limited/);
  assert.match(p.get('rows').textContent,/Authentication failed/);
  assert.doesNotMatch(p.get('rows').textContent,/Degradation blocked/);
  assert.match(p.get('account-routing-rows').textContent,/Quota exhausted/);
  assert.match(p.get('account-routing-rows').children[0].children[2].children[0].className,/state-warn/);
  p.changeLanguage('zh-CN');
  assert.match(p.get('rows').textContent,/额度已用完/);
  assert.doesNotMatch(p.get('rows').textContent,/降智已拦截/);
});

test('account groups count unique identities and keep model health and TTL independent',()=>{
 const p=panel();
 const data={records:[],turn_state_override:{probe:fixture()},account_routing:{accounts:[
  {account:'auth-a',model:'astra',state:'healthy',healthy_until:new Date(Date.now()+60000).toISOString(),last_state_length:332},
  {account:'auth-b',model:'astra',state:'degraded'},
  {account:'auth-a',model:'sol',state:'expired'},
 ]}};
 p.renderData(data);
 assert.equal(p.get('account-routing-count').textContent,'2 个账号 · 3 条模型记录');
 const rows=p.get('account-routing-rows').children;
 assert.equal(rows.length,3);
 assert.equal(rows[0].children[0].getAttribute('rowspan'),'2');
 assert.equal(rows[1].children.length,6);
 assert.equal(rows[1].children[0].textContent,'sol');
 assert.match(rows[1].children[1].textContent,/票据到期/);
 assert.equal(rows[1].children[3].textContent,'—');
 assert.equal(rows[2].children[0].textContent,'auth-b');
 assert.match(rows[2].children[2].textContent,/业务异常/);
 p.changeLanguage('en');
 assert.equal(p.get('account-routing-count').textContent,'2 accounts · 3 model records');
 data.account_routing.accounts=[];p.renderData(data);
 assert.equal(p.get('account-routing-count').textContent,'0 accounts · 0 model records');
});

test('account inventory failures are distinct from no enabled health records',()=>{
 const p=panel({storedLanguage:'en'});
 const data={records:[],turn_state_override:{probe:fixture()},account_routing:{enabled:true,inventory_available:false,accounts:[]}};
 p.renderData(data);
 assert.match(p.get('account-routing-rows').textContent,/inventory unavailable/);
 assert.match(p.get('account-routing-note').textContent,/historical states are hidden/);
 data.account_routing.inventory_available=true;
 p.renderData(data);
 assert.match(p.get('account-routing-rows').textContent,/No health records for enabled accounts/);
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
  assert.equal(p.get('rows').children[0].children[0].colSpan,7);
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
  assert.match(html,/id="account-routing-panel"/);
  assert.doesNotMatch(html,/id="cooldown-toggle"/);
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
