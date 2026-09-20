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
  const win={addEventListener:(name,fn)=>{(windowEvents[name]??=[]).push(fn);},matchMedia:()=>media};
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
  await row.children[0].children[3].children[1].events.click();
  assert.equal(p.actions[0].id,'exit-test');assert.equal(p.actions[0].proxy,undefined);
  row.children[0].children[3].children[0].events.click();
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
  const ops=first.children[0].children[3];
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
  p.get('probe-ip-toggle').events.click();
  p.render(fixture({history:[
    {proxy:'socks5h://192.0.2.1:1080',proxy_label:'自定义出口 <img src=x>',egress_addr:'2001:db8::1',success:true,state_length:292},
    {proxy:'socks5h://192.0.2.1:1080',proxy_label:'另一个账号',success:false},
    {proxy:'direct',success:true},
    {proxy:'http://test-user:test-password@192.0.2.2:8080',success:false}
  ]}));
  const rows=p.get('probe-history').children;
  assert.match(rows[0].children[2].textContent,/自定义出口 <img src=x>/);
  assert.equal(rows[0].children[2].title,'socks5h://192.0.2.1:1080');
  assert.match(rows[0].children[3].textContent,/2001:db8::1/);
  assert.match(rows[0].children[3].textContent,/Proxy-reported · Unverified/);
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


test('public egress history shows IP, location, source label and tooltip',()=>{
  const p=panel();
  p.get('probe-ip-toggle').events.click();
  const records=[
    {proxy:'socks5h://192.0.2.1:1080',proxy_label:'美国',egress_addr:'203.0.113.44',egress_location:'Ashburn · Virginia · 美国',egress_source:'public',success:true},
    {proxy:'socks5h://192.0.2.1:1080',egress_addr:'198.51.100.20',egress_source:'public_sample',success:true}
  ];
  p.renderData({records:[],turn_state_override:{probe:fixture({history:records,success_history:records})}});
  for(const id of ['probe-history','probe-success-history']){
    const rows=p.get(id).children;
    assert.match(rows[0].children[3].textContent,/203\.0\.113\.44/);
    assert.match(rows[0].children[3].textContent,/Ashburn · Virginia · 美国/);
    assert.match(rows[0].children[3].textContent,/公网出口/);
    assert.match(rows[0].children[3].title,/短会话钉住同一出口/);
    assert.match(rows[1].children[3].textContent,/198\.51\.100\.20/);
    assert.match(rows[1].children[3].textContent,/同代理抽样/);
    assert.match(rows[1].children[3].title,/不一定等于本次探测/);
  }
  p.changeLanguage('en');
  const cell=p.get('probe-history').children[0].children[3];
  assert.match(cell.textContent,/Public exit/);
  assert.match(cell.title,/same sticky exit/);
});

test('both histories show connection-reported IPs and explicitly disclose unavailable exits',()=>{
  const p=panel();
  p.get('probe-ip-toggle').events.click();
  const records=[
    {proxy:'socks5h://192.0.2.1:1080',proxy_label:'聚合池',egress_addr:'2001:db8::10',success:true},
    {proxy:'socks5h://192.0.2.1:1080',proxy_label:'聚合池',egress_addr:'2001:db8::11',success:true},
    {proxy:'socks5h://192.0.2.1:1080',egress_addr:'0.0.0.0',success:true},
    {proxy:'socks5://192.0.2.1:1080',egress_addr:'::',success:true},
    {proxy:'http://192.0.2.2:8080',success:true},
    {proxy:'direct',success:true},
    {proxy:'socks5h://192.0.2.1:1080',egress_addr:'<img src=x>',success:true}
  ];
  const data={records:[],turn_state_override:{probe:fixture({history:records,success_history:records})}};
  p.renderData(data);
  for(const id of ['probe-history','probe-success-history']){
    const rows=p.get(id).children;
    assert.equal(rows[0].children.length,7);
    assert.equal(rows[0].children[2].textContent,'聚合池');
    assert.match(rows[0].children[3].textContent,/2001:db8::10.*未核验/);
    assert.match(rows[1].children[3].textContent,/2001:db8::11/);
    assert.match(rows[0].children[3].title,/不保证等于上游/);
    for(const row of rows.slice(2,6))assert.equal(row.children[3].textContent,'未获取');
    assert.match(rows[2].children[3].title,/无法判断/);
    assert.match(rows[4].children[3].title,/入口地址不能代表/);
    assert.equal(rows[6].children[3].children[0].textContent,'<img src=x>');
  }
  for(const [locale,word] of [['en','Unavailable'],['zh-TW','未取得'],['ru','Нет данных']]){
    p.changeLanguage(locale);
    assert.equal(p.get('probe-history').children[4].children[3].textContent,word);
  }
  p.render(fixture());
  assert.equal(p.get('probe-history').children[0].children[0].colSpan,7);
  assert.equal(p.get('probe-success-history').children[0].children[0].colSpan,7);
});


test('egress visibility masks both lists and survives polling and language changes',async()=>{
  const p=panel();
  const record={model:'gpt-6-astra',proxy:'direct',egress_addr:'203.0.113.42',success:true};
  const data={records:[],turn_state_override:{probe:fixture({history:[record],success_history:[record]})}};
  p.renderData(data);
  const addresses=()=>['probe-history','probe-success-history'].map(id=>p.get(id).children[0].children[3]);
  for(const cell of addresses()){
    assert.equal(cell.textContent,'***');
    assert.doesNotMatch(JSON.stringify(cell),/203\.0\.113\.42/);
  }
  p.get('probe-history-panel').open=true;
  p.get('probe-ip-toggle').events.click();
  for(const cell of addresses())assert.match(cell.textContent,/203\.0\.113\.42/);
  assert.equal(p.get('probe-success-ip-toggle').getAttribute('aria-pressed'),'true');
  p.renderData(data);p.changeLanguage('en');
  assert.equal(p.get('probe-ip-toggle').textContent,'Hide');
  assert.equal(p.get('probe-history-panel').open,true);
  for(const cell of addresses())assert.match(cell.textContent,/203\.0\.113\.42/);
  p.get('probe-success-ip-toggle').events.click();
  p.renderData(data);p.changeLanguage('ru');
  for(const cell of addresses())assert.equal(cell.textContent,'***');
  assert.equal(p.get('probe-ip-toggle').textContent,'Показать');
  assert.equal(p.get('probe-success-ip-toggle').getAttribute('aria-pressed'),'false');
  assert.equal(p.actions.length,0);
  p.get('probe-ip-toggle').events.click();
  await p.get('refresh').events.click();
  p.renderData(data);
  for(const cell of addresses())assert.equal(cell.textContent,'***');
});


test('hidden history addresses mask missing and zero values until explicitly shown',()=>{
  const p=panel();
  const records=['203.0.113.42',undefined,'','0.0.0.0','::'].map(egress_addr=>({proxy:'socks5h://192.0.2.1:1080',egress_addr,success:true}));
  const data={records:[],turn_state_override:{probe:fixture({history:records,success_history:records})}};
  const lists=()=>['probe-history','probe-success-history'].map(id=>p.get(id).children.map(row=>row.children[3]));
  const assertHidden=()=>{
    for(const cells of lists())for(const cell of cells){
      assert.equal(cell.textContent,'***');
      assert.ok(!cell.title);
      assert.equal(cell.children.length,0);
      assert.doesNotMatch(JSON.stringify(cell),/203\.0\.113\.42|未获取/);
    }
  };
  p.renderData(data);assertHidden();
  p.get('probe-ip-toggle').events.click();
  for(const cells of lists()){
    assert.match(cells[0].textContent,/203\.0\.113\.42/);
    for(const cell of cells.slice(1))assert.equal(cell.textContent,'未获取');
  }
  p.get('probe-success-ip-toggle').events.click();
  p.renderData(data);p.changeLanguage('en');assertHidden();
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
  assert.equal(rows[0].children[6].children[0].className,'badge state-ok');
  assert.equal(rows[1].children[6].children[0].className,'badge state-warn');
  assert.match(p.get('probe-values').textContent,/剩余/);
});

test('account routing status follows all CPA languages and preserves user text',()=>{
  const p=panel();
  const data={records:[],turn_state_override:{probe:fixture()},account_routing:{enabled:true,accounts:[{
    account:'auth-test-account',model:'astra',state:'healthy',last_state_length:308,observed_at:new Date().toISOString()}]}};
  p.renderData(data);
  for(const [locale,label] of [['en','Healthy'],['zh-TW','健康'],['ru','Исправен'],['zh-CN','健康']]){
    p.changeLanguage(locale);
    assert.match(p.get('account-routing-rows').textContent,new RegExp(label));
    assert.match(p.get('account-routing-rows').textContent,/auth-test-account/);
    if(locale==='en')assert.match(p.get('account-routing-note').textContent,/Routing enabled/);
  }
});
