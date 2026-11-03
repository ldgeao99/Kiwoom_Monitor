// Run: node tests/test_dashboard_loading.cjs (requires Playwright and Chromium).
const {chromium} = require('playwright');
const fs = require('fs');
const path = require('path');
const assert = require('assert/strict');
const html = fs.readFileSync(path.join(__dirname, '../apps/dashboard/index.html'), 'utf8');
(async () => {
  const browser = await chromium.launch({executablePath:process.env.CHROMIUM_PATH || '/usr/bin/chromium', headless:true});
  try {
    const page = await browser.newPage({viewport:{width:1100,height:650}});
    const errors = [], requests = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.addInitScript(() => localStorage.setItem('stock_chart_code', '005930'));
    let releasePrimary, releaseProgram;
    const primaryGate = new Promise(resolve => releasePrimary = resolve);
    const programGate = new Promise(resolve => releaseProgram = resolve);
    const meta = {_cache:{updated_at:'2026-11-03T07:00:00+00:00',stale:true}};
    await page.route('**/*', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/') return route.fulfill({contentType:'text/html', body:html});
      requests.push(url.pathname + url.search);
      let data = {items:[],cont:'N',next:'',...meta};
      if (url.pathname === '/api/summary') {
        await primaryGate;
        data = {markets:[{key:'kospi',name:'코스피',idx:5000,flu:1,pred:50,
                          ind:1,frgnr:2,orgn:-3,series:[],series_prev:[],rising:100,fall:90,flat:10}]};
      } else if (url.pathname === '/api/netprps') {
        await primaryGate;
        data = {market:'kospi',markets:[{key:'kospi',name:'KOSPI'}], columns:[], points:[],default_selected:[]};
      } else if (url.pathname === '/api/prm_upper') {
        await programGate;
        const tp = url.searchParams.get('tp');
        data = {...meta, items:[{code:'005930',name:'side' + tp,net:10,cur:100,flu:1,rank:1}]};
      }
      else if (url.pathname === '/api/overseas') data = {...meta,nasdaq:{cur:1,low:0,high:2}};
      else if (url.pathname === '/api/intraday_investors') data = {groups:{},...meta};
      else if (url.pathname === '/api/stock') data = {code:'005930',name:'삼성전자'};
      else if (url.pathname === '/api/credit_ratio') data = {ratio:null};
      else if (url.pathname === '/api/stock_memo') data = {memo:''};
      else if (url.pathname === '/api/candles') data = {code:url.searchParams.get('code'),tf:'1m',cont:'Y',next:url.searchParams.get('cont') === 'Y' ? 'older2' : 'older',
          items:[{t:'20261103100000',o:100,h:102,l:99,c:101,v:10},{t:'20261103100100',o:101,h:103,l:100,c:102,v:20}]};
      else if (url.pathname === '/api/program') data = {series:[],cont:'N',next:''};
      return route.fulfill({contentType:'application/json',body:JSON.stringify(data)});
    });
    await page.goto('http://monitor.test/', {waitUntil:'domcontentloaded'});
    await page.waitForFunction(() => requestFlights.size >= 2);
    assert.deepEqual(requests.map(x=>x.split('?')[0]).sort(), ['/api/netprps','/api/summary']);
    releasePrimary();
    await page.waitForFunction(() => initialReady);
    await page.waitForFunction(() => document.getElementById('mkt-cards').textContent.includes('코스피'));
    await page.waitForFunction(() => document.getElementById('nasdaq-fut').textContent.includes('나스닥'));
    assert(!requests.some(x=>x.startsWith('/api/candles')), 'offscreen restored chart must not load at boot');
    await page.locator('#program-rank-content').scrollIntoViewIfNeeded();
    await page.waitForFunction(() => [...requestFlights.keys()].some(k=>k.startsWith('prm:')));
    assert(await page.evaluate(() => renderPrmUpper() === renderPrmUpper()), 'same refresh must share a promise');
    assert.equal(requests.filter(x=>x.startsWith('/api/prm_upper')).length,1);
    await page.evaluate(() => { puSide = '1'; renderPrmUpper(); puSide = '2'; renderPrmUpper(); });
    releaseProgram();
    await page.waitForFunction(() => ![...requestFlights.keys()].some(k=>k.startsWith('prm:')));
    assert.equal(await page.evaluate(() => puItems[0].name), 'side2', 'rapid filter changes must keep the selected side');
    await page.locator('#toggle-chart').click();
    await page.locator('#stock-chart').scrollIntoViewIfNeeded();
    await page.waitForFunction(() => !candleLoading && candleItems.length === 2);
    assert(await page.locator('#stock-chart svg').count() > 0, 'chart must render');
    assert(requests.some(x=>x.startsWith('/api/candles') && x.includes('latest=1')));
    assert.equal(await page.evaluate(() => candleNext), 'older');
    await page.evaluate(() => loadCandles(true));
    assert(requests.some(x=>x.startsWith('/api/candles') && x.includes('cont=Y') && x.includes('next=older')));
    assert.equal(await page.evaluate(() => candleNext), 'older2');
    await page.evaluate(() => loadCandles(false, true));
    assert.equal(await page.evaluate(() => candleNext), 'older2', 'latest refresh must keep the loaded history cursor');
    assert.deepEqual(errors, []);
    console.log('PASS: first-screen priority, deferred panels, overseas split, shared refresh, latest chart and history cursor');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode=1; });
