import assert from 'node:assert/strict';
import {test} from 'node:test';
import {createServer} from 'node:http';
import {mkdtemp, writeFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawn} from 'node:child_process';

test('cost lookup uses only local GET requests and validates returned identity and cost', async t => {
  const home = await mkdtemp(join(tmpdir(), 'oracle-costs-'));
  t.after(() => rm(home, {recursive:true, force:true}));
  const calls=[];
  const server=createServer((req,res) => {
    calls.push([req.method, req.url]);
    assert.equal(req.headers.authorization, 'Bearer LOCAL_ONLY');
    const id=new URL(req.url, 'http://localhost').searchParams.get('id');
    res.setHeader('content-type','application/json');
    res.end(JSON.stringify({data:{id:id==='gen-wrong'?'gen-other':id,
      total_cost:id==='gen-negative'?-1:0, model:'fixture/model', secret:'DO_NOT_RETAIN'}}));
  });
  await new Promise(resolve => server.listen(0,'127.0.0.1',resolve));
  t.after(() => server.close());
  await writeFile(join(home,'models.json'),JSON.stringify({providers:{openrouter:{
    baseUrl:`http://127.0.0.1:${server.address().port}/openrouter/api/v1`,
    apiKey:'$AGENT_OPENROUTER_CAPABILITY'}}}));
  const child=spawn(process.execPath,[new URL('billing.mjs',import.meta.url).pathname,'--pi-config'],{
    env:{...process.env,PI_CODING_AGENT_DIR:home,AGENT_OPENROUTER_CAPABILITY:'LOCAL_ONLY'},
    stdio:['pipe','pipe','pipe']});
  let out='',err='';child.stdout.on('data',d=>out+=d);child.stderr.on('data',d=>err+=d);
  child.stdin.end(JSON.stringify(['gen-ok','gen-wrong','gen-negative']));
  const code=await new Promise(resolve=>child.on('close',resolve));
  assert.equal(code,0,err);
  const rows=JSON.parse(out);
  assert.equal(rows[0].total_cost,0);
  assert.ok(rows[1].error);assert.ok(rows[2].error);
  assert.ok(!out.includes('LOCAL_ONLY'));assert.ok(!out.includes('DO_NOT_RETAIN'));
  assert.deepEqual(calls,['gen-ok','gen-wrong','gen-negative'].map(id=>
    ['GET',`/openrouter/api/v1/generation?id=${id}`]));
});
