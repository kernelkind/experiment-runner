// Read-only OpenRouter generation metadata through the selected authentication route.
// Kept separate from inference transport so saved model-runner identities stay valid.
import {spawn} from 'node:child_process';
import {localPiConnection} from './model.mjs';
import {authenticationMode, directConnection} from './direct-auth.mjs';

async function main() {
  const args=process.argv.slice(2);
  if (args.length && (args.length!==1 || args[0]!=='--pi-config')) throw new Error();
  const input=await new Response(process.stdin).text();
  const ids=JSON.parse(input);
  if (!Array.isArray(ids) || new Set(ids).size!==ids.length ||
      ids.some(id=>typeof id!=='string' || !/^gen-[A-Za-z0-9-]+$/.test(id))) throw new Error();
  const direct=authenticationMode()==='direct';
  if (direct && args.length) throw new Error();
  if (!direct && !args.length) {
    const child=spawn('agent-account',['exec','pi','--',process.execPath,
      process.argv[1],'--pi-config'],{stdio:['pipe','inherit','inherit']});
    const interrupt=()=>child.kill('SIGINT');
    const terminate=()=>child.kill('SIGTERM');
    process.on('SIGINT',interrupt);process.on('SIGTERM',terminate);
    child.stdin.on('error',()=>{});
    try {
      const finished=new Promise((resolve,reject)=>{
        child.once('error',reject);child.once('close',code=>resolve(code??1));
      });
      child.stdin.end(input);process.exitCode=await finished;
    } finally {
      process.off('SIGINT',interrupt);process.off('SIGTERM',terminate);
    }
    return;
  }
  const connection=direct ? await directConnection('openrouter') : await localPiConnection('openrouter');
  const rows=[];
  for (const id of ids) {
    try {
      const response=await fetch(`${connection.baseUrl}/generation?id=${encodeURIComponent(id)}`,{
        method:'GET',headers:{Authorization:`Bearer ${connection.apiKey}`},
        redirect:'error',signal:AbortSignal.timeout(30000)});
      if (!response.ok) {rows.push({id,error:`HTTP ${response.status}`});continue;}
      const {data}=await response.json();
      if (data?.id!==id || typeof data.total_cost!=='number' ||
          !Number.isFinite(data.total_cost) || data.total_cost<0) throw new Error();
      rows.push({id,total_cost:data.total_cost,model:data.model??null});
    } catch {rows.push({id,error:'Lookup failed or returned invalid billing metadata'});}
  }
  process.stdout.write(JSON.stringify(rows)+'\n');
}

main().catch(()=>{process.stderr.write('Cost lookup unavailable; no credential details printed.\n');process.exitCode=1;});
