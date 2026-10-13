"""Real user-service memory containment, with no model traffic."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid


class MemoryContainmentTests(unittest.TestCase):
    def setUp(self):
        if any(shutil.which(command) is None for command in ('systemctl', 'systemd-run')):
            self.skipTest('systemd user-service commands are required')
        if subprocess.run(['systemctl', '--user', 'is-system-running'],
                          capture_output=True).returncode != 0:
            self.skipTest('a running systemd user manager is required')
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.unit = 'experiment-memory-test-' + uuid.uuid4().hex
        self.addCleanup(subprocess.run,
                        ['systemctl', '--user', 'stop', self.unit + '.service'],
                        capture_output=True)

    def launch(self, script):
        return subprocess.run([
            'systemd-run', '--user', '--wait', '--pipe', '--collect',
            '--unit=' + self.unit, '--working-directory=' + str(self.root),
            '--property=MemoryMax=128M', '--property=MemorySwapMax=0',
            '--property=OOMPolicy=kill', '--property=KillMode=control-group',
            '--property=Restart=no',
            '--expand-environment=no', sys.executable, '-c', script,
        ], capture_output=True, text=True, timeout=20)

    def test_successful_workload_observes_memory_limits(self):
        result = self.launch('''
import json, pathlib
group = pathlib.Path('/sys/fs/cgroup') / pathlib.Path('/proc/self/cgroup').read_text().strip().split(':', 2)[2].lstrip('/')
print(json.dumps({name: (group / name).read_text().strip() for name in ('memory.max', 'memory.swap.max', 'memory.oom.group')}))
''')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        properties = json.loads(result.stdout)
        self.assertEqual(properties['memory.max'], str(128 * 1024 * 1024))
        self.assertEqual(properties['memory.swap.max'], '0')
        self.assertEqual(properties['memory.oom.group'], '1')

    def test_memory_limit_kills_the_workload_children_and_preserves_an_external_peer(self):
        with subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']) as peer:
            try:
                result = self.launch('''
import os, pathlib, subprocess, sys, time
pathlib.Path('parent.pid').write_text(str(os.getpid()))
with pathlib.Path('starts').open('a') as output:
    output.write('started\\n')
child = subprocess.Popen([sys.executable, '-c', "import os, pathlib, time; pathlib.Path('child.pid').write_text(str(os.getpid())); data = bytearray(256 * 1024 * 1024); time.sleep(30)"])
time.sleep(30)
''')
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn('oom-kill', result.stderr)
                self.assertIsNone(peer.poll(), 'the unrelated peer was killed')
                self.assertEqual((self.root / 'starts').read_text(), 'started\n')
                for name in ('parent', 'child'):
                    pid = int((self.root / (name + '.pid')).read_text())
                    stat = Path('/proc') / str(pid) / 'stat'
                    self.assertTrue(not stat.exists() or stat.read_text().rsplit(')', 1)[1].split()[0] in {'Z', 'X'},
                                    name + ' survived the workload OOM')
            finally:
                peer.terminate()
                peer.wait(timeout=3)
