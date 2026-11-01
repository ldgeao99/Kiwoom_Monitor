import os
import re
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class DeployTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for folder in ('.git', 'bin', 'apps/dashboard', 'apps/rank_monitor'):
            (self.root / folder).mkdir(parents=True)
        for name in ('deploy.sh', 'auto_deploy.sh'):
            shutil.copy(ROOT / name, self.root / name)
        self.env = dict(os.environ, PATH=str(self.root / 'bin') + ':' + os.environ['PATH'],
                        TEST_DEPLOY_ROOT=str(self.root))
        self.stub('git', '''#!/bin/bash
case "$1" in
 fetch) exit 0 ;;
 pull) [ "$TEST_PULL_FAIL" = 1 ] && exit 1; touch "$TEST_DEPLOY_ROOT/updated" ;;
 rev-parse) if [ "$2" = origin/main ] || [ -f "$TEST_DEPLOY_ROOT/updated" ]; then echo new; else echo old; fi ;;
esac
''')
        self.stub('pkill', '#!/bin/bash\necho "$*" >> "$TEST_DEPLOY_ROOT/stops"\n')
        self.stub('sleep', '#!/bin/bash\nexec /bin/sleep 0.1\n')
        self.stub('python3', '''#!/bin/bash
if [ -e /proc/$$/fd/9 ]; then echo 'inherited deploy lock'; exit 99; fi
echo "$$" >> "$TEST_DEPLOY_ROOT/pids"
echo "$PWD|$*" >> "$TEST_DEPLOY_ROOT/starts"
if [ "$TEST_RANK_FAIL" = 1 ] && [ "$2" = rank_monitor_main.py ]; then exit 1; fi
exec /bin/sleep 60
''')
        self.addCleanup(self.stop_stubs)

    def stub(self, name, text):
        path = self.root / 'bin' / name
        path.write_text(text)
        path.chmod(0o755)

    def stop_stubs(self):
        pids = self.root / 'pids'
        if pids.exists():
            for pid in pids.read_text().splitlines():
                try:
                    os.kill(int(pid), signal.SIGTERM)
                except ProcessLookupError:
                    pass

    def run_deploy(self, script):
        return subprocess.run(['bash', str(self.root / script)], cwd='/tmp', env=self.env,
                              capture_output=True, text=True, timeout=10)

    def assert_both_restarted(self, script):
        result = self.run_deploy(script)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        starts = (self.root / 'starts').read_text().splitlines()
        self.assertCountEqual(starts, [f'{self.root}/apps/dashboard|-u dashboard_server.py',
                                     f'{self.root}/apps/rank_monitor|-u rank_monitor_main.py'])
        stops = (self.root / 'stops').read_text()
        self.assertIn('dashboard_server.py', stops)
        self.assertTrue(any(re.search(line.removeprefix('-f '), 'rank_monitor_main.py')
                            for line in stops.splitlines()))
        # Background children must not retain the cron deployment lock.
        unlocked = subprocess.run(['flock', '-n', str(self.root / '.git/auto-deploy.lock'), 'true'])
        self.assertEqual(unlocked.returncode, 0)

    def test_manual_restarts_both_from_any_directory(self):
        self.assert_both_restarted('deploy.sh')

    def test_auto_restarts_both_once_when_remote_changes(self):
        self.assert_both_restarted('auto_deploy.sh')

    def test_auto_no_change_does_not_stop_or_start(self):
        (self.root / 'updated').touch()
        result = self.run_deploy('auto_deploy.sh')
        self.assertEqual(result.returncode, 0)
        self.assertFalse((self.root / 'stops').exists())
        self.assertFalse((self.root / 'starts').exists())

    def test_failed_pull_leaves_existing_processes_alone(self):
        self.env['TEST_PULL_FAIL'] = '1'
        for script in ('deploy.sh', 'auto_deploy.sh'):
            result = self.run_deploy(script)
            self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / 'stops').exists())
        self.assertFalse((self.root / 'starts').exists())

    def test_failed_rank_start_is_reported(self):
        self.env['TEST_RANK_FAIL'] = '1'
        for script in ('deploy.sh', 'auto_deploy.sh'):
            (self.root / 'updated').unlink(missing_ok=True)
            result = self.run_deploy(script)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('순위 모니터 시작 실패', result.stdout)


if __name__ == '__main__':
    unittest.main()
