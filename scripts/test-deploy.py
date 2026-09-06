#!/usr/bin/env python3
"""Run with python3 scripts/test-deploy.py [--docker]; no Go or shared containers touched."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def tool(directory, name, body):
    path = directory / name
    path.write_text('#!/bin/sh\n' + body + '\n')
    path.chmod(0o755)


def test_commands():
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        for name in ('bash', 'dirname', 'seq', 'mktemp', 'rm'):
            (directory / name).symlink_to(shutil.which(name))
        stub = directory / 'stub.py'
        stub.write_text('''import json
import os
from pathlib import Path
import signal
import sys

kind, *args = sys.argv[1:]
state_path = Path(os.environ['STATE'])
state = json.loads(state_path.read_text())
event = {'tool': kind, 'args': args, 'env': {
    k: v for k, v in os.environ.items() if k.startswith('NEXO_')}}
status = 0
output = ''
mode = os.environ['MODE']
if kind == 'docker':
    if args[0] == 'run':
        state['runs'] += 1
        name = args[args.index('--name') + 1]
        if mode in ('start', 'collision') and state['runs'] == 2:
            if mode == 'collision':
                state['containers']['preexisting-collision'] = {'name': name}
            status = 23
            output = 'unowned-failed-run'
        else:
            ident = 'owned-' + str(state['runs'])
            binding = args[args.index('-p') + 1].split(':')
            port = binding[-2] or str(41000 + state['runs'])
            state['containers'][ident] = {'name': name, 'port': port}
            event['created'] = ident
            if '--cidfile' in args:
                Path(args[args.index('--cidfile') + 1]).write_text(ident)
            if mode == 'startup-signal-' + str(state['runs']):
                os.kill(os.getppid(), signal.SIGTERM)
            if mode == 'created-start-failure' and state['runs'] == 2:
                status = 23
            output = ident
    elif args[0] == 'port':
        output = '127.0.0.1:' + state['containers'][args[1]]['port']
    elif args[0] == 'rm':
        for ident in args[2:]:
            state['containers'].pop(ident, None)
    elif args[0] != 'exec':
        raise AssertionError(args)
elif args[0] == 'run':
    if mode == 'migrate-' + os.environ['NEXO_DB_DRIVER']:
        status = 24
elif args[0] == 'test':
    if mode == 'test':
        status = 25
    elif mode in ('SIGTERM', 'SIGINT', 'SIGHUP'):
        os.kill(os.getppid(), getattr(signal, mode))
state_path.write_text(json.dumps(state))
with open(os.environ['EVENTS'], 'a') as log:
    log.write(json.dumps(event) + '\\n')
if output:
    print(output)
sys.exit(status)
''')
        for name in ('docker', 'go'):
            tool(directory, name, f'exec "{sys.executable}" "{stub}" {name} "$@"')
        tool(directory, 'gofmt', 'exit 0')
        env = dict(os.environ, PATH=tmp, STATE=str(directory / 'state'),
                   EVENTS=str(directory / 'events'))
        for key in ('PG_PORT', 'MY_PORT', 'REDIS_PORT', 'NEXO_TEST_DISPOSABLE'):
            env.pop(key, None)
        run_tmp = directory / 'run dirs'
        run_tmp.mkdir()
        sentinel = run_tmp / 'unrelated'
        sentinel.touch()
        env['TMPDIR'] = str(run_tmp)
        seen_names = set()
        scenarios = [('ok', [], {}), ('ok', ['-race', '-p=8', '-run', 'TestSentinel'], {}),
                     ('ok', ['-p', '8'], {'PG_PORT': '35432', 'MY_PORT': '33306', 'REDIS_PORT': '36379'})]
        scenarios.extend((mode, [], {}) for mode in
                         ('start', 'collision', 'created-start-failure', 'migrate-postgres', 'migrate-mysql', 'test',
                          'SIGTERM', 'SIGINT', 'SIGHUP', 'startup-signal-1', 'startup-signal-2', 'startup-signal-3'))
        for mode, args, overrides in scenarios:
            existing = {name: {'name': name} for name in
                        ('nexo-test-pg', 'nexo-test-mysql', 'nexo-test-redis', 'unrelated-id')}
            (directory / 'state').write_text(json.dumps({'runs': 0, 'containers': existing}))
            (directory / 'events').write_text('')
            result = subprocess.run([str(directory / 'bash'), str(ROOT / 'scripts/test-all.sh'), *args],
                                    env=dict(env, MODE=mode, **overrides), capture_output=True, text=True, timeout=15)
            events = [json.loads(line) for line in (directory / 'events').read_text().splitlines()]
            created = [e['created'] for e in events if 'created' in e]
            removed = [ident for e in events if e['tool'] == 'docker' and e['args'][0] == 'rm'
                       for ident in e['args'][2:]]
            assert sorted(removed) == sorted(created), (mode, 'cleanup must use only created IDs', removed, created)
            expected_status = {'ok': 0, 'start': 23, 'collision': 23, 'created-start-failure': 23,
                               'migrate-postgres': 24, 'migrate-mysql': 24, 'test': 25,
                               'SIGTERM': 143, 'SIGINT': 130, 'SIGHUP': 129,
                               'startup-signal-1': 143, 'startup-signal-2': 143, 'startup-signal-3': 143}[mode]
            assert result.returncode == expected_status, (mode, result.returncode, result.stderr)
            remaining = json.loads((directory / 'state').read_text())['containers']
            assert {k: remaining[k] for k in existing} == existing, remaining
            assert set(remaining) == set(existing) | ({'preexisting-collision'} if mode == 'collision' else set()), remaining
            assert list(run_tmp.iterdir()) == [sentinel], list(run_tmp.iterdir())
            runs = [e['args'] for e in events if e['tool'] == 'docker' and e['args'][0] == 'run']
            names = {a[a.index('--name') + 1] for a in runs}
            assert len(names) == len(runs) and names.isdisjoint(seen_names), names
            seen_names.update(names)
            ports = [overrides.get(key, str(41001 + i)) for i, key in enumerate(('PG_PORT', 'MY_PORT', 'REDIS_PORT'))]
            for i, command in enumerate(runs):
                key, container_port = [('PG_PORT', '5432'), ('MY_PORT', '3306'), ('REDIS_PORT', '6379')][i]
                assert command[command.index('-p') + 1] == f'127.0.0.1:{overrides.get(key, "")}:{container_port}', command
            children = [e for e in events if e['tool'] == 'go']
            expected_env = {'NEXO_TEST_PG_DSN': f'postgres://nexo:nexo@127.0.0.1:{ports[0]}/nexo?sslmode=disable',
                            'NEXO_TEST_MYSQL_DSN': f'root:nexo@tcp(127.0.0.1:{ports[1]})/nexo?parseTime=true&loc=UTC',
                            'NEXO_TEST_REDIS_ADDR': f'127.0.0.1:{ports[2]}', 'NEXO_TEST_DISPOSABLE': '1'}
            for child in children:
                assert {k: child['env'].get(k) for k in expected_env} == expected_env, child
            migrations = [e for e in children if e['args'][0] == 'run']
            for i, migration in enumerate(migrations):
                assert migration['args'] == ['run', './cmd/nexo', 'migrate', '-config', 'config/config.example.yaml'], migration
                driver, access, dsn = [('postgres', 'sqlc', 'NEXO_TEST_PG_DSN'), ('mysql', 'gorm', 'NEXO_TEST_MYSQL_DSN')][i]
                assert [migration['env'][key] for key in ('NEXO_DB_DRIVER', 'NEXO_DB_ACCESS', 'NEXO_DB_DSN')] == [driver, access, expected_env[dsn]], migration
            tests = [e['args'] for e in children if e['args'][0] == 'test']
            if mode in ('ok', 'test', 'SIGTERM', 'SIGINT', 'SIGHUP'):
                assert len(migrations) == 2 and tests == [['test', '-count=1', *args, '-p=1', './...']], children
                probes = [e['args'] for e in events if e['tool'] == 'docker' and e['args'][0] == 'exec']
                assert len(probes) == 3, probes
                assert probes[1][2:] == ['mysqladmin', 'ping', '-h', '127.0.0.1', '--protocol=TCP', '-uroot', '-pnexo', '--silent'], probes
            else:
                assert not tests, tests
        print(f'PASS test-all isolation: {len(scenarios)} stub scenarios')
        tool(directory, 'go', 'exit 0')
        make = shutil.which('make')
        for status in (None, 0, 7):
            if status is not None:
                tool(directory, 'staticcheck', f'exit {status}')
            result = subprocess.run([make, '-s', '-f', str(ROOT / 'Makefile'), 'lint'], cwd=tmp, env=env, capture_output=True, text=True)
            assert (result.returncode != 0) == (status == 7), (status, result.stdout, result.stderr)
            assert ('staticcheck not found' in result.stdout) == (status is None), result.stdout
            if status == 7:
                assert 'Error 7' in result.stderr, result.stderr


def test_config():
    nginx = (ROOT / 'deploy/nginx.conf').read_text()
    directives = '\n'.join(line.split('#', 1)[0] for line in nginx.splitlines())
    assert directives.lstrip().startswith('error_log /dev/null;'), 'missing main-level error sink'
    assert 'include ' not in directives, 'review included error_log overrides before enabling includes'
    compose = (ROOT / 'deploy/docker-compose.mysql.yml').read_text()
    health = next(line for line in compose.splitlines() if 'mysqladmin' in line)
    assert '-h127.0.0.1' in health and '--protocol=TCP' in health, health
    assert '$$MYSQL_ROOT_PASSWORD' in health and '-pnexo' not in health, health
    readme = (ROOT / 'README.md').read_text().split('## Embedding', 1)[1].split('## Documentation', 1)[0]
    assert 'cfg.Db.Access = "gorm"' in readme and 'cfg.Db.Driver' in readme, 'embedding config mismatch'
    assert 's, err := nexo.New' in readme and 'ack, err :=' in readme and readme.count('if err != nil') >= 2, 'ignored embedding errors'


def test_nginx():
    name = 'nexo-log-check-' + uuid.uuid4().hex[:12]
    token = 'nexo_secret_probe_' + uuid.uuid4().hex
    def docker(*args):
        return subprocess.check_output(['docker', *args], stderr=subprocess.STDOUT, text=True)
    try:
        docker('run', '-d', '--name', name, '-p', '127.0.0.1::80',
               '--add-host', 'nexo1:127.0.0.1', '--add-host', 'nexo2:127.0.0.1', '--add-host', 'nexo3:127.0.0.1',
               '-v', f'{ROOT}/deploy/nginx.conf:/etc/nginx/nginx.conf:ro', 'nginx:1.27-alpine')
        port = json.loads(docker('inspect', name))[0]['NetworkSettings']['Ports']['80/tcp'][0]['HostPort']
        for attempt in range(50):
            try:
                dump = docker('exec', name, 'nginx', '-T')
                break
            except subprocess.CalledProcessError:
                time.sleep(0.1)
        else:
            raise AssertionError('nginx startup failed')
        assert dump.count('# configuration file ') == 1, 'unexpected included config'
        statuses = []
        for path in ('/ws', '/other', '/api/v1/auth/login'):
            try:
                urllib.request.urlopen(f'http://127.0.0.1:{port}{path}?token={token}', timeout=5).close()
            except urllib.error.HTTPError as err:
                statuses.append(err.code)
        assert statuses == [502, 502, 502], statuses
        logs = docker('logs', name)  # Both stdout and stderr, including the image entrypoint.
        assert ' 502 ' in logs, 'access log probe did not run'
        assert token not in logs, 'request token leaked to container logs'
    finally:
        subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def test_mysql():
    name = 'nexo-mysql-check-' + uuid.uuid4().hex[:12]
    env = dict(os.environ, NEXO_AUTH_NATIVE_SECRET=uuid.uuid4().hex,
               MYSQL_ROOT_PASSWORD=uuid.uuid4().hex)
    with tempfile.TemporaryDirectory() as tmp:
        gate = Path(tmp) / '00-test-gate.sh'
        # The official entrypoint sources this hook while its socket-only server is running.
        gate.write_text('touch /tmp/nexo-init-entered\n'
                        'while [ ! -f /tmp/nexo-init-release ]; do sleep 0.1; done\n')
        gate.chmod(0o644)
        try:
            config = subprocess.check_output(['docker', 'compose', '-f', str(ROOT / 'deploy/docker-compose.mysql.yml'),
                                              'config', '--format', 'json'], env=env, text=True,
                                             stderr=subprocess.DEVNULL, timeout=30)
            health = json.loads(config)['services']['mysql']['healthcheck']['test'][1].replace('$$', '$')
            subprocess.run(['docker', 'run', '-d', '--name', name, '--tmpfs', '/var/lib/mysql',
                            '-e', 'MYSQL_ROOT_PASSWORD', '-v', f'{gate}:/docker-entrypoint-initdb.d/00-test-gate.sh:ro',
                            '--health-cmd', health, '--health-interval', '1s', '--health-retries', '120', 'mysql:8'],
                           env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=180)
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                entered = subprocess.run(['docker', 'exec', name, 'test', '-f', '/tmp/nexo-init-entered'],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
                if entered.returncode == 0:
                    break
                time.sleep(0.5)
            else:
                raise AssertionError('MySQL initialization hook did not start')
            socket = subprocess.run(['docker', 'exec', name, 'mysqladmin', 'ping', '--protocol=SOCKET', '--silent'],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            assert socket.returncode == 0, 'temporary server did not answer socket ping'
            probe = subprocess.run(['docker', 'exec', name, 'sh', '-c', health],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            assert probe.returncode != 0, 'temporary socket-only server accepted by health command'
            state = subprocess.check_output(['docker', 'inspect', '-f', '{{.State.Health.Status}}', name],
                                            text=True, stderr=subprocess.DEVNULL, timeout=10).strip()
            assert state != 'healthy', 'temporary socket-only server marked healthy'
            subprocess.run(['docker', 'exec', name, 'touch', '/tmp/nexo-init-release'],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                state = subprocess.check_output(['docker', 'inspect', '-f', '{{.State.Health.Status}}', name],
                                                text=True, stderr=subprocess.DEVNULL, timeout=10).strip()
                if state == 'healthy':
                    break
                time.sleep(0.5)
            else:
                raise AssertionError('MySQL did not become TCP healthy')
        except subprocess.SubprocessError:
            raise AssertionError('MySQL Docker command failed') from None
        finally:
            subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=30)


if __name__ == '__main__':
    failed = False
    checks = [test_commands, test_config]
    if '--docker' in sys.argv:
        checks.extend([test_nginx, test_mysql])
    for check in checks:
        try:
            check()
            print(f'PASS {check.__name__}')
        except (AssertionError, subprocess.SubprocessError) as err:
            failed = True
            print(f'FAIL {check.__name__}: {err}')
    sys.exit(int(failed))
