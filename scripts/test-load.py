#!/usr/bin/env python3
"""python3 scripts/test-load.py: isolated command regression; never runs real Docker/Go."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]

STUB = r'''import json
import os
from pathlib import Path
import signal
import sys

kind, *args = sys.argv[1:]
state_path = Path(os.environ['STATE'])
state = json.loads(state_path.read_text())
mode = os.environ['MODE']
event = {'tool': kind, 'args': args}
status, output = 0, ''
send_signal = False
if kind == 'docker':
    command = args[0]
    if args[:2] == ['network', 'create']:
        if mode == 'network-collision':
            status = 23
        else:
            output = 'owned-network'
            state['network'] = output
            send_signal = mode == 'network-signal'
    elif args[:2] == ['network', 'rm']:
        assert args[2] == state.pop('network')
    elif command == 'run':
        state['runs'] += 1
        number = state['runs']
        if mode == 'collision' and number == 2:
            status = 23
        else:
            output = f'owned-{number}'
            state['containers'].append(output)
            event['created'] = output
            Path(args[args.index('--cidfile') + 1]).write_text(output)
            if mode == 'created-start-failure' and number == 2:
                status = 23
            if mode == 'migrate' and number == 3:
                status = 24
            send_signal = mode == f'startup-signal-{number}'
            if number >= 3:
                assert len(os.environ['NEXO_AUTH_NATIVE_SECRET']) == 64
                assert os.environ['NEXO_DB_DSN'] == 'postgres://nexo:nexo@pg:5432/nexo?sslmode=disable'
                assert os.environ['NEXO_REDIS_ADDR'] == 'redis:6379'
    elif command == 'rm':
        assert args[1:3] == ['-f', '-v']
        state['containers'].remove(args[3])
    elif command == 'port':
        output = '127.0.0.1:' + str(41000 + int(args[1].split('-')[1]))
    elif command == 'exec':
        status = 1 if mode == 'pg-timeout' else 0
    elif command == 'build':
        state['image'] = args[args.index('-t') + 1]
        Path(args[args.index('--iidfile') + 1]).write_text('image-id')
        status = 26 if mode == 'build' else 0
    elif args[:2] == ['image', 'rm']:
        assert args[2] == state.pop('image')
    elif command == 'logs':
        assert args[1] in state['containers'] and args[1] != 'unrelated'
        output = 'safe container log'
    else:
        raise AssertionError(args)
elif kind == 'go':
    if args == ['env', 'GOPROXY']:
        output = 'https://proxy.golang.org,direct'
    else:
        assert args[:2] == ['run', './deploy/load']
        output = '{"ok":' + ('false' if mode == 'load' else 'true') + '}'
        status = 25 if mode == 'load' else 0
elif kind == 'curl':
    status = 1 if mode == 'node-timeout' else 0
elif kind == 'openssl':
    output = 'a' * 64
elif kind != 'sleep':
    raise AssertionError(kind)
state_path.write_text(json.dumps(state))
with open(os.environ['EVENTS'], 'a') as log:
    log.write(json.dumps(event) + '\n')
if output:
    print(output, flush=True)
if send_signal:
    os.kill(os.getppid(), signal.SIGTERM)
sys.exit(status)
'''


def test_commands():
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        bin_dir = directory / 'bin'
        bin_dir.mkdir()
        for name in ('bash', 'dirname', 'mktemp', 'mkdir', 'rm', 'tee'):
            (bin_dir / name).symlink_to(shutil.which(name))
        stub = directory / 'stub.py'
        stub.write_text(STUB)
        for name in ('docker', 'go', 'curl', 'openssl', 'sleep'):
            path = bin_dir / name
            path.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{stub}" {name} "$@"\n')
            path.chmod(0o755)
        run_tmp = directory / 'run dirs'
        run_tmp.mkdir()
        sentinel = run_tmp / 'unrelated'
        sentinel.touch()
        env = {k: v for k, v in os.environ.items() if not k.startswith('NEXO_')}
        env.update(PATH=str(bin_dir), TMPDIR=str(run_tmp), STATE=str(directory / 'state'),
                   EVENTS=str(directory / 'events'))
        forwarded = ['-users', '20', '-duration', '1s', '-allow-missed-push=false', '-nodes', 'http://unowned.example']
        scenarios = [('ok', {}, [], 0), ('ok', {'NEXO_LOAD_NODES': '02', 'NEXO_LOAD_IMAGE': 'existing:tag',
                      'NEXO_LOAD_OUTPUT': str(directory / 'artifacts')}, forwarded, 0)]
        scenarios += [(mode, {'NEXO_LOAD_NODES': '2'}, [], code) for mode, code in
                      [('network-collision', 23), ('network-signal', 143), ('collision', 23),
                       ('created-start-failure', 23), ('migrate', 24), ('build', 26), ('load', 25),
                       ('pg-timeout', 1), ('node-timeout', 1), ('startup-signal-1', 143),
                       ('startup-signal-4', 143)]]
        scenarios += [('invalid', {'NEXO_LOAD_NODES': value}, [], 2)
                      for value in ('', '0', '1', '-2', '1+2', '2x', '99999999999999999999999')]
        seen_names = set()
        for mode, overrides, flags, expected in scenarios:
            (directory / 'state').write_text(json.dumps({'runs': 0, 'containers': ['unrelated']}))
            (directory / 'events').write_text('')
            result = subprocess.run([str(bin_dir / 'bash'), str(ROOT / 'scripts/test-load.sh'), *flags],
                                    env=dict(env, MODE=mode, **overrides), capture_output=True, text=True, timeout=20)
            assert result.returncode == expected, (mode, result.returncode, result.stderr)
            events = [json.loads(line) for line in (directory / 'events').read_text().splitlines()]
            state = json.loads((directory / 'state').read_text())
            assert state == {'runs': state['runs'], 'containers': ['unrelated']}, (mode, state)
            docker = [e['args'] for e in events if e['tool'] == 'docker']
            created = [e['created'] for e in events if 'created' in e]
            removed = [a[3] for a in docker if a[0] == 'rm']
            assert sorted(created) == sorted(removed), (mode, created, removed)
            probes = [a for a in docker if a[0] == 'exec']
            if probes:
                assert probes[0][2:] == ['pg_isready', '-h', '127.0.0.1', '-U', 'nexo']
            if mode == 'pg-timeout':
                assert len(probes) == 60
            if mode == 'node-timeout':
                assert len([e for e in events if e['tool'] == 'curl']) == 60
            assert 'a' * 64 not in result.stdout + result.stderr + json.dumps(events)
            logs = [a[1] for a in docker if a[0] == 'logs']
            assert sorted(logs) == (sorted(created) if expected else []), (mode, logs)
            runs = [a for a in docker if a[0] == 'run']
            names = {a[a.index('--name') + 1] for a in runs}
            assert len(names) == len(runs) and names.isdisjoint(seen_names)
            seen_names.update(names)
            node_runs = [a for a in runs if '-p' in a]
            node_ids = []
            for args in runs:
                assert args[args.index('--network') + 1] == 'owned-network'
                if '-v' in args:
                    assert args[args.index('-v') + 1] == f'{ROOT}/deploy/config.yaml:/etc/nexo/config.yaml:ro'
            for args in node_runs:
                assert args[args.index('-p') + 1] == '127.0.0.1::8080'
                node_ids.append(next(a for a in args if a.startswith('NEXO_NODE_ID=')))
                assert 'NEXO_LIMITS_WS_CONNS_PER_IP=20000' in args
                assert 'NEXO_LIMITS_AUTH_PER_IP_PER_MIN=0' in args
            assert len(set(node_ids)) == len(node_runs)
            clients = [e['args'] for e in events if e['tool'] == 'go' and e['args'][0] == 'run']
            if mode in ('ok', 'load'):
                count = int(overrides.get('NEXO_LOAD_NODES', '10'))
                urls = ','.join(f'http://127.0.0.1:{41004 + i}' for i in range(count))
                assert len(node_runs) == count
                assert clients == [['run', './deploy/load', *flags, '-nodes', urls, '-disposable']], clients
                assert json.loads(result.stdout) == {'ok': mode == 'ok'}
                assert f'max_connections={count * 10 + 50}' in runs[0]
            else:
                assert not clients
            if 'NEXO_LOAD_IMAGE' in overrides:
                assert not any(a[0] in ('build', 'image') for a in docker)
            if mode == 'invalid':
                assert not events
            if (expected and mode != 'invalid') or 'NEXO_LOAD_OUTPUT' in overrides:
                artifact = Path(result.stderr.split('Load artifacts: ')[-1].strip())
                assert artifact.is_dir(), result.stderr
                assert len(list(artifact.glob('*.log'))) == len(logs)
                if clients:
                    assert (artifact / 'report.json').read_text() == result.stdout
                assert 'a' * 64 not in ''.join(p.read_text() for p in artifact.iterdir())
                shutil.rmtree(artifact)
            assert list(run_tmp.iterdir()) == [sentinel]
        print(f'PASS test-load isolation: {len(scenarios)} fake-command scenarios')


if __name__ == '__main__':
    test_commands()
