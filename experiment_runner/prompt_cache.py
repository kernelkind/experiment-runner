"""Local eligibility for the tested subscription prefix workflow."""
import subprocess


def prefix_policy(prefix):
    try:
        import tiktoken
    except ImportError:
        # Like run-model.sh's Node fallback, use a dependency runtime, never a
        # packaged copy of this runner. Prefix text stays on stdin.
        result = subprocess.run([
            'nix', 'shell', '--impure', '--expr',
            'let pkgs = (builtins.getFlake "nixpkgs").legacyPackages.${builtins.currentSystem}; '
            'in pkgs.python3.withPackages (ps: [ps.tiktoken])',
            '--command', 'python3', '-c',
            'import sys,tiktoken; print(len(tiktoken.get_encoding("o200k_base").encode(sys.stdin.read(), disallowed_special=())))',
        ], input=prefix, text=True, capture_output=True)
        if result.returncode:
            raise ValueError('prefix token counting requires tiktoken (or its Nix runtime)')
        count = int(result.stdout.strip())
    else:
        count = len(tiktoken.get_encoding('o200k_base').encode(prefix, disallowed_special=()))
    return dict(version='automatic-prefix-warmup-v1', tokenizer='o200k_base',
                estimated_prefix_tokens=count, minimum_prefix_tokens=1024,
                eligible=count >= 1024)
