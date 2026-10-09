"""Select project adapters explicitly while sharing one executor."""
import argparse
from pathlib import Path
from . import __version__, project


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--project-root', type=Path)
    parser.add_argument('--version', action='store_true')
    options, remaining = parser.parse_known_args(argv)
    if options.version:
        print(__version__)
        return 0
    project.configure(options.project_root)
    from . import experiment
    return experiment.main(remaining)


if __name__ == '__main__':
    raise SystemExit(main())
