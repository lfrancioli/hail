import argparse
import sys

from . import connect
from . import describe
from . import diagnose
from . import latest
from . import list_clusters
from . import modify
from . import start
from . import stop
from . import submit


def parser():
    main_parser = argparse.ArgumentParser(prog='hailctl dataproc',
                                          description='Manage and monitor Hail deployments.')
    subparsers = main_parser.add_subparsers()

    start_parser = subparsers.add_parser('start',
                                         help='Start a Dataproc cluster configured for Hail.',
                                         description='Start a Dataproc cluster configured for Hail.')
    submit_parser = subparsers.add_parser('submit',
                                          help='Submit a Python script to a running Dataproc cluster.',
                                          description='Submit a Python script to a running Dataproc cluster.')
    connect_parser = subparsers.add_parser('connect',
                                           help='Connect to a running Dataproc cluster.',
                                           description='Connect to a running Dataproc cluster.')
    diagnose_parser = subparsers.add_parser('diagnose',
                                            help='Diagnose problems in a Dataproc cluster.',
                                            description='Diagnose problems in a Dataproc cluster.')
    stop_parser = subparsers.add_parser('stop',
                                        help='Shut down a Dataproc cluster.',
                                        description='Shut down a Dataproc cluster.')

    list_parser = subparsers.add_parser('list',
                                        help='List active Dataproc clusters.',
                                        description='List active Dataproc clusters.')

    modify_parser = subparsers.add_parser('modify',
                                          help='Modify active Dataproc clusters.',
                                          description='Modify active Dataproc clusters.')

    describe_parser = subparsers.add_parser('describe',
                                            help='Gather information about a hail file (including the schema)',
                                            description='Gather information about a hail file (including the schema)')

    latest_parser = subparsers.add_parser('latest',
                                          help='Find the newest deployed SHA and the locations of the newest JARs and ZIPs',
                                          description='Find the newest deployed SHA and the locations of the newest JARs and ZIPs')

    start_parser.set_defaults(module='start')
    start.init_parser(start_parser)

    submit_parser.set_defaults(module='submit')
    submit.init_parser(submit_parser)

    connect_parser.set_defaults(module='connect')
    connect.init_parser(connect_parser)

    diagnose_parser.set_defaults(module='diagnose')
    diagnose.init_parser(diagnose_parser)

    stop_parser.set_defaults(module='stop')
    stop.init_parser(stop_parser)

    list_parser.set_defaults(module='list')

    modify_parser.set_defaults(module='modify')
    modify.init_parser(modify_parser)

    describe_parser.set_defaults(module='describe')
    describe.init_parser(describe_parser)

    latest_parser.set_defaults(module='latest')
    latest.init_parser(latest_parser)
    return main_parser


def main(args):
    if not args:
        parser().print_help()
        sys.exit(0)
    jmp = {
        'start': start,
        'submit': submit,
        'connect': connect,
        'diagnose': diagnose,
        'stop': stop,
        'list': list_clusters,
        'modify': modify,
        'describe': describe,
        'latest': latest
    }

    args, pass_through_args = parser().parse_known_args(args=args)
    jmp[args.module].main(args, pass_through_args)
