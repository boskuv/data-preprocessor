from operator import contains
import optparse
import sys
import app.processing.handlers as handlers
from inspect import getmembers, isfunction


parser = optparse.OptionParser(usage="Usage: %prog [options]")

parser.add_option("-c", "--config", type=str, help="path to configuration file")


passed_args = [arg for arg in sys.argv]

possible_args = list()
for option in parser._get_all_options()[1:]:
    possible_args.extend(option._short_opts)
    possible_args.extend(option._long_opts)


def resolve_cmd_args():
    if len(sys.argv) == 1 or len(set(possible_args).intersection(passed_args)) == 0:
        parser.print_help()
        sys.exit()

    options, _ = parser.parse_args()
    return options
