"""
MapReduce job submission script.

Before using this script, start the MapReduce server.
$ ./bin/mapreduce start

Then, submit a job.  Everything has a default.
$ mapreduce-submit

You can change any of the options.
$ mapreduce-submit --help
"""

import sys
import socket
import json
import click


# Configure command line options
@click.command()
@click.option(
    "--host", "-h", "host", default="localhost",
    help="Manager host, default=localhost",
)
@click.option(
    "--port", "-p", "port", default=6000,
    help="Manager port number, default=6000",
)
@click.option(
    "--input", "-i", "input_directory", default="tests/testdata/input",
    help="Input directory, default=tests/testdata/input",
    type=click.Path(file_okay=False, dir_okay=True),
)
@click.option(
    "--output", "-o", "output_directory", default="output",
    help="Output directory, default=output",
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
)
@click.option(
    "--mapper", "-m", "mapper_executable",
    default="tests/testdata/exec/wc_map.sh",
    help="Mapper executable, default=tests/testdata/exec/wc_map.sh",
    type=click.Path(file_okay=True, dir_okay=False),
)
@click.option(
    "--reducer", "-r", "reducer_executable",
    default="tests/testdata/exec/wc_reduce.sh",
    help="Reducer executable, default=tests/testdata/exec/wc_reduce.sh",
    type=click.Path(file_okay=True, dir_okay=False),
)
@click.option(
    "--nmappers", "num_mappers", default=2, type=int,
    help="Number of mappers, default=2",
)
@click.option(
    "--nreducers", "num_reducers", default=2, type=int,
    help="Number of reducers, default=2",
)
@click.option("--shutdown", "-s", is_flag=True, help="Shutdown the server.")
def main(
    *,
    host: str,
    port: int,
    input_directory: str,
    output_directory: str,
    mapper_executable: str,
    reducer_executable: str,
    num_mappers: int,
    num_reducers: int,
    shutdown: bool,
) -> None:
    """Top level command line interface."""
    # We want a bunch of arguments, this is the top level CLI.
    # pylint: disable=too-many-arguments
    if shutdown:
        message = json.dumps({
            "message_type": "shutdown",
        })
    else:
        message = json.dumps({
            "message_type": "new_manager_job",
            "input_directory": input_directory,
            "output_directory": output_directory,
            "mapper_executable": mapper_executable,
            "reducer_executable": reducer_executable,
            "num_mappers": num_mappers,
            "num_reducers": num_reducers
        })

    # Send the data to the port that Manager is on
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            sock.sendall(str.encode(message))
    except socket.error as err:
        sys.exit(f"Failed to send message to Manager: {err}")

    # Print to CLI
    if shutdown:
        print(f"Shut down Manager {host}:{port}")
    else:
        print(f"Submitted job to Manager {host}:{port}")
        print("input directory     ", input_directory)
        print("output directory    ", output_directory)
        print("mapper executable   ", mapper_executable)
        print("reducer executable  ", reducer_executable)
        print("num mappers         ", num_mappers)
        print("num reducers        ", num_reducers)


if __name__ == "__main__":
    # Click will provide the arguments, disable this pylint check.
    # pylint: disable=no-value-for-parameter
    main()
