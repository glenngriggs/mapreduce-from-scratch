"""See unit test function docstring."""

from pathlib import Path
import utils
from utils import TESTDATA_DIR


def test_wordcount(mapreduce_client, tmp_path):
    """Run a word count MapReduce job.

    Note: 'mapreduce_client' is a fixture function that starts a fresh Manager
    and Workers.  It is implemented in conftest.py and reused by many tests.
    Docs: https://docs.pytest.org/en/latest/fixture.html

    Note: 'tmp_path' is a fixture provided by the pytest-mock package.  This
    fixture creates a temporary directory for use within this test.  See
    https://docs.pytest.org/en/6.2.x/tmpdir.html for more info.

    """
    utils.send_message({
        "message_type": "new_manager_job",
        "input_directory": TESTDATA_DIR/"input",
        "output_directory": tmp_path,
        "mapper_executable": TESTDATA_DIR/"exec/wc_map.sh",
        "reducer_executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "num_mappers": 2,
        "num_reducers": 1
    }, port=mapreduce_client.manager_port)

    # Wait for output to be created
    utils.wait_for_exists(f"{tmp_path}/part-00000")

    # Verify final output file contents
    outfile00 = Path(f"{tmp_path}/part-00000")
    word_count_correct = Path(TESTDATA_DIR/"correct/word_count_correct.txt")
    with outfile00.open(encoding="utf-8") as infile:
        actual = sorted(infile.readlines())
    with word_count_correct.open(encoding="utf-8") as infile:
        correct = sorted(infile.readlines())
    assert actual == correct
