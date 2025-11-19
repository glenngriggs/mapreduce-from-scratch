"""See unit test function docstring."""

from pathlib import Path
import utils
from utils import TESTDATA_DIR


def test_many_mappers(mapreduce_client, tmp_path):
    """Run a word count MapReduce job with more mappers and reducers.

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
        "num_mappers": 4,
        "num_reducers": 2
    }, port=mapreduce_client.manager_port)

    # Wait for output to be created
    utils.wait_for_exists(
        f"{tmp_path}/part-00000",
        f"{tmp_path}/part-00001",
    )

    # Verify number of files
    assert len(list(tmp_path.iterdir())) == 2

    # Verify final output file contents
    outfile00 = Path(f"{tmp_path}/part-00000")
    outfile01 = Path(f"{tmp_path}/part-00001")
    word_count_correct = Path(TESTDATA_DIR/"correct/word_count_correct.txt")
    with outfile00.open(encoding="utf-8") as infile:
        outputfile0 = infile.readlines()
    with outfile01.open(encoding="utf-8") as infile:
        outputfile1 = infile.readlines()
    actual = sorted(outputfile0 + outputfile1)
    with word_count_correct.open(encoding="utf-8") as infile:
        correct = sorted(infile.readlines())
    assert actual == correct
