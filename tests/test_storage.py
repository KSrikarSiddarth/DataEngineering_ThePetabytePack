"""Tests for the storage helpers on a local folder (the same calls serve abfss:// paths on the cluster)."""
from storage import delete, exists, join, mkdirs, read_text, write_text
from test_spark_transform import spark  # noqa: F401


def test_join_keeps_the_scheme_and_drops_extra_slashes():
    assert join("abfss://lake@acct.dfs.core.windows.net/", "batch", "silver") == \
        "abfss://lake@acct.dfs.core.windows.net/batch/silver"
    assert join("/tmp/out", "gold", 3) == "/tmp/out/gold/3"


def test_text_roundtrip_creates_parents_and_missing_file_reads_as_none(spark, tmp_path):  # noqa: F811
    path = join(tmp_path, "a", "b", "info.json")
    assert read_text(spark, path) is None
    write_text(spark, path, '{"cores": 4}')
    assert read_text(spark, path) == '{"cores": 4}'
    write_text(spark, path, "second")                      # replaces
    assert read_text(spark, path) == "second"


def test_exists_mkdirs_and_delete_of_a_folder_tree(spark, tmp_path):  # noqa: F811
    folder = join(tmp_path, "t", "x=1")
    assert not exists(spark, folder)
    mkdirs(spark, folder)
    write_text(spark, join(folder, "f.txt"), "x")
    assert exists(spark, folder)
    delete(spark, join(tmp_path, "t"))
    assert not exists(spark, folder)
    delete(spark, join(tmp_path, "t"))                     # a missing folder is not an error
