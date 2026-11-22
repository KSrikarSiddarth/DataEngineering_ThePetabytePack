"""Cluster mode (SetupF): s3a:// paths, the setupf profile, settings-file mapping and the Spark session settings.

No MinIO or cluster is needed: the object store is replaced by a local folder behind pyarrow's SubTreeFileSystem.
"""
import pandas as pd
import pyarrow.fs as pafs
import pytest

from mobilitypulse.common import storage
from mobilitypulse.common.config import apply_cluster_env, load_config, load_env_file
from mobilitypulse.common.storage import RemotePath


class _S3Like(pafs.FileSystemHandler):
    """A local folder that behaves like an object store: writing a key creates its 'folders' implicitly."""

    def __init__(self, root):
        self.fs = pafs.SubTreeFileSystem(str(root), pafs.LocalFileSystem())

    def get_type_name(self): return "s3like"
    def normalize_path(self, path): return path
    def __eq__(self, other): return isinstance(other, _S3Like) and other.fs.base_path == self.fs.base_path
    def __ne__(self, other): return not self == other
    def get_file_info(self, paths): return self.fs.get_file_info(paths)
    def get_file_info_selector(self, selector): return self.fs.get_file_info(selector)
    def create_dir(self, path, recursive): self.fs.create_dir(path, recursive=recursive)
    def delete_dir(self, path): self.fs.delete_dir(path)
    def delete_dir_contents(self, path, missing_dir_ok=False): self.fs.delete_dir_contents(path, missing_dir_ok=missing_dir_ok)
    def delete_root_dir_contents(self): self.fs.delete_dir_contents("", accept_root_dir=True)
    def delete_file(self, path): self.fs.delete_file(path)
    def move(self, src, dest): self.fs.move(src, dest)
    def copy_file(self, src, dest): self.fs.copy_file(src, dest)
    def open_input_stream(self, path): return self.fs.open_input_stream(path)
    def open_input_file(self, path): return self.fs.open_input_file(path)

    def open_output_stream(self, path, metadata):
        self.fs.create_dir(path.rsplit("/", 1)[0], recursive=True)
        return self.fs.open_output_stream(path, metadata=metadata)

    def open_append_stream(self, path, metadata): return self.fs.open_append_stream(path, metadata=metadata)


@pytest.fixture
def fake_s3(tmp_path, monkeypatch):
    root = tmp_path / "store"
    (root / "lake").mkdir(parents=True)
    fs = pafs.PyFileSystem(_S3Like(root))
    monkeypatch.setattr(storage, "s3_filesystem", lambda: fs)
    return root


def test_remote_path_behaves_like_a_path():
    p = RemotePath("s3a://lake/mp") / "gold" / "forecasts"
    assert str(p) == "s3a://lake/mp/gold/forecasts"
    assert p.name == "forecasts"
    assert str(p.parent) == "s3a://lake/mp/gold"
    assert str(RemotePath("s3://lake//mp/")) == "s3a://lake/mp"       # scheme and slashes are normalised
    assert str(RemotePath("s3a://lake").parent) == "s3a://lake"
    assert p == RemotePath("s3a://lake/mp/gold/forecasts") and len({p, RemotePath(str(p))}) == 1
    with pytest.raises(ValueError):
        RemotePath("/just/a/folder")


def test_remote_path_file_operations(fake_s3):
    base = RemotePath("s3a://lake/mp")
    f = base / "models" / "best_model.json"
    assert not f.exists()
    f.write_text('{"model": "gbt"}', encoding="utf-8")
    assert f.exists() and f.read_text(encoding="utf-8") == '{"model": "gbt"}'
    for m in ("2024-01", "2024-02"):
        (base / "d" / f"month={m}" / "part.txt").write_text(m)
    assert [c.name for c in (base / "d").glob("month=*")] == ["month=2024-01", "month=2024-02"]
    base.mkdir(parents=True, exist_ok=True)   # no-op on an object store
    storage.rmtree(base / "d")
    assert not (base / "d").exists() and f.exists()
    storage.rmtree(base / "missing")          # missing target: nothing happens


def test_pandas_roundtrip_and_upload(fake_s3, tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    target = RemotePath("s3a://lake/mp/gold/model_metrics") / "model_metrics.parquet"
    storage.write_parquet_pandas(df, target)
    pd.testing.assert_frame_equal(storage.read_parquet_pandas(target), df)

    src = tmp_path / "raw"
    (src / "tlc").mkdir(parents=True)
    (src / "tlc" / "a.parquet").write_bytes(b"PAR1aaaa")
    (src / "tlc" / "b.parquet.part").write_bytes(b"half")      # unfinished downloads are never uploaded
    dst = RemotePath("s3a://lake/mp/raw")
    assert storage.upload_tree(src, dst) == 1
    assert (dst / "tlc" / "a.parquet").exists() and not (dst / "tlc" / "b.parquet.part").exists()
    assert storage.upload_tree(src, dst) == 0                   # unchanged files are skipped
    (src / "tlc" / "a.parquet").write_bytes(b"PAR1aaaaaaaa")
    assert storage.upload_tree(src, dst) == 1                   # a changed file is sent again


def test_config_with_s3a_data_dir(tmp_path):
    cfg = load_config(overrides={"paths": {"data_dir": "s3a://lake/mobilitypulse", "local_dir": str(tmp_path)}})
    assert isinstance(cfg.path("bronze"), RemotePath) and str(cfg.path("gold")) == "s3a://lake/mobilitypulse/gold"
    assert str(cfg.path("raw_lake")) == "s3a://lake/mobilitypulse/raw"
    # what must stay on the driver machine is local
    assert cfg.path("raw") == tmp_path / "raw" and cfg.path("state") == tmp_path / "state"
    assert cfg.path("serving_db") == tmp_path / "serving" / "mobilitypulse.db"


def test_local_mode_is_unchanged(tmp_path):
    cfg = load_config(overrides={"paths": {"data_dir": str(tmp_path)}})
    assert cfg.local_dir == tmp_path and cfg.path("raw") == cfg.path("raw_lake") == tmp_path / "raw"
    assert not cfg.is_cluster


def test_cluster_env_mapping():
    out = apply_cluster_env({"spark": {"master": "local[*]"}}, {
        "MASTER_IP": "10.0.0.4", "DRIVER_MEMORY": "2g", "EXECUTOR_MEMORY": "3g", "EXECUTOR_CORES": "3",
        "TOTAL_CORES": "3", "S3_ENDPOINT": "http://10.0.0.5:9000", "S3_ACCESS_KEY": "k", "S3_SECRET_KEY": "s",
        "KAFKA_BOOTSTRAP": "10.0.0.5:9092", "DRIVER_HOST": "", "EXECUTOR_PYTHON": "python3"})
    assert out["spark"]["master"] == "spark://10.0.0.4:7077" and out["spark"]["driver_host"] == "10.0.0.4"
    assert out["spark"]["executor_cores"] == 3 and out["spark"]["cores_max"] == 3
    assert out["storage"]["s3_endpoint"] == "http://10.0.0.5:9000" and out["kafka"]["bootstrap_servers"] == "10.0.0.5:9092"
    # DRIVER_HOST (driver on VM2) wins over MASTER_IP
    assert apply_cluster_env({}, {"MASTER_IP": "10.0.0.4", "DRIVER_HOST": "10.0.0.5"})["spark"]["driver_host"] == "10.0.0.5"
    # an empty settings file leaves local mode alone
    assert apply_cluster_env({"spark": {"master": "local[*]"}}, {})["spark"]["master"] == "local[*]"


def test_setupf_profile(monkeypatch, tmp_path):
    env_file = tmp_path / "setupF.env"
    env_file.write_text("# comment\nS3_ENDPOINT=http://10.0.0.5:9000\nS3_ACCESS_KEY=sparkadmin\n"
                        "S3_SECRET_KEY=pw123456\nMASTER_IP=10.0.0.4\nKAFKA_BOOTSTRAP=\n")
    for name in ("MASTER_IP", "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "KAFKA_BOOTSTRAP", "MOBILITYPULSE_DATA_DIR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MP_PROFILE", "setupf")
    monkeypatch.setenv("MP_ENV_FILE", str(env_file))
    cfg = load_config()
    assert cfg.is_cluster and cfg.section("spark")["master"] == "spark://10.0.0.4:7077"
    assert str(cfg.data_dir) == "s3a://lake/mobilitypulse"
    assert cfg.section("kafka")["bootstrap_servers"] == "localhost:9092"        # empty value keeps the default
    assert storage.s3_settings()["endpoint"] == "http://10.0.0.5:9000"
    monkeypatch.setenv("MP__SPARK__EXECUTOR_MEMORY", "2g")                      # MP__ overrides still win
    assert load_config().section("spark")["executor_memory"] == "2g"


@pytest.fixture(autouse=True)
def _restore_environ():
    """load_config(profile) copies settings-file values into os.environ; undo that after every test in this module."""
    import os
    before = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(before)


def test_unknown_profile(monkeypatch):
    monkeypatch.setenv("MP_PROFILE", "nope")
    with pytest.raises(FileNotFoundError):
        load_config()


def test_load_env_file_does_not_override(monkeypatch, tmp_path):
    f = tmp_path / "e"
    f.write_text("A_ONE=file\nA_TWO='quoted'\n")
    monkeypatch.setenv("A_ONE", "shell")
    monkeypatch.delenv("A_TWO", raising=False)
    assert load_env_file(f) == {"A_ONE": "file", "A_TWO": "quoted"}
    import os
    assert os.environ["A_ONE"] == "shell" and os.environ["A_TWO"] == "quoted"
    monkeypatch.delenv("A_TWO")


def test_spark_session_settings_for_cluster(monkeypatch):
    from mobilitypulse.common import spark as sp

    cfg = load_config(overrides={"paths": {"data_dir": "s3a://lake/x"}, "spark": {"master": "spark://10.0.0.4:7077"},
                                 "storage": {"s3_endpoint": "http://10.0.0.5:9000", "s3_access_key": "k",
                                             "s3_secret_key": "s"}})
    conf = sp._s3a_conf(cfg)
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://10.0.0.5:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.hadoop.fs.s3a.aws.credentials.provider"].endswith("SimpleAWSCredentialsProvider")
    with pytest.raises(RuntimeError, match="driver_host"):          # executors could not call back without it
        sp.get_spark(cfg)
    zip_path = sp._package_zip()
    import zipfile
    names = zipfile.ZipFile(zip_path).namelist()
    assert "mobilitypulse/ml/metrics.py" in names and "mobilitypulse/common/storage.py" in names


def test_profile_finds_setupF_env_in_the_project_folder(monkeypatch, tmp_path):
    """The default settings file is <project>/setupF.env (found even though the profile name is lower case)."""
    from mobilitypulse.common import config as cfgmod

    (tmp_path / "setupF.env").write_text("MASTER_IP=10.9.8.7\nS3_ENDPOINT=http://10.9.8.6:9000\n")
    monkeypatch.setattr(cfgmod, "PROJECT_ROOT", tmp_path)
    for name in ("MASTER_IP", "S3_ENDPOINT", "MP_ENV_FILE", "MOBILITYPULSE_DATA_DIR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MP_PROFILE", "setupf")
    assert cfgmod._profile_env_file("setupf") == tmp_path / "setupF.env"
    assert load_config().section("spark")["master"] == "spark://10.9.8.7:7077"
