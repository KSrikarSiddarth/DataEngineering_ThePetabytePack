"""Storage helpers shared by the Spark jobs.

The jobs use these instead of pathlib and shutil, so one code path works on a local folder and on ADLS Gen2
(abfss://<container>@<account>.dfs.core.windows.net/<folder>), where executors on other machines read and write
the files. Paths are plain strings, and Hadoop's FileSystem (reached through the Spark JVM) does the work.
"""


def join(base, *parts):
    """`base` and `parts` joined with '/', keeping a scheme such as abfss:// intact."""
    return "/".join([str(base).rstrip("/"), *map(str, parts)])


def _locate(spark, path):
    """The Hadoop FileSystem that holds `path`, and `path` as a Hadoop Path."""
    jpath = spark._jvm.org.apache.hadoop.fs.Path(str(path))
    return jpath.getFileSystem(spark._jsc.hadoopConfiguration()), jpath


def exists(spark, path):
    """True if the file or folder exists."""
    fs, jpath = _locate(spark, path)
    return fs.exists(jpath)


def delete(spark, path):
    """Remove a file or a whole folder tree; nothing happens if it is missing."""
    fs, jpath = _locate(spark, path)
    fs.delete(jpath, True)


def mkdirs(spark, path):
    """Create a folder and its parents (nothing happens if it exists)."""
    fs, jpath = _locate(spark, path)
    fs.mkdirs(jpath)


def read_text(spark, path):
    """The content of a small text file, or None if the file does not exist."""
    fs, jpath = _locate(spark, path)
    if not fs.exists(jpath):
        return None
    stream = fs.open(jpath)
    try:
        return spark._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()


def write_text(spark, path, text):
    """Create or replace a small text file (missing parent folders are created)."""
    fs, jpath = _locate(spark, path)
    stream = fs.create(jpath, True)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()
