#!/usr/bin/env python3

import contextlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import zipfile

import pymysql

import conf
from create_mysql_db import ensure_database
from download_umls import get_extract_dir


STEP_DOWNLOAD = "downloaded"
STEP_EXTRACT = "extracted"
STEP_DB_LOAD = "db_loaded"
STEP_RDF = "rdf_generated"
STATE_VERSION = 1
REQUIRED_META_FILES = (
    "populate_mysql_db.sh",
    "mysql_tables.sql",
    "mysql_indexes.sql",
)
REQUIRED_DB_TABLES = ("MRCONSO", "MRREL", "MRSAB", "MRSTY")
LOG_STREAM = None


def log(message):
    print(message, flush=True)


class TeeStream:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)
        return len(data)

    def flush(self):
        for stream in self.streams:
            stream.flush()

    def isatty(self):
        return any(getattr(stream, "isatty", lambda: False)() for stream in self.streams)


def get_pipeline_work_dir():
    work_dir = getattr(conf, "PIPELINE_WORK_DIR", None)
    if work_dir:
        return Path(work_dir).expanduser().resolve()
    return Path("data/pipeline").resolve() / conf.UMLS_VERSION


def get_state_path():
    return get_pipeline_work_dir() / "state.json"


def get_log_path():
    log_path = getattr(conf, "PIPELINE_LOG_FILE", None)
    if log_path:
        return Path(log_path).expanduser().resolve()
    return get_pipeline_work_dir() / "pipeline.log"


def load_state():
    state_path = get_state_path()
    if not state_path.exists():
        return {"state_version": STATE_VERSION, "steps": {}}
    with state_path.open() as state_file:
        state = json.load(state_file)
    state.setdefault("state_version", STATE_VERSION)
    state.setdefault("steps", {})
    return state


def save_state(state):
    state_path = get_state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=state_path.parent, delete=False
    ) as temp_file:
        json.dump(state, temp_file, indent=2, sort_keys=True)
        temp_file.write("\n")
        temp_path = Path(temp_file.name)
    temp_path.replace(state_path)


def mark_step_complete(state, step_name, details):
    state["steps"][step_name] = details
    save_state(state)


def run_command(command, cwd=None):
    process = subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    try:
        for line in process.stdout:
            LOG_STREAM.write(line)
            LOG_STREAM.flush()
        return_code = process.wait()
    finally:
        if process.stdout is not None:
            process.stdout.close()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)


def get_download_path():
    from umls_downloader import download_umls_full

    download_dir = getattr(conf, "UMLS_DOWNLOAD_DIR", None)
    if download_dir:
        download_dir = Path(download_dir).expanduser().resolve()
        os.environ["BIO_HOME"] = download_dir.parent.as_posix()

    path = download_umls_full(
        version=conf.UMLS_VERSION.upper(),
        api_key=conf.UMLS_API_KEY,
    )
    return Path(path).resolve()


def find_meta_dir(extract_dir):
    candidates = []
    for script_path in extract_dir.rglob("populate_mysql_db.sh"):
        if script_path.parent.name == "META":
            candidates.append(script_path.parent.resolve())
    if not candidates:
        raise RuntimeError(
            "Could not find META/populate_mysql_db.sh under %s" % extract_dir
        )
    if len(candidates) > 1:
        candidates.sort(key=lambda path: len(path.parts))
    return candidates[0]


def extracted_release_is_ready(extract_dir):
    if not extract_dir.exists():
        return False, None
    try:
        meta_dir = find_meta_dir(extract_dir)
    except RuntimeError:
        return False, None
    for file_name in REQUIRED_META_FILES:
        if not (meta_dir / file_name).exists():
            return False, None
    return True, meta_dir


def extract_release(zip_path, extract_dir):
    ready, meta_dir = extracted_release_is_ready(extract_dir)
    if ready:
        log("Skipping extraction; found existing META directory at %s" % meta_dir)
        return meta_dir

    extract_dir.mkdir(parents=True, exist_ok=True)
    log("Extracting %s into %s" % (zip_path, extract_dir))
    with zipfile.ZipFile(zip_path) as zip_file:
        zip_file.extractall(extract_dir)

    ready, meta_dir = extracted_release_is_ready(extract_dir)
    if not ready:
        raise RuntimeError("Extraction completed but required META files were not found.")
    return meta_dir


def shell_assignment(name, value):
    return "%s=%s" % (name, shlex.quote(str(value)))


def get_mysql_client_flags():
    extra_flags = getattr(conf, "MYSQL_CLIENT_FLAGS", ())
    if isinstance(extra_flags, str):
        extra_flags = shlex.split(extra_flags)
    return " ".join(shlex.quote(flag) for flag in extra_flags)


def patch_sql_template_in_place(meta_dir):
    sql_path = meta_dir / "mysql_tables.sql"
    content = sql_path.read_text()
    patched = content.replace("@LINE_TERMINATION@", "'\\n'")
    if patched != content:
        sql_path.write_text(patched)
        log("Patched @LINE_TERMINATION@ placeholders in %s" % sql_path)


def render_loader_script(source_path):
    replacements = {
        "MYSQL_HOME": getattr(conf, "MYSQL_HOME"),
        "user": conf.DB_USER,
        "password": conf.DB_PASS,
        "db_name": conf.DB_NAME,
    }
    lines = source_path.read_text().splitlines()
    output_lines = []
    mysql_client_flags = get_mysql_client_flags()
    for line in lines:
        stripped = line.strip()
        replaced = False
        for key, value in replacements.items():
            if stripped.startswith(key + "="):
                output_lines.append(shell_assignment(key, value))
                replaced = True
                break
        if not replaced and "$MYSQL_HOME/bin/mysql -vvv" in line and mysql_client_flags:
            output_lines.append(
                line.replace(
                    "$MYSQL_HOME/bin/mysql -vvv",
                    "$MYSQL_HOME/bin/mysql -vvv %s" % mysql_client_flags,
                )
            )
            replaced = True
        if not replaced:
            output_lines.append(line)
    missing = [key for key in replacements if not any(l.startswith(key + "=") for l in output_lines)]
    if missing:
        raise RuntimeError(
            "Could not patch loader script; missing assignments for %s" % ", ".join(missing)
        )
    return "\n".join(output_lines) + "\n"


def patch_loader_script_in_place(script_path):
    script_path.write_text(render_loader_script(script_path))
    script_path.chmod(0o755)


def run_loader(meta_dir):
    ensure_database(conf.DB_NAME, recreate=True)
    patch_sql_template_in_place(meta_dir)
    loader_script = meta_dir / "populate_mysql_db.sh"
    patch_loader_script_in_place(loader_script)

    log("Loading UMLS tables into MySQL database %s" % conf.DB_NAME)
    run_command(
        [loader_script.as_posix()],
        cwd=meta_dir,
    )


def validate_loaded_database():
    try:
        connection = pymysql.connect(
            host=conf.DB_HOST,
            user=conf.DB_USER,
            passwd=conf.DB_PASS,
            db=conf.DB_NAME,
            charset="utf8mb4",
        )
    except pymysql.MySQLError:
        return False
    try:
        with connection.cursor() as cursor:
            for table_name in REQUIRED_DB_TABLES:
                cursor.execute("SHOW TABLES LIKE %s", (table_name,))
                if cursor.fetchone() is None:
                    return False
            cursor.execute("SELECT COUNT(*) FROM MRCONSO")
            row = cursor.fetchone()
            return bool(row and row[0] > 0)
    except pymysql.MySQLError:
        return False
    finally:
        connection.close()


def get_rdf_output_dir():
    return Path(conf.OUTPUT_FOLDER).expanduser().resolve()


def rdf_output_is_ready():
    output_dir = get_rdf_output_dir()
    if not output_dir.exists():
        return False
    return any(output_dir.glob("*.ttl"))


def run_umls2rdf():
    log("Running umls2rdf.py")
    run_command([sys.executable, "-u", "umls2rdf.py"])


def validate_config():
    missing = []
    for attr in ("UMLS_VERSION", "UMLS_API_KEY", "DB_HOST", "DB_NAME", "DB_USER", "DB_PASS", "MYSQL_HOME"):
        if not getattr(conf, attr, None):
            missing.append(attr)
    if missing:
        raise RuntimeError("Missing required conf.py settings: %s" % ", ".join(missing))

    mysql_bin = Path(conf.MYSQL_HOME).expanduser().resolve() / "bin/mysql"
    if not mysql_bin.exists():
        raise RuntimeError("mysql client not found at %s" % mysql_bin)


def main():
    validate_config()
    state = load_state()

    zip_path = get_download_path()
    mark_step_complete(
        state,
        STEP_DOWNLOAD,
        {"archive_path": zip_path.as_posix()},
    )
    log("Archive ready at %s" % zip_path)

    extract_dir = get_extract_dir(zip_path)
    meta_dir = extract_release(zip_path, extract_dir)
    mark_step_complete(
        state,
        STEP_EXTRACT,
        {
            "extract_dir": extract_dir.as_posix(),
            "meta_dir": meta_dir.as_posix(),
        },
    )

    if state["steps"].get(STEP_DB_LOAD) and validate_loaded_database():
        log("Skipping MySQL load; database %s is already populated." % conf.DB_NAME)
    else:
        run_loader(meta_dir)
        if not validate_loaded_database():
            raise RuntimeError("MySQL load completed but validation failed.")
        mark_step_complete(
            state,
            STEP_DB_LOAD,
            {"db_name": conf.DB_NAME},
        )

    if state["steps"].get(STEP_RDF) and rdf_output_is_ready():
        log("Skipping RDF export; output already exists at %s" % get_rdf_output_dir())
    else:
        run_umls2rdf()
        if not rdf_output_is_ready():
            raise RuntimeError("umls2rdf.py finished but no TTL output was found.")
        mark_step_complete(
            state,
            STEP_RDF,
            {"output_dir": get_rdf_output_dir().as_posix()},
        )

    log("Pipeline completed successfully.")


if __name__ == "__main__":
    log_path = get_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as log_file:
        tee_stream = TeeStream(sys.__stdout__, log_file)
        tee_error_stream = TeeStream(sys.__stderr__, log_file)
        LOG_STREAM = tee_stream
        with contextlib.redirect_stdout(tee_stream), contextlib.redirect_stderr(tee_error_stream):
            log("===== Pipeline started =====")
            log("Log file: %s" % log_path)
            try:
                main()
            except Exception:
                log("Pipeline failed.")
                raise
