#!/usr/bin/env python3

import os
from pathlib import Path
import zipfile

import conf


def get_extract_dir(zip_path):
    extract_dir = getattr(conf, "UMLS_EXTRACT_DIR", None)
    if extract_dir:
        return Path(extract_dir).expanduser().resolve()
    return zip_path.parent / "extracted"


def main():
    from umls_downloader import download_umls_full

    download_dir = getattr(conf, "UMLS_DOWNLOAD_DIR", None)
    if download_dir:
        download_dir = Path(download_dir).expanduser().resolve()
        os.environ["BIO_HOME"] = download_dir.parent.as_posix()

    path = download_umls_full(
        version=conf.UMLS_VERSION.upper(),
        api_key=conf.UMLS_API_KEY,
    )
    extract_dir = get_extract_dir(path)
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(path) as zip_file:
        zip_file.extractall(extract_dir)

    print(extract_dir)


if __name__ == "__main__":
    main()
