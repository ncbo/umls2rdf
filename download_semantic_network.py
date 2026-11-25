#!/usr/bin/env python3
"""Download the UMLS Semantic Network from NLM and save the two files
umls2rdf.py reads when ADD_STY_DEFINITIONS_AND_RELATIONS is on, SRDEF and
SRSTR, into SEMANTIC_NETWORK_DIR from conf.py (default data/semantic-network),
or into the folder given as the first argument. No UMLS API key is needed.

The Semantic Network has not changed since UMLS 2015AA, so this is needed
once per server, not once per UMLS release."""

import io
import os
import sys
import tarfile
import urllib.request

SEMANTIC_NETWORK_URL = ("https://www.nlm.nih.gov/research/umls/knowledge_sources/"
                        "semantic_network/sn_current.tgz")
SEMANTIC_NETWORK_FILES = ("SRDEF", "SRSTR")
DEFAULT_SEMANTIC_NETWORK_DIR = "data/semantic-network"


def extract_semantic_network(archive, dest):
    """Save SRDEF and SRSTR from the sn_current.tgz file object into dest,
    and return the archive's release folder name (e.g. 2023AA)."""
    found, release = {}, None
    with tarfile.open(fileobj=archive, mode="r:gz") as tar:
        for member in tar.getmembers():
            name = os.path.basename(member.name)
            if member.isfile() and name in SEMANTIC_NETWORK_FILES:
                found[name] = tar.extractfile(member).read()
                release = os.path.dirname(member.name) or release
    missing = [name for name in SEMANTIC_NETWORK_FILES if name not in found]
    if missing:
        raise ValueError("Semantic Network archive has no %s" % ", ".join(missing))
    os.makedirs(dest, exist_ok=True)
    for name, data in found.items():
        with open(os.path.join(dest, name), "wb") as f:
            f.write(data)
    return release


def main():
    if len(sys.argv) > 1:
        dest = sys.argv[1]
    else:
        import conf
        dest = getattr(conf, "SEMANTIC_NETWORK_DIR", DEFAULT_SEMANTIC_NETWORK_DIR)
    dest = os.path.expanduser(dest)
    with urllib.request.urlopen(SEMANTIC_NETWORK_URL, timeout=300) as response:
        archive = io.BytesIO(response.read())
    release = extract_semantic_network(archive, dest)
    print("Semantic Network %s: %s saved to %s" % (
        release, " and ".join(SEMANTIC_NETWORK_FILES), dest))


if __name__ == "__main__":
    main()
