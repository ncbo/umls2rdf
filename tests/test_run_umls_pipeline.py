import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import pymysql

import conf
import run_umls_pipeline
from run_umls_pipeline import expected_row_counts, rdf_output_is_ready, validate_loaded_database
from umls2rdf import MANIFEST_FILENAME


MRFILES = """\
MRCONSO.RRF|Concept names and sources|CUI,LAT|18|300|9000|
MRREL.RRF|Related Concepts|CUI1,AUI1|16|500|9000|
CHANGE/DELETEDCUI.RRF|Deleted concepts|PCUI,PSTR|2|7|100|
MRHIER.RRF|Computable hierarchies|CUI,AUI|9|42|100|
"""


class ExpectedRowCountsTest(unittest.TestCase):
    def test_reads_counts_for_tables_umls2rdf_uses(self):
        with tempfile.TemporaryDirectory() as meta_dir:
            Path(meta_dir, "MRFILES.RRF").write_text(MRFILES)
            self.assertEqual(expected_row_counts(meta_dir), {"MRCONSO": 300, "MRREL": 500})

    def test_missing_mrfiles(self):
        with tempfile.TemporaryDirectory() as meta_dir:
            self.assertEqual(expected_row_counts(meta_dir), {})


class FakeCursor:
    def __init__(self, counts):
        self.counts = counts
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, args=None):
        if query.startswith("SHOW TABLES"):
            self.result = (args[0],) if args[0] in self.counts else None
        else:
            table_name = query.split("`")[1]
            self.result = (self.counts[table_name],)

    def fetchone(self):
        return self.result


class ValidateLoadedDatabaseTest(unittest.TestCase):
    def validate(self, counts, expected):
        connection = mock.Mock()
        connection.cursor.return_value = FakeCursor(counts)
        with mock.patch.object(pymysql, "connect", return_value=connection), \
                mock.patch.object(run_umls_pipeline, "expected_row_counts", return_value=expected), \
                mock.patch.object(run_umls_pipeline, "log") as log:
            result = validate_loaded_database("META")
        connection.close.assert_called_once()
        return result, [call.args[0] for call in log.call_args_list]

    def all_tables(self, rows=10):
        return {name: rows for name in run_umls_pipeline.REQUIRED_DB_TABLES}

    def test_passes_when_counts_match_mrfiles(self):
        result, _ = self.validate(self.all_tables(), {"MRCONSO": 10, "MRREL": 10})
        self.assertTrue(result)

    def test_fails_on_partial_load(self):
        result, messages = self.validate(self.all_tables(), {"MRREL": 11})
        self.assertFalse(result)
        self.assertIn("table MRREL has 10 rows, MRFILES.RRF lists 11", messages[-1])

    def test_fails_on_missing_table(self):
        counts = self.all_tables()
        del counts["MRSAT"]
        result, messages = self.validate(counts, {})
        self.assertFalse(result)
        self.assertIn("table MRSAT is missing", messages[-1])

    def test_fails_on_empty_table(self):
        counts = self.all_tables()
        counts["MRDEF"] = 0
        result, messages = self.validate(counts, {})
        self.assertFalse(result)
        self.assertIn("table MRDEF is empty", messages[-1])


class RdfOutputIsReadyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.tmp.name)
        patcher = mock.patch.object(run_umls_pipeline, "get_rdf_output_dir", return_value=self.output_dir)
        patcher.start()
        self.addCleanup(patcher.stop)
        version = mock.patch.object(conf, "UMLS_VERSION", "2025AB")
        version.start()
        self.addCleanup(version.stop)
        self.addCleanup(self.tmp.cleanup)

    def write_manifest(self, umls_version="2025AB"):
        manifest = {
            "umls_version": umls_version,
            "semantic_types_file": "umls_semantictypes.ttl",
            "ontologies": [{"sab": "AIR", "file": "AI-RHEUM.ttl"}],
            "skipped": [],
        }
        (self.output_dir / MANIFEST_FILENAME).write_text(json.dumps(manifest))

    def write_ttl(self, name):
        (self.output_dir / name).write_text("@prefix x: <http://x/> .\n")

    def test_semantic_types_file_alone_is_not_enough(self):
        self.write_ttl("umls_semantictypes.ttl")
        self.assertFalse(rdf_output_is_ready())

    def test_ready_when_manifest_files_exist(self):
        self.write_ttl("umls_semantictypes.ttl")
        self.write_ttl("AI-RHEUM.ttl")
        self.write_manifest()
        self.assertTrue(rdf_output_is_ready())

    def test_not_ready_when_listed_file_missing_or_empty(self):
        self.write_ttl("umls_semantictypes.ttl")
        self.write_manifest()
        self.assertFalse(rdf_output_is_ready())
        (self.output_dir / "AI-RHEUM.ttl").write_text("")
        self.assertFalse(rdf_output_is_ready())

    def test_not_ready_for_other_umls_version(self):
        self.write_ttl("umls_semantictypes.ttl")
        self.write_ttl("AI-RHEUM.ttl")
        self.write_manifest(umls_version="2025AA")
        self.assertFalse(rdf_output_is_ready())


if __name__ == "__main__":
    unittest.main()
