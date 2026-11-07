import collections
import io
import json
import os
import tempfile
import types
import unittest
from unittest import mock

import pymysql.cursors

import conf
from umls2rdf import (
    MANIFEST_FILENAME,
    UmlsClass,
    UmlsOntology,
    UmlsTable,
    export_ontologies,
    get_language_tag,
    get_rel_code_source,
    get_rel_code_target,
    ontologies_over_error_limit,
    remove_manifest,
    write_manifest,
)


def make_atom(cui, label, ispref="", stt="", tty=""):
    atom = [""] * 15
    atom[0] = cui
    atom[4] = stt
    atom[7] = ""
    atom[6] = ispref
    atom[12] = tty
    atom[13] = cui
    atom[14] = label
    return atom


def make_code_atom(cui, aui, code, label, ispref="", stt="", tty=""):
    atom = make_atom(cui, label, ispref=ispref, stt=stt, tty=tty)
    atom[7] = aui
    atom[13] = code
    return atom


def make_rel(source_cui, target_cui, rel, rela=""):
    row = [""] * 8
    row[0] = target_cui
    row[3] = rel
    row[4] = source_cui
    row[7] = rela
    return row


def make_code_rel(source_cui, source_aui, target_cui, target_aui, rel, rela=""):
    row = [""] * 8
    row[0] = target_cui
    row[1] = target_aui
    row[3] = rel
    row[4] = source_cui
    row[5] = source_aui
    row[7] = rela
    return row


def make_sty(cui, tui):
    row = [""] * 2
    row[0] = cui
    row[1] = tui
    return row


def make_att(cui, atn, atv):
    row = [""] * 11
    row[0] = cui
    row[8] = atn
    row[10] = atv
    return row


def make_code_att(code, atn, atv):
    row = make_att("", atn, atv)
    row[5] = code
    return row


class UmlsClassDedupeRegressionTest(unittest.TestCase):
    def make_term(self, rels):
        cui = "C0001"
        return UmlsClass(
            "http://example.org/test",
            atoms=[make_atom(cui, "Preferred label")],
            rels=rels,
            defs=[],
            atts=[],
            rank=[],
            rank_by_tty={},
            sty=[make_sty(cui, "T001")],
            sty_by_cui={cui: [0]},
            load_on_cuis=True,
            is_root=False,
        )

    def render_term(self, term, dedupe_enabled, tree=None):
        with mock.patch.object(
            conf,
            "DEDUPE_CLASS_TRIPLES",
            dedupe_enabled,
            create=True,
        ):
            return term.toRDF(tree=tree)

    def test_dedupes_duplicate_literal_triples_in_load_on_codes_mode(self):
        term = UmlsClass(
            "http://example.org/test",
            atoms=[make_code_atom("C0001", "A001", "CODE1", "Preferred label", tty="PT")],
            rels=[],
            defs=[],
            atts=[
                make_code_att("CODE1", "TH", "NLM (1994)"),
                make_code_att("CODE1", "TH", "NLM (1994)"),
            ],
            rank=[],
            rank_by_tty={},
            sty=[make_sty("C0001", "T001")],
            sty_by_cui={"C0001": [0]},
            load_on_cuis=False,
            is_root=False,
        )

        rdf_without_dedupe = self.render_term(term, dedupe_enabled=False)
        rdf_with_dedupe = self.render_term(term, dedupe_enabled=True)

        expected = '<http://example.org/test/TH> """NLM (1994)"""^^xsd:string ;'
        self.assertEqual(rdf_without_dedupe.count(expected), 2)
        self.assertEqual(rdf_with_dedupe.count(expected), 1)

    def test_dedupes_duplicate_subclass_triples_in_load_on_cuis_mode(self):
        rel = make_rel("C0001", "CParent", "CHD")
        term = self.make_term([rel, make_rel("C0001", "CParent", "CHD")])
        rdf_without_dedupe = self.render_term(term, dedupe_enabled=False)
        rdf_with_dedupe = self.render_term(term, dedupe_enabled=True)

        self.assertEqual(
            rdf_without_dedupe.count("rdfs:subClassOf <http://example.org/test/CParent> ;"),
            2,
        )
        self.assertEqual(
            rdf_with_dedupe.count("rdfs:subClassOf <http://example.org/test/CParent> ;"),
            1,
        )

    def test_dedupes_duplicate_object_triples_in_load_on_cuis_mode(self):
        rel = make_rel("C0001", "CTarget", "RO", "relatedTo")
        term = self.make_term([rel, make_rel("C0001", "CTarget", "RO", "relatedTo")])
        rdf_without_dedupe = self.render_term(term, dedupe_enabled=False)
        rdf_with_dedupe = self.render_term(term, dedupe_enabled=True)

        self.assertEqual(
            rdf_without_dedupe.count(
                "<http://example.org/test/relatedTo> <http://example.org/test/CTarget> ;"
            ),
            2,
        )
        self.assertEqual(
            rdf_with_dedupe.count(
                "<http://example.org/test/relatedTo> <http://example.org/test/CTarget> ;"
            ),
            1,
        )

    def test_sorts_entries_within_generated_class(self):
        atoms = [
            make_atom("C0001", "Preferred label"),
            make_atom("C0001", "Alpha synonym"),
            make_atom("C0001", "Zulu label"),
        ]
        rels = [make_rel("C0001", "CPARENT", "CHD")]
        atts = [
            make_att("C0001", "IS_DRUG_CLASS", "Y"),
            make_att("C0001", "ATC_LEVEL", "5"),
        ]
        sty = [
            make_sty("C0001", "T121"),
            make_sty("C0001", "T109"),
        ]
        term = UmlsClass(
            "http://example.org/test",
            atoms=atoms,
            rels=rels,
            defs=[],
            atts=atts,
            rank=[],
            rank_by_tty={},
            sty=sty,
            sty_by_cui={"C0001": [0, 1]},
            load_on_cuis=True,
            is_root=False,
        )
        rdf = self.render_term(term, dedupe_enabled=True)

        self.assertLess(
            rdf.index('"""Alpha synonym"""@en'),
            rdf.index('"""Zulu label"""@en'),
        )
        self.assertLess(
            rdf.index("rdfs:subClassOf <http://example.org/test/CPARENT> ;"),
            rdf.index('<http://example.org/test/ATC_LEVEL> """5"""^^xsd:string ;'),
        )
        self.assertLess(
            rdf.index('<http://example.org/test/ATC_LEVEL> """5"""^^xsd:string ;'),
            rdf.index('<http://example.org/test/IS_DRUG_CLASS> """Y"""^^xsd:string ;'),
        )
        self.assertLess(
            rdf.index('<http://example.org/test/IS_DRUG_CLASS> """Y"""^^xsd:string ;'),
            rdf.index('umls:cui """C0001"""^^xsd:string ;'),
        )
        self.assertLess(
            rdf.index('umls:tui """T109"""^^xsd:string ;'),
            rdf.index('umls:tui """T121"""^^xsd:string ;'),
        )
        self.assertLess(
            rdf.index('umls:hasSTY <http://purl.bioontology.org/ontology/STY/T109> ;'),
            rdf.index('umls:hasSTY <http://purl.bioontology.org/ontology/STY/T121> ;'),
        )

    def test_identical_output_for_equivalent_class_inputs_in_different_orders(self):
        term_a = UmlsClass(
            "http://example.org/test",
            atoms=[
                make_atom("C0001", "Preferred label", ispref="Y", stt="PF", tty="PT"),
                make_atom("C0001", "Alpha synonym"),
                make_atom("C0001", "Zulu label"),
            ],
            rels=[
                make_rel("C0001", "CTargetB", "RO", "relatedToB"),
                make_rel("C0001", "CPARENT", "CHD"),
                make_rel("C0001", "CTargetA", "RO", "relatedToA"),
            ],
            defs=[],
            atts=[
                make_att("C0001", "IS_DRUG_CLASS", "Y"),
                make_att("C0001", "ATC_LEVEL", "5"),
            ],
            rank=[],
            rank_by_tty={},
            sty=[
                make_sty("C0001", "T121"),
                make_sty("C0001", "T109"),
            ],
            sty_by_cui={"C0001": [0, 1]},
            load_on_cuis=True,
            is_root=False,
        )
        term_b = UmlsClass(
            "http://example.org/test",
            atoms=[
                make_atom("C0001", "Zulu label"),
                make_atom("C0001", "Preferred label", ispref="Y", stt="PF", tty="PT"),
                make_atom("C0001", "Alpha synonym"),
            ],
            rels=[
                make_rel("C0001", "CTargetA", "RO", "relatedToA"),
                make_rel("C0001", "CTargetB", "RO", "relatedToB"),
                make_rel("C0001", "CPARENT", "CHD"),
            ],
            defs=[],
            atts=[
                make_att("C0001", "ATC_LEVEL", "5"),
                make_att("C0001", "IS_DRUG_CLASS", "Y"),
            ],
            rank=[],
            rank_by_tty={},
            sty=[
                make_sty("C0001", "T109"),
                make_sty("C0001", "T121"),
            ],
            sty_by_cui={"C0001": [0, 1]},
            load_on_cuis=True,
            is_root=False,
        )

        rdf_a = self.render_term(term_a, dedupe_enabled=True)
        rdf_b = self.render_term(term_b, dedupe_enabled=True)

        self.assertEqual(rdf_a, rdf_b)


class UmlsClassBehaviorTest(unittest.TestCase):
    def test_get_pref_label_prefers_single_pf_atom_in_load_on_cuis_mode(self):
        term = UmlsClass(
            "http://example.org/test",
            atoms=[
                make_atom("C0001", "Later synonym"),
                make_atom("C0001", "Preferred label", ispref="Y", stt="PF", tty="PT"),
                make_atom("C0001", "Other preferred", ispref="Y", stt="VC", tty="SY"),
            ],
            rels=[],
            defs=[],
            atts=[],
            rank=[],
            rank_by_tty={},
            sty=[make_sty("C0001", "T001")],
            sty_by_cui={"C0001": [0]},
            load_on_cuis=True,
            is_root=False,
        )

        self.assertEqual(term.getPrefLabel(), "Preferred label")

    def test_skips_known_bogus_parents_in_subclass_output(self):
        term = UmlsClass(
            "http://example.org/test",
            atoms=[make_atom("C0001", "Preferred label")],
            rels=[
                make_rel("C0001", "138875005", "CHD"),
                make_rel("C0001", "V-HL7V3.0", "CHD"),
                make_rel("C0001", "C1553931", "CHD"),
                make_rel("C0001", "VALID_PARENT", "CHD"),
            ],
            defs=[],
            atts=[],
            rank=[],
            rank_by_tty={},
            sty=[make_sty("C0001", "T001")],
            sty_by_cui={"C0001": [0]},
            load_on_cuis=True,
            is_root=False,
        )

        with mock.patch.object(conf, "DEDUPE_CLASS_TRIPLES", True, create=True):
            rdf = term.toRDF(tree=None)

        self.assertIn("rdfs:subClassOf <http://example.org/test/VALID_PARENT> ;", rdf)
        self.assertNotIn("138875005", rdf)
        self.assertNotIn("V-HL7V3.0", rdf)
        self.assertNotIn("C1553931", rdf)


class UmlsOntologyBehaviorTest(unittest.TestCase):
    def test_terms_rewrite_code_mode_relations_and_filter_self_maps(self):
        ont = UmlsOntology("TEST", "http://example.org/test", con=None, load_on_cuis=False)
        ont.loaded = True
        ont.lang = "eng"
        ont.atoms = [
            make_code_atom("CUI_SOURCE", "AUI_SOURCE", "CODE1", "Source preferred", tty="PT"),
            make_code_atom("CUI_TARGET", "AUI_TARGET", "CODE2", "Target preferred", tty="PT"),
            make_code_atom("CUI_SELF", "AUI_SELF", "CODE1", "Source synonym", tty="SY"),
        ]
        ont.atoms_by_code = collections.defaultdict(list, {"CODE1": [0, 2], "CODE2": [1]})
        ont.atoms_by_aui = collections.defaultdict(
            list,
            {"AUI_SOURCE": [0], "AUI_TARGET": [1], "AUI_SELF": [2]},
        )
        ont.rels = [
            make_code_rel("CUI_SOURCE", "AUI_SOURCE", "CUI_TARGET", "AUI_TARGET", "RO", "mappedTo"),
            make_code_rel("CUI_SOURCE", "AUI_SOURCE", "CUI_SELF", "AUI_SELF", "RO", "selfMap"),
        ]
        ont.rels_by_aui_src = collections.defaultdict(list, {"AUI_SOURCE": [0, 1]})
        ont.sty = [make_sty("CUI_SOURCE", "T001"), make_sty("CUI_TARGET", "T002")]
        ont.sty_by_cui = collections.defaultdict(list, {"CUI_SOURCE": [0], "CUI_TARGET": [1]})

        terms = {term.code(): term for term in ont.terms()}

        self.assertEqual(sorted(terms.keys()), ["CODE1", "CODE2"])
        self.assertEqual(len(terms["CODE1"].rels), 1)
        self.assertEqual(get_rel_code_source(terms["CODE1"].rels[0], False), "CODE1")
        self.assertEqual(get_rel_code_target(terms["CODE1"].rels[0], False), "CODE2")

    def test_write_into_writes_ontology_header_metadata(self):
        ont = UmlsOntology("TEST", "http://example.org/test", con=None, load_on_cuis=False)
        ont.loaded = True
        ont.lang = "eng"
        ont.tree = None
        ont.mrsab_record = [""] * 24
        ont.mrsab_record[3] = "TEST-RSAB"
        ont.mrsab_record[6] = "2025-test-version"
        ont.mrsab_record[9] = "2025AB"
        ont.mrsab_record[23] = "Test Ontology Title"

        writes = []
        fout = mock.Mock()
        fout.write.side_effect = writes.append

        with mock.patch("umls2rdf.codecs.open", return_value=fout):
            returned = ont.write_into("ignored.ttl")

        self.assertIs(returned, fout)
        written = "".join(writes)
        self.assertIn("<http://example.org/test>", written)
        self.assertIn('rdfs:label "Test Ontology Title" ;', written)
        self.assertIn('owl:versionInfo "2025-test-version" ;', written)
        self.assertIn('dcterms:source "UMLS 2025AB"', written)
        self.assertIn('skos:altLabel "TEST-RSAB" .', written)

    def make_code_ontology(self, atoms_by_code):
        ont = UmlsOntology("TEST", "http://example.org/test", con=None, load_on_cuis=False)
        ont.loaded = True
        ont.lang = "eng"
        ont.tree = None
        ont.mrsab_record = [""] * 24
        for code, atom in atoms_by_code.items():
            ont.atoms_by_code[code].append(len(ont.atoms))
            ont.atoms_by_aui[atom[7]].append(len(ont.atoms))
            ont.atoms.append(atom)
        ont.sty_by_cui = collections.defaultdict(list)
        return ont

    def test_write_into_reports_failed_terms_and_keeps_writing(self):
        good = make_code_atom("C0001", "A001", "GOOD", "Good label", tty="PT")
        # TTY "XX" has no MRRANK row, so the preferred label lookup fails.
        bad = make_code_atom("C0002", "A002", "BAD", "Bad label", tty="XX")
        ont = self.make_code_ontology({"GOOD": good, "BAD": bad})
        ont.rank = [["1", "TEST", "PT", "N"]]
        ont.rank_by_tty = collections.defaultdict(list, {"PT": [0]})

        writes = []
        fout = mock.Mock()
        fout.write.side_effect = writes.append
        with mock.patch("umls2rdf.codecs.open", return_value=fout), \
                mock.patch("umls2rdf.sys.stderr", new_callable=io.StringIO) as stderr:
            ont.write_into("ignored.ttl")

        self.assertEqual(ont.terms_written, 1)
        self.assertEqual(ont.term_errors, ["BAD"])
        self.assertIn("ERROR TEST: term BAD not written: IndexError", stderr.getvalue())
        self.assertIn("<http://example.org/test/GOOD>", "".join(writes))

    def test_write_into_reports_terms_that_fail_to_build(self):
        good = make_code_atom("C0001", "A001", "GOOD", "Good label", tty="PT")
        bad = make_code_atom("C0002", "A002", "BAD", "Bad label", tty="PT")
        ont = self.make_code_ontology({"BAD": bad, "GOOD": good})
        ont.rank = [["1", "TEST", "PT", "N"]]
        ont.rank_by_tty = collections.defaultdict(list, {"PT": [0]})
        # The relation's source AUI matches no atom, so building BAD raises
        # "more than one or none codes" before toRDF is reached.
        ont.rels = [make_code_rel("C0002", "A_GHOST", "C0001", "A001", "CHD")]
        ont.rels_by_aui_src = collections.defaultdict(list, {"A002": [0]})

        writes = []
        fout = mock.Mock()
        fout.write.side_effect = writes.append
        with mock.patch("umls2rdf.codecs.open", return_value=fout), \
                mock.patch("umls2rdf.sys.stderr", new_callable=io.StringIO) as stderr:
            ont.write_into("ignored.ttl")

        self.assertEqual(ont.terms_written, 1)
        self.assertEqual(ont.term_errors, ["BAD"])
        self.assertIn("ERROR TEST: term BAD not written: AttributeError", stderr.getvalue())
        self.assertIn("<http://example.org/test/GOOD>", "".join(writes))

    def test_write_into_loads_tables_before_opening_output(self):
        atom = make_code_atom("C0001", "A001", "CODE1", "Label", tty="PT")
        ont = self.make_code_ontology({"CODE1": atom})
        ont.loaded = False
        ont.lang = None

        def load_tables():
            ont.loaded = True
            ont.lang = "eng"

        with mock.patch.object(ont, "load_tables", side_effect=load_tables) as load, \
                mock.patch("umls2rdf.codecs.open", return_value=mock.Mock()):
            ont.write_into("ignored.ttl")

        load.assert_called_once_with()
        self.assertEqual(ont.terms_written, 1)

    def test_write_into_unmapped_language_does_not_open_output(self):
        ont = self.make_code_ontology({})
        ont.lang = "xyz"

        with mock.patch("umls2rdf.codecs.open") as open_file:
            with self.assertRaisesRegex(ValueError, "LAT 'xyz'"):
                ont.write_into("ignored.ttl")

        open_file.assert_not_called()

    def test_write_into_works_with_conf_missing_newer_settings(self):
        atom = make_code_atom("C0001", "A001", "CODE1", "Label", tty="PT")
        ont = self.make_code_ontology({"CODE1": atom})
        old_conf = types.SimpleNamespace(
            UMLS_BASE_URI="http://example.org/", UMLS_VERSION="2025AB")

        with mock.patch("umls2rdf.conf", old_conf), \
                mock.patch("umls2rdf.codecs.open", return_value=mock.Mock()):
            ont.write_into("ignored.ttl")

        self.assertEqual(ont.terms_written, 1)
        self.assertEqual(ont.term_errors, [])


class LanguageTagTest(unittest.TestCase):
    def test_unmapped_language_fails_with_clear_error(self):
        with self.assertRaisesRegex(ValueError, "LAT 'xyz' \\(ontology TEST\\)"):
            get_language_tag("xyz", "TEST")

    def test_mapped_language(self):
        self.assertEqual(get_language_tag("eng", "TEST"), "en")


class UmlsTableScanTest(unittest.TestCase):
    def scan(self, rows, **kwargs):
        cursor = mock.MagicMock()
        cursor.__iter__.return_value = iter(rows)
        conn = mock.Mock()
        conn.cursor.return_value = cursor
        table = UmlsTable("MRCONSO", conn, load_select=kwargs.pop("load_select", None))
        with mock.patch("umls2rdf.sys.stdout", new_callable=io.StringIO):
            result = list(table.scan(**kwargs))
        return result, conn, cursor

    def test_streams_one_query_without_paging(self):
        rows = [("r%d" % i,) for i in range(5)]
        result, conn, cursor = self.scan(rows, filt="SAB = 'TEST'")

        self.assertEqual(result, rows)
        conn.cursor.assert_called_once_with(pymysql.cursors.SSCursor)
        cursor.execute.assert_called_once_with("SELECT * FROM MRCONSO WHERE SAB = 'TEST'")
        cursor.close.assert_called_once()

    def test_limit_is_pushed_into_query(self):
        rows = [("r%d" % i,) for i in range(5)]
        result, _, cursor = self.scan(rows, limit=2)

        self.assertEqual(result, rows[:2])
        cursor.execute.assert_called_once_with("SELECT * FROM MRCONSO LIMIT 2")
        cursor.close.assert_called_once()

    def test_load_select_is_used_verbatim(self):
        result, _, cursor = self.scan([("a",)], load_select="SELECT 1")

        self.assertEqual(result, [("a",)])
        cursor.execute.assert_called_once_with("SELECT 1")


class ExportOntologiesTest(unittest.TestCase):
    UMLS_CONF = [
        ["MDRISL", "MDRISL.ttl", "load_on_codes"],
        ["#OLD", "OLD.ttl", "load_on_codes"],
        ["ICD10;ICD10ALT", "ICD10.ttl", "load_on_codes"],
        ["AIR", "AIR.ttl", "load_on_codes"],
    ]

    def export(self, side_effect):
        with mock.patch("umls2rdf.export_ontology", side_effect=side_effect) as export_one, \
                mock.patch("umls2rdf.sys.stderr", new_callable=io.StringIO) as stderr:
            outcome = export_ontologies(None, self.UMLS_CONF, "out", {}, "")
        return outcome, export_one, stderr.getvalue()

    def test_failed_ontology_is_reported_and_the_rest_still_export(self):
        icd10 = {"sab": "ICD10", "file": "ICD10.ttl", "terms_written": 5, "term_errors": 0}
        (results, skipped, failures), export_one, stderr = self.export(
            [ValueError("No language tag mapping for MRSAB LAT 'isl'"), icd10, None])

        self.assertEqual(export_one.call_count, 3)
        self.assertEqual(results, [icd10])
        self.assertEqual(skipped, ["AIR"])
        self.assertEqual(failures, ["MDRISL"])
        self.assertIn("ERROR MDRISL: ontology not exported: ValueError", stderr)

    def test_lost_connection_stops_the_run(self):
        error = pymysql.err.OperationalError(2013, "Lost connection")
        with self.assertRaises(pymysql.err.OperationalError):
            self.export([error])


class ManifestTest(unittest.TestCase):
    def test_error_limit(self):
        results = [
            {"sab": "A", "term_errors": 0},
            {"sab": "B", "term_errors": 3},
        ]
        self.assertEqual([r["sab"] for r in ontologies_over_error_limit(results, 0)], ["B"])
        self.assertEqual(ontologies_over_error_limit(results, 3), [])

    def test_write_and_remove_manifest(self):
        with tempfile.TemporaryDirectory() as output_dir:
            with mock.patch.object(conf, "UMLS_VERSION", "2025AB"):
                write_manifest(output_dir, [{"sab": "A", "file": "A.ttl"}], ["B"])
            with open(os.path.join(output_dir, MANIFEST_FILENAME)) as fmanifest:
                manifest = json.load(fmanifest)
            self.assertEqual(manifest["umls_version"], "2025AB")
            self.assertEqual(manifest["skipped"], ["B"])
            self.assertEqual(os.listdir(output_dir), [MANIFEST_FILENAME])

            remove_manifest(output_dir)
            self.assertEqual(os.listdir(output_dir), [])


if __name__ == "__main__":
    unittest.main()
