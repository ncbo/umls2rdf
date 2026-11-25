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
    UMLS_LANGCODE_MAP,
    UmlsClass,
    UmlsOntology,
    UmlsTable,
    export_ontologies,
    get_language_tag,
    get_umls2rdf_tool,
    get_umls_release_docs,
    get_rel_code_source,
    get_rel_code_target,
    get_umls_conf_path,
    ontologies_over_error_limit,
    parse_umls_conf_line,
    read_semantic_network,
    read_umls_conf,
    remove_manifest,
    write_manifest,
    write_semantic_types_file,
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
        ont.lang = "spa"
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
        ont.mrsab_record[13] = 3
        ont.mrsab_record[23] = "Test Ontology Title"
        ont.umls_docs = UMLS_DOCS
        ont.tool = "umls2rdf v9.9.9"

        writes = []
        fout = mock.Mock()
        fout.write.side_effect = writes.append

        with mock.patch("umls2rdf.codecs.open", return_value=fout), \
                mock.patch("umls2rdf.sys.stderr", new_callable=io.StringIO) as stderr:
            returned = ont.write_into("ignored.ttl")

        self.assertIs(returned, fout)
        written = "".join(writes)
        self.assertIn("<http://example.org/test>", written)
        self.assertIn(
            'rdfs:comment "RDF Version of the UMLS ontology TEST; converted with '
            'the UMLS2RDF tool (https://github.com/ncbo/umls2rdf), developed by '
            'the NCBO project."@en ;', written)
        self.assertIn('rdfs:label "Test Ontology Title"@en ;', written)
        self.assertIn('owl:versionInfo "2025-test-version" ;', written)
        self.assertIn('dcterms:source "UMLS 2025AB"', written)
        self.assertIn('skos:altLabel "TEST-RSAB"@en ;', written)
        self.assertIn('dcterms:issued "2025-11-03"^^xsd:date ;', written)
        self.assertIn('dcterms:language <http://lexvo.org/id/iso639-1/en> ;', written)
        self.assertIn('dcterms:license <https://example.org/license#category3> ;', written)
        self.assertIn(
            'cc:useGuidelines "Use of this content requires a UMLS Metathesaurus License '
            'from the U.S. National Library of Medicine (https://uts.nlm.nih.gov/). '
            'UMLS source restriction level 3: General terms + additional restrictions '
            'in category 12.3. See https://example.org/license#category3 for the terms '
            'of this level."@en ;', written)
        self.assertIn('pav:createdWith "umls2rdf v9.9.9" .\n\n', written)
        self.assertEqual(stderr.getvalue(), "")


UMLS_DOCS = get_umls_release_docs([
    ("RELEASE", "umls.release.date", "release_info", "20251103"),
    ("RELEASE", "umls.release.name", "release_info", "2025AB"),
    ("SRL", "0", "expanded_form", "No additional restrictions; general terms of the license agreement apply."),
    ("SRL", "0", "uri", "https://example.org/license"),
    ("SRL", "3", "expanded_form", "General terms + additional restrictions in category 12.3"),
    ("SRL", "3", "uri", "https://example.org/license#category3"),
    ("REL", "PAR", "expanded_form", "has parent relationship in a Metathesaurus source vocabulary"),
])


class OntologyHeaderMetadataTest(unittest.TestCase):
    def make_ontology(self, srl=3, umls_docs=UMLS_DOCS):
        ont = UmlsOntology("TEST", "http://example.org/test", con=None, load_on_cuis=False)
        ont.mrsab_record = [""] * 24
        ont.mrsab_record[13] = srl
        ont.umls_docs = umls_docs
        return ont

    def optional_lines(self, ont, lang_tag="en"):
        with mock.patch("umls2rdf.sys.stderr", new_callable=io.StringIO) as stderr:
            lines = ont.ontology_optional_lines(lang_tag)
        return lines, stderr.getvalue()

    def test_reads_release_date_and_restriction_levels_from_mrdoc(self):
        self.assertEqual(UMLS_DOCS["release_date"], "20251103")
        self.assertEqual(sorted(UMLS_DOCS["srl"]), ["0", "3"])
        self.assertEqual(UMLS_DOCS["srl"]["3"]["uri"], "https://example.org/license#category3")

    def test_level_zero_is_a_license_level_not_a_missing_value(self):
        lines, warnings = self.optional_lines(self.make_ontology(srl=0))
        self.assertIn("dcterms:license <https://example.org/license>", lines)
        self.assertIn("UMLS source restriction level 0: No additional restrictions; "
                      "general terms of the license agreement apply. See", lines)
        self.assertEqual(warnings, "")

    def test_missing_mrdoc_rows_warn_and_leave_lines_out(self):
        lines, warnings = self.optional_lines(self.make_ontology(umls_docs=None))
        self.assertNotIn("dcterms:issued", lines)
        self.assertNotIn("dcterms:license", lines)
        self.assertNotIn("cc:useGuidelines", lines)
        self.assertIn("dcterms:language <http://lexvo.org/id/iso639-1/en>", lines)
        self.assertIn("WARNING TEST: no UMLS release date in MRDOC", warnings)
        self.assertIn("WARNING TEST: no MRDOC uri and expanded_form for SRL 3", warnings)

    def test_missing_srl_warns_and_leaves_license_out(self):
        lines, warnings = self.optional_lines(self.make_ontology(srl=None))
        self.assertNotIn("dcterms:license", lines)
        self.assertIn("dcterms:issued", lines)
        self.assertIn("WARNING TEST: no SRL in MRSAB", warnings)

    def test_unknown_srl_level_warns_and_leaves_license_out(self):
        lines, warnings = self.optional_lines(self.make_ontology(srl=7))
        self.assertNotIn("dcterms:license", lines)
        self.assertIn("no MRDOC uri and expanded_form for SRL 7", warnings)

    def test_malformed_release_date_warns_and_leaves_issued_out(self):
        docs = dict(UMLS_DOCS, release_date="2025-11")
        lines, warnings = self.optional_lines(self.make_ontology(umls_docs=docs))
        self.assertNotIn("dcterms:issued", lines)
        self.assertIn("MRDOC UMLS release date '2025-11' is not YYYYMMDD", warnings)

    def test_no_tool_leaves_created_with_out(self):
        lines, warnings = self.optional_lines(self.make_ontology())
        self.assertNotIn("pav:createdWith", lines)
        self.assertEqual(warnings, "")

    def test_language_uses_primary_subtag(self):
        lines, _ = self.optional_lines(self.make_ontology(), lang_tag="pt-BR")
        self.assertIn("dcterms:language <http://lexvo.org/id/iso639-1/pt>", lines)


class Umls2rdfToolTest(unittest.TestCase):
    def tool(self, describe):
        """get_umls2rdf_tool() with `git describe --tags --always`
        returning `describe` (None: not a git checkout)."""
        with mock.patch("umls2rdf.run_git", return_value=describe) as run_git, \
                mock.patch("umls2rdf.sys.stderr", new_callable=io.StringIO) as stderr:
            tool = get_umls2rdf_tool()
        run_git.assert_called_once_with("describe", "--tags", "--always")
        return tool, stderr.getvalue()

    def test_release_tag_gives_the_release_name(self):
        tool, warnings = self.tool("v1.3.0")
        self.assertEqual(tool, "umls2rdf v1.3.0")
        self.assertEqual(warnings, "")

    def test_untagged_commit_gives_the_describe_name(self):
        tool, warnings = self.tool("v1.2.0-8-g3c22d00")
        self.assertEqual(tool, "umls2rdf v1.2.0-8-g3c22d00")
        self.assertEqual(warnings, "")

    def test_no_git_checkout_warns_and_returns_none(self):
        tool, warnings = self.tool(None)
        self.assertIsNone(tool)
        self.assertIn("WARNING umls2rdf: cannot read the git version", warnings)


SEM_TYPES = """<http://purl.bioontology.org/ontology/STY/T051> a owl:Class ;
\tskos:notation "T051"^^xsd:string ;
\tskos:prefLabel "Event"@en .
<http://purl.bioontology.org/ontology/STY/T051> rdfs:subClassOf owl:Thing .
"""


class SemanticTypesFileTest(unittest.TestCase):
    def write(self, umls_docs=UMLS_DOCS, tool="umls2rdf v9.9.9", umls_version="2025AB"):
        with tempfile.TemporaryDirectory() as output_dir, \
                mock.patch.object(conf, "UMLS_VERSION", umls_version), \
                mock.patch.object(conf, "UMLS_BASE_URI", "http://purl.bioontology.org/ontology/"), \
                mock.patch("umls2rdf.sys.stderr", new_callable=io.StringIO) as stderr:
            path = write_semantic_types_file(output_dir, SEM_TYPES, umls_docs, tool)
            self.assertEqual(os.path.basename(path), "umls_semantictypes.ttl")
            with open(path, encoding="utf-8") as f:
                return f.read(), stderr.getvalue()

    def test_header_gives_umls_release_as_version_and_release_date(self):
        written, warnings = self.write()
        header = written.split("<http://purl.bioontology.org/ontology/STY/T051>")[0]
        self.assertIn("<http://purl.bioontology.org/ontology/STY/>\n    a owl:Ontology ;", header)
        self.assertIn(
            'rdfs:comment "RDF Version of the UMLS ontology STY; converted with '
            'the UMLS2RDF tool (https://github.com/ncbo/umls2rdf), developed by '
            'the NCBO project."@en ;', header)
        self.assertIn('rdfs:label "UMLS Semantic Network"@en ;', header)
        self.assertIn('owl:versionInfo "2025AB" ;', header)
        self.assertIn('dcterms:source "UMLS 2025AB" ;', header)
        self.assertIn('skos:altLabel "STY"@en ;', header)
        self.assertIn('dcterms:issued "2025-11-03"^^xsd:date ;', header)
        self.assertIn('dcterms:language <http://lexvo.org/id/iso639-1/en> ;', header)
        self.assertIn('pav:createdWith "umls2rdf v9.9.9" .\n\n', header)
        self.assertEqual(warnings, "")

    def test_version_is_upper_case(self):
        written, _ = self.write(umls_version="2025ab")
        self.assertIn('owl:versionInfo "2025AB" ;', written)
        self.assertIn('dcterms:source "UMLS 2025AB" ;', written)

    def test_header_has_no_skos_import_or_license(self):
        written, _ = self.write()
        self.assertNotIn("owl:imports", written)
        self.assertNotIn("dcterms:license", written)
        self.assertNotIn("cc:useGuidelines", written)

    def test_prefixes_then_header_then_semantic_types(self):
        written, _ = self.write()
        prefixes = written.index("@prefix owl:")
        header = written.index("a owl:Ontology")
        terms = written.index('skos:prefLabel "Event"@en')
        self.assertLess(prefixes, header)
        self.assertLess(header, terms)
        self.assertTrue(written.endswith(SEM_TYPES))

    def test_missing_release_date_warns_and_leaves_issued_out(self):
        written, warnings = self.write(umls_docs=None, tool=None)
        self.assertNotIn("dcterms:issued", written)
        self.assertNotIn("pav:createdWith", written)
        self.assertIn('dcterms:language <http://lexvo.org/id/iso639-1/en> .\n\n', written)
        self.assertIn("WARNING STY: no UMLS release date in MRDOC", warnings)

    def test_source_headers_still_import_skos(self):
        ont = UmlsOntology("TEST", "http://example.org/test", con=None, load_on_cuis=False)
        ont.loaded = True
        ont.lang = "eng"
        ont.tree = None
        writes = []
        fout = mock.Mock()
        fout.write.side_effect = writes.append
        with mock.patch("umls2rdf.codecs.open", return_value=fout), \
                mock.patch("umls2rdf.sys.stderr", new_callable=io.StringIO):
            ont.write_into("ignored.ttl")
        self.assertIn('rdfs:label "TEST"@en ;\n'
                      '    owl:imports <http://www.w3.org/2004/02/skos/core> ;\n'
                      '    owl:versionInfo', "".join(writes))

    def test_written_file_is_valid_turtle(self):
        try:
            import rdflib
        except ImportError:
            self.skipTest("rdflib is not installed")
        written, _ = self.write()
        graph = rdflib.Graph().parse(data=written, format="turtle")
        ontology = rdflib.URIRef("http://purl.bioontology.org/ontology/STY/")
        self.assertEqual(
            str(graph.value(ontology, rdflib.OWL.versionInfo)), "2025AB")
        self.assertEqual(
            str(graph.value(ontology, rdflib.URIRef("http://purl.org/dc/terms/issued"))),
            "2025-11-03")


SRDEF = (
    "STY|T051|Event|B|A broadly defined activity.||NULL||evnt||\n"
    "STY|T052|Activity|B1|An operation or series of operations.|Use \"rarely\".|"
    "Only when no other type applies.|Y|acty||\n"
    "RL|T166|associated_with|R|Has a significant relationship to.||||AW|associated_with|\n"
    "RL|T139|functionally_related_to|R3|Related by function.||||FR|functionally_related_to|\n"
    "RL|T151|affects|R3.1|Produces a direct effect on.||||AF|affected_by|\n"
    "RL|T186|isa|H|The basic hierarchical link.||||IS|inverse_isa|\n"
)
SRSTR = (
    "Activity|isa|Event|D|\n"
    "Activity|affects|Event|D|\n"
    "Event|affects|Activity|B|\n"
    "affects|isa|functionally_related_to|D|\n"
    "Event|associated_with||D|\n"
)


class SemanticNetworkTest(unittest.TestCase):
    def read(self):
        with tempfile.TemporaryDirectory() as sn_dir, \
                mock.patch.object(conf, "UMLS_BASE_URI", "http://purl.bioontology.org/ontology/"):
            for name, data in (("SRDEF", SRDEF), ("SRSTR", SRSTR)):
                with open(os.path.join(sn_dir, name), "w", encoding="utf-8") as f:
                    f.write(data)
            return read_semantic_network(sn_dir)

    def test_types_get_definitions_notes_and_links(self):
        sty = "http://purl.bioontology.org/ontology/STY/"
        written = self.read()
        self.assertIn(
            '<%sT052> <%sSTN> "B1"^^xsd:string ;\n'
            '\t<%sABR> "acty"^^xsd:string ;\n'
            '\t<%sNH> "Y"^^xsd:string ;\n'
            '\tskos:definition "An operation or series of operations."@en ;\n'
            '\tskos:scopeNote "Only when no other type applies."@en ;\n'
            '\tskos:example "Use \\"rarely\\"."@en ;\n'
            '\t<%sT151> <%sT051> .\n' % ((sty,) * 6), written)
        # NULL notes are left out, and the blocked link is not written.
        self.assertIn('<%sT051> <%sSTN> "B"^^xsd:string ;\n'
                      '\t<%sABR> "evnt"^^xsd:string ;\n'
                      '\tskos:definition "A broadly defined activity."@en .\n'
                      % (sty, sty, sty), written)

    def test_relations_are_object_properties(self):
        sty = "http://purl.bioontology.org/ontology/STY/"
        written = self.read()
        self.assertIn('<%sT151> a owl:ObjectProperty ;\n'
                      '\trdfs:label "affects"@en ;\n'
                      '\tskos:prefLabel "affects"@en ;\n'
                      '\tskos:notation "T151"^^xsd:string ;\n'
                      '\t<%sRTN> "R3.1"^^xsd:string ;\n'
                      '\tskos:definition "Produces a direct effect on."@en ;\n'
                      '\trdfs:subPropertyOf <%sT139> .\n' % (sty, sty, sty), written)
        # A relation that is its own inverse gets no owl:inverseOf.
        self.assertNotIn("owl:inverseOf", written)

    def test_is_valid_turtle_with_the_semantic_types(self):
        try:
            import rdflib
        except ImportError:
            self.skipTest("rdflib is not installed")
        with mock.patch.object(conf, "UMLS_VERSION", "2025AB"), \
                mock.patch.object(conf, "UMLS_BASE_URI", "http://purl.bioontology.org/ontology/"), \
                mock.patch("umls2rdf.sys.stderr", new_callable=io.StringIO), \
                tempfile.TemporaryDirectory() as output_dir:
            path = write_semantic_types_file(output_dir, SEM_TYPES + self.read(), UMLS_DOCS)
            graph = rdflib.Graph().parse(path, format="turtle")
        sty = rdflib.Namespace("http://purl.bioontology.org/ontology/STY/")
        self.assertEqual(set(graph.objects(sty.T052, sty.T151)), {sty.T051})
        self.assertEqual(graph.value(sty.T051, rdflib.RDF.type), rdflib.OWL.Class)

    def test_empty_srdef_fails(self):
        with tempfile.TemporaryDirectory() as sn_dir:
            for name in ("SRDEF", "SRSTR"):
                open(os.path.join(sn_dir, name), "w").close()
            with self.assertRaises(ValueError):
                read_semantic_network(sn_dir)


class UmlsLanguageCodeMapTest(unittest.TestCase):
    def test_uses_valid_bcp47_language_codes_for_known_problem_cases(self):
        self.assertEqual(UMLS_LANGCODE_MAP["cze"], "cs")
        self.assertEqual(UMLS_LANGCODE_MAP["jpn"], "ja")
        self.assertEqual(UMLS_LANGCODE_MAP["kor"], "ko")
        self.assertEqual(UMLS_LANGCODE_MAP["swe"], "sv")

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
        ["MDRISL", "MDRISL.ttl", "load_on_codes", False],
        ["#OLD", "OLD.ttl", "load_on_codes", False],
        ["ICD10;ICD10ALT", "ICD10.ttl", "load_on_codes", True],
        ["AIR", "AIR.ttl", "load_on_codes", False],
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
        self.assertEqual([c.args[4] for c in export_one.call_args_list], [False, True, False])

    def test_lost_connection_stops_the_run(self):
        error = pymysql.err.OperationalError(2013, "Lost connection")
        with self.assertRaises(pymysql.err.OperationalError):
            self.export([error])


class ObsoleteCodesTest(unittest.TestCase):
    NS = "http://example.org/test/"

    def make_ontology(self):
        ont = UmlsOntology("TEST", self.NS, con=None, include_obsolete=True)
        ont.loaded = True
        ont.lang = "eng"
        ont.tree = None
        current = [
            make_code_atom("C_CUR", "A_CUR", "CUR", "Current label", tty="PT"),
            make_code_atom("C_PAR", "A_PAR", "PAR", "Parent label", tty="PT"),
        ]
        for atom in current:
            ont.atoms_by_code[atom[13]].append(len(ont.atoms))
            ont.atoms_by_aui[atom[7]].append(len(ont.atoms))
            ont.atoms.append(atom)
        ont.add_obsolete_codes([
            make_code_atom("C_RET", "A_RET", "RET", "Obsolete label", tty="OP"),
            make_code_atom("C_RET", "A_RET2", "RET", "Obsolete synonym", tty="OF"),
            # Obsolete name of a current code: must not be added.
            make_code_atom("C_CUR", "A_CUR_OLD", "CUR", "Old current label", tty="OP"),
        ])
        ont.rels = [
            make_code_rel("C_RET", "A_RET", "C_CUR", "A_CUR", "SY", "same_as"),
            make_code_rel("C_RET", "A_RET", "C_PAR", "A_PAR", "CHD"),
            make_code_rel("C_CUR", "A_CUR", "C_RET", "A_RET", "SY", "same_as"),
            make_code_rel("C_CUR", "A_CUR", "C_PAR", "A_PAR", "CHD"),
        ]
        for index, rel in enumerate(ont.rels):
            ont.rels_by_aui_src[rel[5]].append(index)
        ont.sty_by_cui = collections.defaultdict(list)
        return ont

    def render(self, term, tree=None):
        with mock.patch.object(conf, "DEDUPE_CLASS_TRIPLES", True, create=True):
            return term.toRDF(tree=tree)

    def test_only_codes_without_current_names_are_obsolete(self):
        ont = self.make_ontology()
        self.assertEqual(ont.obsolete_codes, {"RET"})
        self.assertEqual(len(ont.atoms_by_code["RET"]), 2)
        self.assertEqual(len(ont.atoms_by_code["CUR"]), 1)

    def test_obsolete_class_is_deprecated_outside_the_tree_with_history_links(self):
        ont = self.make_ontology()
        rdf = self.render(ont.build_term("RET"))

        self.assertIn('\towl:deprecated "true"^^xsd:boolean ;', rdf)
        self.assertIn('skos:prefLabel """Obsolete label"""@en', rdf)
        self.assertIn("\t<%ssame_as> <%sCUR> ;" % (self.NS, self.NS), rdf)
        self.assertNotIn("rdfs:subClassOf", rdf)

    def test_current_class_is_unchanged_by_obsolete_codes(self):
        with_obsolete = self.make_ontology()
        without = self.make_ontology()
        without.obsolete_codes = set()
        del without.atoms_by_code["RET"]
        for aui in ("A_RET", "A_RET2"):
            del without.atoms_by_aui[aui]

        rdf = self.render(with_obsolete.build_term("CUR"))
        self.assertEqual(rdf, self.render(without.build_term("CUR")))
        self.assertNotIn("RET", rdf)
        self.assertNotIn("owl:deprecated", rdf)
        self.assertIn("rdfs:subClassOf <%sPAR>" % self.NS, rdf)

    def test_obsolete_mesh_descriptor_stays_out_of_the_mesh_tree(self):
        # MeSH parents come from the tree and its roots from MN tree numbers,
        # not from CHD rels, so build_term's CHD/PAR filter does not cover them.
        tree = {"D000002": {"D000001"}}

        def mesh_class(is_obsolete):
            return UmlsClass(
                self.NS, atoms=[make_code_atom("C1", "A1", "D000002", "Heading", tty="PT")],
                rels=[], defs=[], atts=[make_code_att("D000002", "MN", "C04")],
                rank=[], rank_by_tty=collections.defaultdict(list), sty=[],
                sty_by_cui=collections.defaultdict(list), is_obsolete=is_obsolete)

        current = self.render(mesh_class(False), tree)
        self.assertIn("rdfs:subClassOf <%sD000001>" % self.NS, current)
        self.assertIn("rdfs:subClassOf owl:Thing", current)

        obsolete = self.render(mesh_class(True), tree)
        self.assertIn("owl:deprecated", obsolete)
        self.assertNotIn("rdfs:subClassOf", obsolete)

    def test_load_on_cuis_ignores_the_setting(self):
        ont = UmlsOntology("TEST", self.NS, con=None, load_on_cuis=True, include_obsolete=True)
        self.assertFalse(ont.include_obsolete)


class UmlsConfTest(unittest.TestCase):
    def test_reads_entries_skipping_comments_and_blank_lines(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "umls.conf")
            with open(path, "w") as fconf:
                fconf.write("AIR,AI-RHEUM.ttl,load_on_codes\n"
                            "#CPT,CPT.ttl,load_on_codes #disabled\n"
                            "\n   \n"
                            "MSH;MESH, MESH.ttl ,load_on_codes \n")
            self.assertEqual(read_umls_conf(path), [
                ["AIR", "AI-RHEUM.ttl", "load_on_codes", False],
                ["MSH;MESH", "MESH.ttl", "load_on_codes", False],
            ])

    def test_include_obsolete_option(self):
        self.assertEqual(
            parse_umls_conf_line("SNOMEDCT_US;SNOMEDCT,SNOMEDCT.ttl,load_on_codes, include_obsolete", "x"),
            ["SNOMEDCT_US;SNOMEDCT", "SNOMEDCT.ttl", "load_on_codes", True])
        self.assertEqual(parse_umls_conf_line("AIR,AIR.ttl,load_on_codes,", "x"),
                         ["AIR", "AIR.ttl", "load_on_codes", False])

    def test_malformed_lines_fail_naming_the_line(self):
        for line, message in (
                ("AIR,AIR.ttl", "expected SAB,file,strategy"),
                ("AIR,AIR.ttl,load_on_codes,include_obsolete,extra", "expected SAB,file,strategy"),
                ("AIR,AIR.ttl,load_on_codes,include_retired", "unknown option 'include_retired'"),
                ("HL7,HL7.ttl,load_on_cuis,include_obsolete", "include_obsolete needs load_on_codes")):
            with self.assertRaisesRegex(ValueError, "umls.conf:2: " + message):
                parse_umls_conf_line(line, "umls.conf:2")

    def test_reader_reports_file_and_line_number(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "umls.conf")
            with open(path, "w") as fconf:
                fconf.write("# comment\nAIR,AIR.ttl,load_on_codes\nHL7,HL7.ttl\n")
            with self.assertRaisesRegex(ValueError, "umls.conf:3: "):
                read_umls_conf(path)

    def test_missing_file_points_at_sample_and_setting(self):
        with self.assertRaisesRegex(FileNotFoundError, "umls_sample.conf.*UMLS_CONF"):
            read_umls_conf("/nonexistent/umls.conf")

    def test_path_defaults_to_umls_conf_and_follows_setting(self):
        with mock.patch("umls2rdf.conf", types.SimpleNamespace()):
            self.assertEqual(get_umls_conf_path(), "umls.conf")
        with mock.patch("umls2rdf.conf", types.SimpleNamespace(UMLS_CONF="~/cfg/umls.conf")):
            self.assertEqual(get_umls_conf_path(), os.path.expanduser("~/cfg/umls.conf"))

    def test_sample_conf_parses(self):
        sample = os.path.join(os.path.dirname(__file__), "..", "umls_sample.conf")
        entries = read_umls_conf(sample)
        self.assertIn(["MED-RT;MEDRT", "MEDRT.ttl", "load_on_codes", False], entries)


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
