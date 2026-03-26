import collections
import unittest
from unittest import mock

import conf
from umls2rdf import UmlsClass, UmlsOntology, get_rel_code_source, get_rel_code_target


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


if __name__ == "__main__":
    unittest.main()
