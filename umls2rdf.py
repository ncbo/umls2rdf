#! /usr/bin/env python

DEBUG = False

import codecs
import datetime
import json
import sys
import os
import subprocess
import traceback
import urllib.request, urllib.parse, urllib.error
from string import Template
import collections
import pymysql
import pymysql.cursors
import pdb
from functools import reduce
from itertools import groupby

from download_semantic_network import DEFAULT_SEMANTIC_NETWORK_DIR, SEMANTIC_NETWORK_FILES

try:
    import conf
except:
    raise

PREFIXES = """
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix umls: <http://bioportal.bioontology.org/ontologies/umls/> .
@prefix cc: <http://creativecommons.org/ns#> .
@prefix pav: <http://purl.org/pav/> .
"""

ONTOLOGY_HEADER = Template("""
<$uri>
    a owl:Ontology ;
    rdfs:comment $comment ;
    rdfs:label $label ;$imports
    owl:versionInfo $versioninfo ;
    dcterms:source $source$optional_lines .

""")

# The source files import SKOS, as they have since 2012. BioPortal loads
# UMLS-format submissions as they are and does not follow the import; an
# OWL-format submission would pull SKOS's own classes in as roots.
SKOS_IMPORT = "\n    owl:imports <http://www.w3.org/2004/02/skos/core> ;"

# The semantic types file (umls_semantictypes.ttl). The Semantic Network is
# not an MRSAB source, so its header comes from the UMLS release itself.
SEMANTIC_TYPES_CODE = "STY"
SEMANTIC_TYPES_LABEL = "UMLS Semantic Network"
SEMANTIC_TYPES_FILE = "umls_semantictypes.ttl"

# Credits umls2rdf in every header; BioPortal copies rdfs:comment into the
# submission's notes. The wording is what earlier releases wrote; it has no
# umls2rdf version (that goes in pav:createdWith), so it stays the same from
# one release to the next.
ONTOLOGY_COMMENT = ("RDF Version of the UMLS ontology %s; "
                    "converted with the UMLS2RDF tool "
                    "(https://github.com/ncbo/umls2rdf), "
                    "developed by the NCBO project.")

STY_URL = "http://bioportal.bioontology.org/ontologies/umls/sty/"
HAS_STY = "umls:hasSTY"
HAS_AUI = "umls:aui"
HAS_CUI = "umls:cui"
HAS_TUI = "umls:tui"

MRCONSO_CODE = 13
MRCONSO_AUI = 7
MRCONSO_STR = 14
MRCONSO_STT = 4
MRCONSO_SCUI = 9
MRCONSO_ISPREF = 6
MRCONSO_TTY = 12
MRCONSO_TS = 2
MRCONSO_CUI = 0

# http://www.nlm.nih.gov/research/umls/sourcereleasedocs/current/SNOMEDCT/relationships.html
MRREL_AUI1 = 1
MRREL_AUI2 = 5
MRREL_CUI1 = 0
MRREL_CUI2 = 4
MRREL_REL = 3
MRREL_RELA = 7

MRDEF_AUI = 1
MRDEF_DEF = 5
MRDEF_CUI = 0

MRSAT_CUI = 0
MRSAT_CODE = 5
MRSAT_ATV = 10
MRSAT_ATN = 8

MRDOC_DOCKEY = 0
MRDOC_VALUE = 1
MRDOC_TYPE = 2
MRDOC_DESC = 3

MRRANK_TTY = 2
MRRANK_RANK = 0

MRSTY_CUI = 0
MRSTY_TUI = 1

MRSAB_LAT = 19
MRSAB_RSAB = 3
MRSAB_SVER = 6
MRSAB_SRL = 13
MRSAB_IMETA = 9
MRSAB_CURVER = 21
MRSAB_SSN = 23

UMLS_LANGCODE_MAP = {"eng" : "en", "fre" : "fr", "cze" : "cs", "fin" : "fi", "ger" : "de", "ita" : "it", "jpn" : "ja", "pol" : "pl", "por" : "pt", "rus" : "ru", "spa" : "es", "swe" : "sv", "scr" : "hr", "dut" : "nl", "lav" : "lv", "hun" : "hu", "kor" : "ko", "dan" : "da", "nor" : "no", "heb" : "he", "baq" : "eu"}

# Written to OUTPUT_FOLDER only after a run finishes within the allowed number
# of term errors; run_umls_pipeline.py uses it to decide whether the export
# step is complete.
MANIFEST_FILENAME = "umls2rdf_manifest.json"


# dcterms:language values. BioPortal reads them into naturalLanguage.
LEXVO_ISO639_1 = "http://lexvo.org/id/iso639-1/"

# cc:useGuidelines text. The level's terms and URL come from MRDOC, so the
# wording of each restriction is NLM's own.
UMLS_LICENSE_NOTICE = (
    "Use of this content requires a UMLS Metathesaurus License from the "
    "U.S. National Library of Medicine (https://uts.nlm.nih.gov/). "
    "UMLS source restriction level %(level)s: %(terms)s "
    "See %(uri)s for the terms of this level.")

# Settings added after conf.py files were already deployed. conf.py is not
# tracked, so read these with defaults that keep the old behavior.
CONF_DEFAULTS = {
    "DEDUPE_CLASS_TRIPLES": False,
    "INCLUDE_SEMANTIC_TYPES": True,
    "ADD_STY_DEFINITIONS_AND_RELATIONS": False,
    "SEMANTIC_NETWORK_DIR": DEFAULT_SEMANTIC_NETWORK_DIR,
    "PROCESS_ONLY_CURRENT_UMLS_VERSION": False,
    "MAX_TERM_ERRORS": 0,
    # List of sources to export. Relative paths are resolved against the
    # working directory, as umls.conf always was.
    "UMLS_CONF": "umls.conf",
}

UMLS_CONF_SAMPLE = "umls_sample.conf"

def conf_setting(name):
    return getattr(conf, name, CONF_DEFAULTS[name])

def get_umls_conf_path():
    return os.path.expanduser(conf_setting("UMLS_CONF"))

def parse_umls_conf_line(line, where):
    """Return [SAB, output file, strategy, include_obsolete] for one umls.conf
    line. A malformed line raises ValueError naming it."""
    fields = [field.strip() for field in line.split(",")]
    if len(fields) not in (3, 4):
        raise ValueError("%s: expected SAB,file,strategy[,include_obsolete], got %r"
                         % (where, line))
    umls_code, file_out, load_on_field = fields[:3]
    option = fields[3] if len(fields) == 4 else ""
    if option not in ("", "include_obsolete"):
        raise ValueError("%s: unknown option %r; the only option is include_obsolete"
                         % (where, option))
    include_obsolete = option == "include_obsolete"
    if include_obsolete and load_on_field == "load_on_cuis":
        raise ValueError("%s: include_obsolete needs load_on_codes" % where)
    return [umls_code, file_out, load_on_field, include_obsolete]

def read_umls_conf(path):
    """Return the entries of a umls.conf file (see parse_umls_conf_line),
    skipping blank lines and lines starting with '#'."""
    if not os.path.isfile(path):
        raise FileNotFoundError(
            "Source list %s not found. Copy %s to umls.conf and edit it, or "
            "set UMLS_CONF in conf.py to the file's path." % (path, UMLS_CONF_SAMPLE))
    entries = []
    with open(path, "r") as fconf:
        for lineno, line in enumerate(fconf.read().splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            entries.append(parse_umls_conf_line(line, "%s:%d" % (path, lineno)))
    return entries

def get_language_tag(lat, ont_code):
    if lat not in UMLS_LANGCODE_MAP:
        raise ValueError(
            "No language tag mapping for MRSAB LAT '%s' (ontology %s); "
            "add it to UMLS_LANGCODE_MAP" % (lat, ont_code))
    return UMLS_LANGCODE_MAP[lat]

def warn(ont_code, message):
    sys.stderr.write("WARNING %s: %s\n" % (ont_code, message))
    sys.stderr.flush()

def run_git(*args):
    """Output of a git command run in this script's checkout, or None."""
    try:
        result = subprocess.run(
            ["git", "-C", os.path.dirname(os.path.abspath(__file__))] + list(args),
            capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()

def get_umls2rdf_tool():
    """Name and version of the umls2rdf code running now, for
    pav:createdWith: "umls2rdf " plus its git describe name, such as
    "umls2rdf v1.3.0" at a release tag or "umls2rdf v1.2.0-8-g3c22d00" after
    it. Uncommitted changes in the checkout are not marked.
    BioPortal stores this string as the submission's
    usedOntologyEngineeringTool. None (with a warning) when this is not a
    git checkout."""
    name = run_git("describe", "--tags", "--always")
    if not name:
        warn("umls2rdf", "cannot read the git version of this checkout; "
             "leaving pav:createdWith out of the headers")
        return None
    return "umls2rdf %s" % name

def get_umls_release_docs(mrdoc_records):
    """Collect the MRDOC rows the ontology header uses: the UMLS release date
    (YYYYMMDD, as MRDOC has it) and, for each source restriction level (SRL),
    NLM's description ("expanded_form") and URL ("uri")."""
    docs = {"release_date": None, "srl": {}}
    for record in mrdoc_records:
        dockey = record[MRDOC_DOCKEY]
        value = record[MRDOC_VALUE]
        doc_type = record[MRDOC_TYPE]
        if dockey == "RELEASE" and value == "umls.release.date" and doc_type == "release_info":
            docs["release_date"] = record[MRDOC_DESC]
        elif dockey == "SRL" and doc_type in ("expanded_form", "uri"):
            docs["srl"].setdefault(str(value), {})[doc_type] = record[MRDOC_DESC]
    return docs

def get_umls_url(code):
    return "%s%s/"%(conf.UMLS_BASE_URI,code)

def get_output_dir():
    return conf.OUTPUT_FOLDER

def flatten(matrix):
    return reduce(lambda x,y: x+y,matrix)

def escape(string):
    return string.replace("\\","\\\\").replace('"','\\"')

def format_release_date(raw, ont_code):
    """MRDOC's UMLS release date (YYYYMMDD) as YYYY-MM-DD, or None (with a
    warning naming ont_code) when it is missing or malformed."""
    if not raw:
        warn(ont_code, "no UMLS release date in MRDOC; "
             "leaving dcterms:issued out of the header")
        return None
    try:
        return datetime.datetime.strptime(str(raw).strip(), "%Y%m%d").date().isoformat()
    except ValueError:
        warn(ont_code, "MRDOC UMLS release date '%s' is not YYYYMMDD; "
             "leaving dcterms:issued out of the header" % raw)
        return None

def header_optional_lines(lines):
    """Header statements, each on its own line after dcterms:source."""
    return "".join(" ;\n    %s" % line for line in lines)

def semantic_types_header(umls_docs, tool):
    """owl:Ontology header for the semantic types file. Its version and
    source are the UMLS release, and its release date is the UMLS release
    date. There is no license line: the Semantic Network has no MRSAB row,
    so it has no source restriction level."""
    # BioPortal's STY versions are upper case (2026AB); conf.py may not be.
    umls_version = str(conf.UMLS_VERSION).strip().upper()
    lines = ["skos:altLabel %s" % turtle_string(SEMANTIC_TYPES_CODE, lang="en")]
    release_date = format_release_date((umls_docs or {}).get("release_date"),
                                       SEMANTIC_TYPES_CODE)
    if release_date:
        lines.append("dcterms:issued %s" % turtle_string(release_date, datatype="xsd:date"))
    lines.append("dcterms:language <%sen>" % LEXVO_ISO639_1)
    if tool:
        lines.append("pav:createdWith %s" % turtle_string(tool))
    return ONTOLOGY_HEADER.substitute(dict(
        uri=get_umls_url(SEMANTIC_TYPES_CODE),
        comment=turtle_string(ONTOLOGY_COMMENT % SEMANTIC_TYPES_CODE, lang="en"),
        label=turtle_string(SEMANTIC_TYPES_LABEL, lang="en"),
        imports="",
        versioninfo=turtle_string(umls_version),
        source=turtle_string("UMLS %s" % umls_version),
        optional_lines=header_optional_lines(lines),
    ))

def write_semantic_types_file(output_dir, sem_types, umls_docs=None, tool=None):
    """Write umls_semantictypes.ttl: prefixes, header, then the semantic
    types (generate_semantic_types with roots). Returns its path."""
    output_file = os.path.join(output_dir, SEMANTIC_TYPES_FILE)
    with codecs.open(output_file, "w", "utf-8") as semfile:
        semfile.write(PREFIXES)
        semfile.write(semantic_types_header(umls_docs, tool))
        semfile.write(sem_types)
    return output_file

def read_semantic_network(sn_dir):
    """Turtle that adds the Semantic Network's own data to the semantic types
    file, from SRDEF and SRSTR in sn_dir (the NET folder of the full UMLS
    release, or NLM's Semantic Network download). Each type gets its
    definition, usage note, example, abbreviation and tree number; the
    relations become object properties; and each type links to the types
    SRSTR relates it to (except blocked links, and isa, which is already
    rdfs:subClassOf)."""
    url = get_umls_url(SEMANTIC_TYPES_CODE)
    types, rels = {}, {}
    with codecs.open(os.path.join(sn_dir, "SRDEF"), "r", "utf-8") as srdef:
        for line in srdef:
            f = line.rstrip("\r\n").split("|")
            if len(f) < 10:
                continue
            rec = dict(ui=f[1], name=f[2], tn=f[3], defn=f[4], ex=f[5],
                       un=f[6], nh=f[7], abr=f[8], inv=f[9])
            (types if f[0] == "STY" else rels)[rec["name"]] = rec
    links = collections.defaultdict(set)
    with codecs.open(os.path.join(sn_dir, "SRSTR"), "r", "utf-8") as srstr:
        for line in srstr:
            f = line.rstrip("\r\n").split("|")
            if len(f) < 4 or f[1] == "isa" or f[3] == "B":
                continue
            if f[0] in types and f[1] in rels and f[2] in types:
                links[types[f[0]]["ui"]].add((rels[f[1]]["ui"], types[f[2]]["ui"]))

    def text(value):
        return value if value and value != "NULL" else ""

    rel_by_tn = {r["tn"]: r["ui"] for r in rels.values()}
    out = []
    for r in sorted(rels.values(), key=lambda r: r["ui"]):
        lines = ["<%s%s> a owl:ObjectProperty" % (url, r["ui"]),
                 "rdfs:label %s" % turtle_string(r["name"], lang="en"),
                 "skos:prefLabel %s" % turtle_string(r["name"], lang="en"),
                 "skos:notation %s" % turtle_string(r["ui"], datatype="xsd:string"),
                 "<%sRTN> %s" % (url, turtle_string(r["tn"], datatype="xsd:string"))]
        if text(r["defn"]):
            lines.append("skos:definition %s" % turtle_string(r["defn"], lang="en"))
        # Tree numbers as for the types: R3.1's parent is R3, R3's is R.
        parent = rel_by_tn.get(r["tn"].rsplit(".", 1)[0] if "." in r["tn"] else r["tn"][:-1])
        if parent:
            lines.append("rdfs:subPropertyOf <%s%s>" % (url, parent))
        if r["inv"] in rels and r["inv"] != r["name"]:
            lines.append("owl:inverseOf <%s%s>" % (url, rels[r["inv"]]["ui"]))
        out.append(" ;\n\t".join(lines) + " .\n")
    for t in sorted(types.values(), key=lambda t: t["ui"]):
        lines = ["<%s%s> <%sSTN> %s" % (url, t["ui"], url,
                                        turtle_string(t["tn"], datatype="xsd:string"))]
        if text(t["abr"]):
            lines.append("<%sABR> %s" % (url, turtle_string(t["abr"], datatype="xsd:string")))
        if t["nh"] == "Y":
            lines.append("<%sNH> %s" % (url, turtle_string("Y", datatype="xsd:string")))
        for prop, key in (("skos:definition", "defn"), ("skos:scopeNote", "un"),
                          ("skos:example", "ex")):
            if text(t[key]):
                lines.append("%s %s" % (prop, turtle_string(t[key], lang="en")))
        for rel_ui, target in sorted(links[t["ui"]]):
            lines.append("<%s%s> <%s%s>" % (url, rel_ui, url, target))
        out.append(" ;\n\t".join(lines) + " .\n")
    if not types or not rels:
        raise ValueError("%s: no semantic types or relations in SRDEF" % sn_dir)
    return "\n\n" + "\n".join(out)

def turtle_string(value, lang=None, datatype=None):
    value = escape(str(value))
    literal = '"""%s"""' % value if "\n" in value else '"%s"' % value
    if lang:
        return '%s@%s' % (literal, lang)
    if datatype:
        return '%s^^%s' % (literal, datatype)
    return literal

def get_mrsab_record(mrsab, ont_code):
    records = list(mrsab.scan(filt="RSAB = '" + ont_code + "'"))
    if not records:
        return None
    current_records = [record for record in records if record[MRSAB_CURVER] == 'Y']
    if current_records:
        return current_records[0]
    return records[0]

def get_url_term(ns,code):
    if ns[-1] == '/':
        ret = ns + urllib.parse.quote(code)
    else:
        ret = "%s/%s"%(ns,urllib.parse.quote(code))
    return ret

def get_rel_fragment(rel):
    return rel[MRREL_RELA] if rel[MRREL_RELA] else rel[MRREL_REL]


# NOTE: See UmlsOntology.terms() for the reason these functions use -1 and -2
# indices to obtain the source and target codes, respectively.
def get_rel_code_source(rel,on_cuis):
    return rel[-1] if not on_cuis else rel[MRREL_CUI2]
def get_rel_code_target(rel,on_cuis):
    return rel[-2] if not on_cuis else rel[MRREL_CUI1]

def get_code(reg,load_on_cuis):
    if load_on_cuis:
        return reg[MRCONSO_CUI]
    if reg[MRCONSO_CODE]:
        return reg[MRCONSO_CODE]
    raise AttributeError("No code on reg [%s]"%("|".join(reg)))

def __get_connection():
    con = pymysql.connect(host=conf.DB_HOST,user=conf.DB_USER,
              password=conf.DB_PASS,database=conf.DB_NAME)
    # UmlsTable.scan streams rows; give the server more than the default 60s
    # to wait on the client between network writes.
    with con.cursor() as cursor:
        cursor.execute("SET SESSION net_write_timeout = 3600")
    return con

def generate_semantic_types(con,with_roots=False):
    url = get_umls_url("STY")
    hierarchy = collections.defaultdict(lambda : list())
    all_nodes = list()
    mrsty = UmlsTable("MRSTY",con,
                load_select="SELECT DISTINCT TUI, STN, STY FROM MRSTY")
    ont = list()

    for stt in mrsty.scan():
        hierarchy[stt[1]].append(stt[0])
        sty_term = """<%s> a owl:Class ;
\tskos:notation "%s"^^xsd:string ;
\tskos:prefLabel "%s"@en .
"""%(url+stt[0],stt[0],stt[2])
        ont.append(sty_term)
        all_nodes.append(stt)

    for node in all_nodes:
        parent = node[1]
        if "." in parent:
           parent = ".".join(node[1].split(".")[0:-1])
        else:
            parent = parent[0:-1]

        rdfs_subclasses = []
        for x in hierarchy[parent]:
            if node[0] != x:
                rdfs_subclasses.append(
                        "<%s> rdfs:subClassOf <%s> ."%(url+node[0],url+x))

        if len(rdfs_subclasses) == 0 and with_roots:
                rdfs_subclasses = ["<%s> rdfs:subClassOf owl:Thing ."%(url+node[0])]

        for sc in rdfs_subclasses:
            ont.append(sc)
    data_ont_ttl = "\n".join(ont)
    return data_ont_ttl




class UmlsTable(object):
    def __init__(self,table_name,conn,load_select=None):
        self.table_name = table_name
        self.conn = conn
        self.load_select = load_select

    def mesh_tree(self):
        q = """select DISTINCT c1.code as parent, c2.code as child
from MRREL r, MRCONSO c1, MRCONSO c2 where r.sab = 'MSH' and r.rel = 'CHD'
and c1.cui = r.cui1
and c2.cui = r.cui2
and c2.code like 'D%'
and c1.code like 'D%'
and c1.sab = 'MSH'
and c2.sab = 'MSH'
        """
        cursor = self.conn.cursor()
        cursor.execute(q)
        result = cursor.fetchall()
        edges = collections.defaultdict(set)
        for record in result:
            edges[record[1]].add(record[0])
        return edges

    def count(self):
        q = "SELECT count(*) FROM %s"%self.table_name
        cursor = self.conn.cursor()
        cursor.execute(q)
        result = cursor.fetchall()
        for record in result:
            cursor.close()
            return int(record[0])

    def scan(self,filt=None,limit=None):
        # One query, streamed with an unbuffered cursor. The previous
        # LIMIT/OFFSET paging had no ORDER BY, so MySQL was free to return
        # rows in a different order per page (skipping or repeating rows),
        # and every page rescanned all the rows before its offset.
        # The connection cannot run another query until the cursor is closed,
        # so callers must consume (or close) the generator before the next scan.
        if self.load_select:
            q = self.load_select
        elif filt:
            q = "SELECT * FROM %s WHERE %s" % (self.table_name, filt)
        else:
            q = "SELECT * FROM %s" % self.table_name
        if limit and not self.load_select:
            q += " LIMIT %d" % int(limit)
        sys.stdout.write("[UMLS-Query] %s\n" % q)
        sys.stdout.flush()
        cursor = self.conn.cursor(pymysql.cursors.SSCursor)
        try:
            cursor.execute(q)
            for i, record in enumerate(cursor, 1):
                yield record
                if limit and i >= limit:
                    break
        finally:
            cursor.close()



class UmlsClass(object):
    def __init__(self,ns,atoms=None,rels=None,
                 defs=None,atts=None,rank=None,
                 rank_by_tty=None,sty=None,
                 sty_by_cui=None,load_on_cuis=False,
                 is_root=None,is_obsolete=False):
        self.ns = ns
        self.is_obsolete = is_obsolete
        self.atoms = atoms
        self.rels = rels
        self.defs = defs
        self.atts = atts
        self.rank = rank
        self.rank_by_tty = rank_by_tty
        self.sty = sty
        self.sty_by_cui = sty_by_cui
        self.load_on_cuis = load_on_cuis
        self.is_root = is_root
        self.class_properties = dict()

    def code(self):
        codes = set([get_code(x,self.load_on_cuis) for x in self.atoms])
        if len(codes) != 1:
            raise AttributeError("Only one code per term.")
        #if DEBUG:
            #sys.stderr.write(self.atoms)
            #sys.stderr.write(codes)
        return codes.pop()

    def getAltLabels(self,prefLabel):
        #is_pref_atoms =  filter(lambda x: x[MRCONSO_ISPREF] == 'Y', self.atoms)
        return set([atom[MRCONSO_STR] for atom in self.atoms if atom[MRCONSO_STR] != prefLabel])

    def getPrefLabel(self):
        if self.load_on_cuis:
            if len(self.atoms) == 1:
                return self.atoms[0][MRCONSO_STR]

            labels = set([x[MRCONSO_STR] for x in self.atoms])
            if len(labels) == 1:
                return labels.pop()

            is_pref_atoms =  [x for x in self.atoms if x[MRCONSO_ISPREF] == 'Y']
            if len(is_pref_atoms) == 0:
                return self.atoms[0][MRCONSO_STR]
            elif len(is_pref_atoms) == 1:
                return is_pref_atoms[0][MRCONSO_STR]

            is_pref_atoms =  [x for x in is_pref_atoms if x[MRCONSO_STT] == 'PF']
            if len(is_pref_atoms) == 0:
                return self.atoms[0][MRCONSO_STR]
            elif len(is_pref_atoms) == 1:
                return is_pref_atoms[0][MRCONSO_STR]

            is_pref_atoms =  [x for x in self.atoms if x[MRCONSO_TTY][0] == 'P']
            if len(is_pref_atoms) == 1:
                return is_pref_atoms[0][MRCONSO_STR]
            return self.atoms[0][MRCONSO_STR]
        else:
            #if ISPREF=Y is not 1 then we look into MRRANK.
            if len(self.rank) > 0:
                sort_key = \
                lambda x: int(self.rank[self.rank_by_tty[x[MRCONSO_TTY]][0]][MRRANK_RANK])
                mmrank_sorted_atoms = sorted(self.atoms,key=sort_key,reverse=True)
                return mmrank_sorted_atoms[0][MRCONSO_STR]
            #there is no rank to use
            else:
                pref_atom = [x for x in self.atoms if 'P' in x[MRCONSO_TTY]]
                if len(pref_atom) == 1:
                    return pref_atom[0][MRCONSO_STR]
            raise AttributeError("Unable to select pref label")

    def getURLTerm(self,code):
        return get_url_term(self.ns,code)

    def _append_object_triple(self, rdf_term, seen_triples, subject, predicate, obj):
        triple = (subject, predicate, obj)
        if seen_triples is not None:
            if triple in seen_triples:
                self.duplicate_triples_skipped["object"] += 1
                return rdf_term
            seen_triples.add(triple)
        return rdf_term + "\t<%s> <%s> ;\n" % (predicate, obj)

    def _append_literal_triple(self, rdf_term, seen_triples, subject, predicate, value):
        triple = (subject, predicate, value)
        if seen_triples is not None:
            if triple in seen_triples:
                self.duplicate_triples_skipped["literal"] += 1
                return rdf_term
            seen_triples.add(triple)
        return rdf_term + "\t<%s> \"\"\"%s\"\"\"^^xsd:string ;\n" % (predicate, escape(value))

    def _append_subclass_triple(self, rdf_term, seen_triples, subject, obj):
        triple = (subject, "rdfs:subClassOf", obj)
        if seen_triples is not None:
            if triple in seen_triples:
                self.duplicate_triples_skipped["subclass"] += 1
                return rdf_term
            seen_triples.add(triple)
        rendered_obj = "<%s>" % (obj,) if "://" in obj else obj
        return rdf_term + "\trdfs:subClassOf %s ;\n" % (rendered_obj,)

    def _sorted_rels(self):
        return sorted(
            self.rels,
            key=lambda rel: (
                rel[MRREL_REL] != 'CHD',
                get_rel_fragment(rel),
                get_rel_code_target(rel, self.load_on_cuis),
                get_rel_code_source(rel, self.load_on_cuis),
            ),
        )

    def _sorted_atts(self):
        return sorted(
            self.atts,
            key=lambda att: (
                att[MRSAT_ATN],
                att[MRSAT_ATV],
            ),
        )

    def properties(self):
        return self.class_properties

    def _dedupe_repeated_triples(self):
        return conf_setting("DEDUPE_CLASS_TRIPLES")

    def toRDF(self,fmt="Turtle",hierarchy=True,lang="en",tree=None):
        if not fmt == "Turtle":
            raise AttributeError("Only fmt='Turtle' is currently supported")
        self.duplicate_triples_skipped = {
            "literal": 0,
            "object": 0,
            "subclass": 0,
        }
        term_code = self.code()
        url_term = self.getURLTerm(term_code)
        prefLabel = self.getPrefLabel()
        altLabels = self.getAltLabels(prefLabel)
        rdf_term = """<%s> a owl:Class ;
\tskos:prefLabel \"\"\"%s\"\"\"@%s ;
\tskos:notation \"\"\"%s\"\"\"^^xsd:string ;
"""%(url_term,escape(prefLabel),lang,escape(term_code))
        if self.is_obsolete:
            rdf_term += "\towl:deprecated \"true\"^^xsd:boolean ;\n"
            # Obsolete classes take no parents or roots from the tree. Only
            # MSH has one; the other sources' hierarchy comes from CHD rels,
            # which build_term already drops for obsolete codes.
            tree = None

        seen_triples = set() if self._dedupe_repeated_triples() else None

        if len(altLabels) > 0:
            rdf_term += """\tskos:altLabel %s ;
"""%(" , ".join(['\"\"\"%s\"\"\"@%s'%(escape(x),lang) for x in sorted(set(altLabels))]))

        if self.is_root:
            rdf_term = self._append_subclass_triple(rdf_term, seen_triples, url_term, "owl:Thing")

        if len(self.defs) > 0:
            rdf_term += """\tskos:definition %s ;
"""%(" , ".join(['\"\"\"%s\"\"\"@%s'%(escape(x),lang) for x in sorted(set([d[MRDEF_DEF] for d in self.defs]))]))

        count_parents = 0
        if tree:
            if term_code in tree:
                for parent in sorted(tree[term_code]):
                    o = self.getURLTerm(parent)
                    rdf_term = self._append_subclass_triple(rdf_term, seen_triples, url_term, o)
        for rel in self._sorted_rels():
            source_code = get_rel_code_source(rel,self.load_on_cuis)
            target_code = get_rel_code_target(rel,self.load_on_cuis)
            if source_code != term_code:
                raise AttributeError("Inconsistent code in rel")
            # Map child relations to rdf:subClassOf (skip parent relations).
            if rel[MRREL_REL] == 'PAR':
                continue
            if rel[MRREL_REL] == 'CHD' and hierarchy:
                o = self.getURLTerm(target_code)
                count_parents += 1
                if target_code == "ICD-10-CM":
                    #skip bogus ICD10CM parent
                    continue
                if target_code == "138875005":
                    #skip bogus SNOMED root concept
                    continue
                if target_code == "V-HL7V3.0" or target_code == "C1553931":
                    #skip bogus HL7V3.0 root concept
                    continue
                if not tree:
                    rdf_term = self._append_subclass_triple(rdf_term, seen_triples, url_term, o)
            else:
                p = self.getURLTerm(get_rel_fragment(rel))
                o = self.getURLTerm(target_code)
                rdf_term = self._append_object_triple(rdf_term, seen_triples, url_term, p, o)
                if p not in self.class_properties:
                    self.class_properties[p] = \
                        UmlsAttribute(p,get_rel_fragment(rel))

        for att in self._sorted_atts():
            atn = att[MRSAT_ATN]
            atv = att[MRSAT_ATV]
            if atn == 'AQ':
                # Skip all these values (they are replicated in MRREL for
                # SNOMEDCT, unknown relationship for MSH).
                #if DEBUG:
                #  sys.stderr.write("att: %s\n" % str(att))
                #  sys.stderr.flush()
                continue
            #MESH ROOTS ONLY DESCRIPTORS
            if tree and atn == "MN" and term_code.startswith("D"):
                if len(atv.split(".")) == 1:
                    rdf_term = self._append_subclass_triple(rdf_term, seen_triples, url_term, "owl:Thing")
            p = self.getURLTerm(atn)
            rdf_term = self._append_literal_triple(rdf_term, seen_triples, url_term, p, atv)
            if p not in self.class_properties:
                self.class_properties[p] = UmlsAttribute(p,atn)

        #auis = set([x[MRCONSO_AUI] for x in self.atoms])
        cuis = sorted(set([x[MRCONSO_CUI] for x in self.atoms]))
        sty_recs = flatten([indexes for indexes in [self.sty_by_cui[cui] for cui in cuis]])
        types = sorted(set([self.sty[index][MRSTY_TUI] for index in sty_recs]))

        #for t in auis:
        #    rdf_term += """\t%s \"\"\"%s\"\"\"^^xsd:string ;\n"""%(HAS_AUI,t)
        for t in cuis:
            rdf_term += """\t%s \"\"\"%s\"\"\"^^xsd:string ;\n"""%(HAS_CUI,t)
        for t in types:
            rdf_term += """\t%s \"\"\"%s\"\"\"^^xsd:string ;\n"""%(HAS_TUI,t)
        for t in types:
            rdf_term += """\t%s <%s> ;\n"""%(HAS_STY,get_umls_url("STY")+t)

        return rdf_term + " .\n\n"



class UmlsAttribute(object):
    def __init__(self,uri,att):
        self.uri = uri
        self.att = att

    def getURLTerm(self,code):
        return get_url_term(self.ns,code)

    def toRDFWithDesc(self,label,desc,_type):
        uri_rdf = self.uri
        if self.uri.startswith("http"):
            uri_rdf = "<%s>"%self.uri
        return """%s a owl:%s ;
    \trdfs:label \"\"\"%s\"\"\";
    \trdfs:comment \"\"\"%s\"\"\" .
    \n"""%(uri_rdf,_type,label,escape(desc))

    def toRDF(self,dockey,desc,fmt="Turtle"):
        if not fmt == "Turtle":
            raise AttributeError("Only fmt='Turtle' is currently supported")
        _type = ""
        if "REL" in dockey:
            _type = "ObjectProperty"
        elif dockey == "ATN":
            _type = "DatatypeProperty"
        else:
            raise AttributeError("Unknown DOCKEY" + dockey)

        label = self.att
        if len(desc) < 20:
            label = desc
        if "_" in label:
            label = " ".join(self.att.split("_"))
            label = label[0].upper() + label[1:]

        return """<%s> a owl:%s ;
\trdfs:label \"\"\"%s\"\"\";
\trdfs:comment \"\"\"%s\"\"\" .
\n"""%(self.uri,_type,label,escape(desc))



class UmlsOntology(object):
    def __init__(self,ont_code,ns,con,load_on_cuis=False,include_obsolete=False):
        self.loaded = False
        self.ont_code = ont_code
        # Obsolete codes are a code-level notion, so only in load_on_codes mode.
        self.include_obsolete = include_obsolete and not load_on_cuis
        self.obsolete_codes = set()
        self.ns = ns
        self.con = con
        self.load_on_cuis = load_on_cuis
        #self.alt_uri_code = alt_uri_code
        self.atoms = list()
        self.atoms_by_code = collections.defaultdict(lambda : list())
        if not self.load_on_cuis:
            self.atoms_by_aui = collections.defaultdict(lambda : list())
        self.rels = list()
        self.rels_by_aui_src = collections.defaultdict(lambda : list())
        self.defs = list()
        self.defs_by_aui = collections.defaultdict(lambda : list())
        self.atts = list()
        self.atts_by_code = collections.defaultdict(lambda : list())
        self.rank = list()
        self.rank_by_tty = collections.defaultdict(lambda : list())
        self.sty = list()
        self.sty_by_cui = collections.defaultdict(lambda : list())
        self.cui_roots = set()
        self.lang = None
        self.ont_properties = dict()
        self.mrsab_record = None
        # Output of get_umls_release_docs(); None leaves the release date and
        # license out of the header.
        self.umls_docs = None
        # Output of get_umls2rdf_tool(); None leaves pav:createdWith out.
        self.tool = None

    def mrsab_value(self, idx, default=None):
        if self.mrsab_record and self.mrsab_record[idx]:
            return self.mrsab_record[idx]
        return default

    def ontology_version(self):
        return self.mrsab_value(MRSAB_SVER, conf.UMLS_VERSION)

    def ontology_label(self):
        return self.mrsab_value(MRSAB_SSN, self.ont_code)

    def ontology_source(self):
        imeta = self.mrsab_value(MRSAB_IMETA)
        if imeta:
            return "UMLS %s" % imeta
        return "UMLS %s" % conf.UMLS_VERSION

    def ontology_alt_label(self):
        return self.mrsab_value(MRSAB_RSAB)

    def ontology_release_date(self):
        """UMLS release date as YYYY-MM-DD, or None (with a warning)."""
        return format_release_date((self.umls_docs or {}).get("release_date"),
                                   self.ont_code)

    def ontology_license(self):
        """(license URL, use guidelines text) for the source's restriction
        level (MRSAB SRL), or None (with a warning)."""
        # Not mrsab_value(): SRL 0 is a valid level but falsy.
        srl = self.mrsab_record[MRSAB_SRL] if self.mrsab_record else None
        if srl is None or str(srl).strip() == "":
            warn(self.ont_code, "no SRL in MRSAB; "
                 "leaving dcterms:license and cc:useGuidelines out of the header")
            return None
        level = str(srl).strip()
        doc = (self.umls_docs or {}).get("srl", {}).get(level, {})
        uri = doc.get("uri")
        terms = doc.get("expanded_form")
        if not uri or not terms:
            warn(self.ont_code, "no MRDOC uri and expanded_form for SRL %s; "
                 "leaving dcterms:license and cc:useGuidelines out of the header" % level)
            return None
        terms = terms.strip()
        if not terms.endswith("."):
            terms += "."
        return uri, UMLS_LICENSE_NOTICE % dict(level=level, terms=terms, uri=uri)

    def ontology_optional_lines(self, lang_tag):
        """Header statements that are left out when their data is missing."""
        lines = []
        alt_label = self.ontology_alt_label()
        if alt_label:
            lines.append("skos:altLabel %s" % turtle_string(alt_label, lang="en"))
        release_date = self.ontology_release_date()
        if release_date:
            lines.append("dcterms:issued %s" % turtle_string(release_date, datatype="xsd:date"))
        lines.append("dcterms:language <%s%s>" % (LEXVO_ISO639_1, lang_tag.split("-")[0]))
        license_info = self.ontology_license()
        if license_info:
            license_uri, use_guidelines = license_info
            lines.append("dcterms:license <%s>" % license_uri)
            lines.append("cc:useGuidelines %s" % turtle_string(use_guidelines, lang="en"))
        if self.tool:
            lines.append("pav:createdWith %s" % turtle_string(self.tool))
        return header_optional_lines(lines)

    def should_process_current_version(self):
        return self.mrsab_value(MRSAB_IMETA) == conf.UMLS_VERSION

    def load_tables(self,limit=None):
        mrconso = UmlsTable("MRCONSO",self.con)
        if self.ont_code == 'MSH':
            self.tree = mrconso.mesh_tree()
        else:
            self.tree = None
        mrsab  = UmlsTable("MRSAB", self.con)
        self.mrsab_record = get_mrsab_record(mrsab, self.ont_code)
        self.lang = self.mrsab_value(MRSAB_LAT)
        if not self.lang:
            raise ValueError("No LAT found in MRSAB for ontology %s" % self.ont_code)
        self.lang = self.lang.lower()
        # Fail before loading anything rather than failing on every term.
        get_language_tag(self.lang, self.ont_code)
        mrconso_filt = "SAB = '%s' AND lat = '%s' AND SUPPRESS = 'N'"%(
                                                    self.ont_code,self.lang)
        for atom in mrconso.scan(filt=mrconso_filt,limit=limit):
            index = len(self.atoms)
            self.atoms_by_code[get_code(atom,self.load_on_cuis)].append(index)
            if not self.load_on_cuis:
                self.atoms_by_aui[atom[MRCONSO_AUI]].append(index)
            self.atoms.append(atom)
        if DEBUG:
            sys.stderr.write("length atoms: %d\n" % len(self.atoms))
            sys.stderr.write("length atoms_by_aui: %d\n" % len(self.atoms_by_aui))
            sys.stderr.write("atom example: %s\n\n" % str(self.atoms))
            sys.stderr.flush()

        if self.include_obsolete:
            mrconso_filt = "SAB = '%s' AND lat = '%s' AND SUPPRESS = 'O'"%(
                                                    self.ont_code,self.lang)
            self.add_obsolete_codes(mrconso.scan(filt=mrconso_filt,limit=limit))
            sys.stdout.write("%s obsolete codes: %d\n" % (
                self.ont_code, len(self.obsolete_codes)))
            sys.stdout.flush()

        mrconso_filt = "SAB = 'SRC' AND CODE = 'V-%s'"%self.ont_code
        for atom in mrconso.scan(filt=mrconso_filt,limit=limit):
            self.cui_roots.add(atom[MRCONSO_CUI])
        if DEBUG:
            sys.stderr.write("length cui_roots: %d\n\n" % len(self.cui_roots))
            sys.stderr.flush()

        #
        mrrel = UmlsTable("MRREL",self.con)
        mrrel_filt = "SAB = '%s' AND SUPPRESS = 'N'"%self.ont_code
        field = MRREL_AUI2 if not self.load_on_cuis else MRREL_CUI2
        for rel in mrrel.scan(filt=mrrel_filt,limit=limit):
            index = len(self.rels)
            self.rels_by_aui_src[rel[field]].append(index)
            self.rels.append(rel)
        if DEBUG:
            sys.stderr.write("length rels: %d\n\n" % len(self.rels))
            sys.stderr.flush()
        #
        mrdef = UmlsTable("MRDEF",self.con)
        mrdef_filt = "SAB = '%s'"%self.ont_code
        field = MRDEF_AUI if not self.load_on_cuis else MRDEF_CUI
        for defi in mrdef.scan(filt=mrdef_filt):
            index = len(self.defs)
            self.defs_by_aui[defi[field]].append(index)
            self.defs.append(defi)
        if DEBUG:
            sys.stderr.write("length defs: %d\n\n" % len(self.defs))
            sys.stderr.flush()
        #
        mrsat = UmlsTable("MRSAT",self.con)
        # SAB is the source that asserts the attribute, not the atom's source:
        # RXNORM gives RXCUIs to MMSL, GS, ... atoms, whose CODE is that
        # source's code and can match an unrelated RXNORM code
        # (ncbo/umls2rdf#30). Keep atom attributes on this source's atoms only.
        mrsat_filt = ("SAB = '%s' AND CODE IS NOT NULL AND (STYPE <> 'AUI' OR "
                      "EXISTS (SELECT 1 FROM MRCONSO c "
                      "WHERE c.AUI = MRSAT.METAUI AND c.SAB = MRSAT.SAB))"
                      % self.ont_code)
        field = MRSAT_CODE if not self.load_on_cuis else MRSAT_CUI
        for att in mrsat.scan(filt=mrsat_filt):
            index = len(self.atts)
            self.atts_by_code[att[field]].append(index)
            self.atts.append(att)
        if DEBUG:
            sys.stderr.write("length atts: %d\n\n" % len(self.atts))
            sys.stderr.flush()
        #
        mrrank = UmlsTable("MRRANK",self.con)
        mrrank_filt = "SAB = '%s'"%self.ont_code
        for rank in mrrank.scan(filt=mrrank_filt):
            index = len(self.rank)
            self.rank_by_tty[rank[MRRANK_TTY]].append(index)
            self.rank.append(rank)
        if DEBUG:
            sys.stderr.write("length rank: %d\n\n" % len(self.rank))
            sys.stderr.flush()
        #
        load_mrsty = "SELECT sty.* FROM MRSTY sty, MRCONSO conso \
        WHERE conso.SAB = '%s' AND conso.cui = sty.cui AND conso.suppress %s"
        # Obsolete codes' CUIs are only reachable through their obsolete atoms.
        load_mrsty %= (self.ont_code,
                       "IN ('N','O')" if self.include_obsolete else "= 'N'")
        mrsty = UmlsTable("MRSTY",self.con,load_select=load_mrsty)
        for sty in mrsty.scan(filt=None):
            index = len(self.sty)
            self.sty_by_cui[sty[MRSTY_CUI]].append(index)
            self.sty.append(sty)
        if DEBUG:
            sys.stderr.write("length sty: %d\n\n" % len(self.sty))
            sys.stderr.flush()
        # Track the loaded status
        self.loaded = True
        sys.stdout.write("%s tables loaded ...\n" % self.ont_code)
        sys.stdout.flush()

    def add_obsolete_codes(self, obsolete_atoms):
        """Add the obsolete ('O') atoms of codes that have no current atom.

        Such codes are obsolete: the source no longer uses them, but UMLS still
        ships their names and the source's history links (SNOMED CT's same_as,
        replaced_by, possibly_equivalent_to, ...). They are exported as
        owl:deprecated classes so their URIs keep resolving and point to their
        replacements. Obsolete atoms of current codes are not added: they
        would add stale synonyms to classes that are still in use.
        """
        current_codes = set(self.atoms_by_code)
        for atom in obsolete_atoms:
            code = atom[MRCONSO_CODE]
            if not code or code in current_codes:
                continue
            index = len(self.atoms)
            self.atoms_by_code[code].append(index)
            self.atoms_by_aui[atom[MRCONSO_AUI]].append(index)
            self.atoms.append(atom)
            self.obsolete_codes.add(code)

    def terms(self):
        if not self.loaded:
            self.load_tables()
        for code in self.atoms_by_code:
            yield self.build_term(code)

    def build_term(self, code):
        # Note: most UMLS ontologies are 'load_on_codes' (only HL7 is load_on_cuis)
        obsolete = code in self.obsolete_codes
        code_atoms = [self.atoms[row] for row in self.atoms_by_code[code]]
        field = MRCONSO_CUI if self.load_on_cuis else MRCONSO_AUI
        ids = [x[field] for x in code_atoms]
        rels = list()
        for _id in ids:
            rels += [self.rels[x] for x in self.rels_by_aui_src[_id]]
        rels_to_class = list()
        is_root = False
        if self.load_on_cuis:
            rels_to_class = rels
            for rel in rels_to_class:
                if rel[MRREL_CUI1] in self.cui_roots:
                    is_root = True
                    break
        else:
            for rel in rels:
                rel_with_codes = list(rel)
                aui_source = rel[MRREL_AUI2]
                aui_target = rel[MRREL_AUI1]
                code_source = [ get_code(self.atoms[x],self.load_on_cuis) \
                                    for x in self.atoms_by_aui[aui_source] ]
                code_target = [ get_code(self.atoms[x],self.load_on_cuis) \
                                    for x in self.atoms_by_aui[aui_target] ]
                # TODO: Check use of CUI1 (target) or CUI2 (source) here:
                if (rel[MRREL_CUI1] in self.cui_roots) and rel[MRREL_REL] == "CHD":
                    is_root = True
                elif self.ont_code == "ICD10CM":
                    # TODO: patch to fix ICD10-CM hierachy.
                    if rel[MRREL_CUI1] == "C3264380" and rel[MRREL_REL] == "CHD":
                        is_root = True

                if len(code_source) != 1 or len(code_target) > 1:
                    raise AttributeError("more than one or none codes")
                if len(code_source) == 1 and len(code_target) == 1 and \
                    code_source[0] != code_target[0]:
                    code_source = code_source[0]
                    code_target = code_target[0]
                    if code_target in self.obsolete_codes and not obsolete:
                        # Leave out links from current codes to obsolete ones,
                        # so current classes are exactly as without obsolete codes.
                        continue
                    if obsolete and rel[MRREL_REL] in ('CHD', 'PAR'):
                        # Obsolete codes stay out of the class tree.
                        continue
                    # NOTE: the order of these append operations below is important.
                    # get_rel_code_source() - it uses [-1]
                    # get_rel_code_target() - it uses [-2]
                    # which are called from UmlsClass.toRDF().
                    rel_with_codes.append(code_target)
                    rel_with_codes.append(code_source)
                    rels_to_class.append(rel_with_codes)
        aui_codes_def = [self.defs_by_aui[_id] for _id in ids]
        aui_codes_def = [item for sublist in aui_codes_def for item in sublist]
        defs = [self.defs[index] for index in aui_codes_def]
        atts = [self.atts[x] for x in self.atts_by_code[code]]

        umls_class = UmlsClass(self.ns,atoms=code_atoms,rels=rels_to_class,
            defs=defs,atts=atts,rank=self.rank,rank_by_tty=self.rank_by_tty,
            sty=self.sty, sty_by_cui=self.sty_by_cui,
            load_on_cuis=self.load_on_cuis,is_root=is_root and not obsolete,
            is_obsolete=obsolete)

        # TODO: patch to fix roots in SNOMED-CT
        # suppress should fix this one
        #if self.ont_code == "SNOMEDCT_US":
        #    umls_class.is_root = (umls_class.code() == "138875005")

        return umls_class

    def write_into(self,file_path,hierarchy=True):
        # Load (and validate the language) before opening the output file, so a
        # failure here does not leave a header-only file behind.
        if not self.loaded:
            self.load_tables()
        lang_tag = get_language_tag(self.lang, self.ont_code)
        sys.stdout.write("%s writing terms ... %s\n" % (self.ont_code, file_path))
        sys.stdout.flush()
        fout = codecs.open(file_path,"w","utf-8")
        #nterms = len(self.atoms_by_code)
        fout.write(PREFIXES)
        fout.write(ONTOLOGY_HEADER.substitute(dict(
            uri=self.ns,
            comment=turtle_string(ONTOLOGY_COMMENT % self.ont_code, lang="en"),
            label=turtle_string(self.ontology_label(), lang="en"),
            imports=SKOS_IMPORT,
            versioninfo=turtle_string(self.ontology_version()),
            source=turtle_string(self.ontology_source()),
            optional_lines=self.ontology_optional_lines(lang_tag)
        )))
        duplicate_counts = self._empty_duplicate_counts()
        self.terms_written = 0
        self.obsolete_written = 0
        self.term_errors = []
        # Build each term inside the try: an exception raised while building
        # a term from inside the terms() generator would end the generator
        # and abort the whole ontology.
        for code in self.atoms_by_code:
            term = None
            try:
                term = self.build_term(code)
                rdf_text = term.toRDF(lang=lang_tag,tree=self.tree)
                fout.write(rdf_text)
                self.terms_written += 1
                if term.is_obsolete:
                    self.obsolete_written += 1
                for kind, count in term.duplicate_triples_skipped.items():
                    duplicate_counts[kind] += count
            except Exception as e:
                self.term_errors.append(code)
                sys.stderr.write("ERROR %s: term %s not written: %s: %s\n" % (
                    self.ont_code, code, type(e).__name__, e))
                sys.stderr.flush()

            if term is None:
                continue
            for att in term.properties():
                if att not in self.ont_properties:
                    self.ont_properties[att] = term.properties()[att]

        sys.stdout.write("%s terms written: %d (obsolete: %d), failed: %d\n" % (
            self.ont_code, self.terms_written, self.obsolete_written,
            len(self.term_errors)))
        sys.stdout.flush()

        if conf_setting("DEDUPE_CLASS_TRIPLES"):
            sys.stdout.write(
                "%s duplicate triples skipped: literal=%d object=%d subclass=%d\n"
                % (
                    self.ont_code,
                    duplicate_counts["literal"],
                    duplicate_counts["object"],
                    duplicate_counts["subclass"],
                )
            )
            sys.stdout.flush()

        return fout

    def properties(self):
        return self.ont_properties

    def _empty_duplicate_counts(self):
        return {
            "literal": 0,
            "object": 0,
            "subclass": 0,
        }

    def write_properties(self,fout,property_docs):
        self.ont_properties["hasSTY"] =\
                UmlsAttribute(HAS_STY,"hasSTY")
        for p in self.ont_properties:
            prop = self.ont_properties[p]
            if "hasSTY"  in p:
                fout.write(prop.toRDFWithDesc(
                    "Semantic type UMLS property",
                    "Semantic type UMLS property",
                    "ObjectProperty"))
                continue
            doc = property_docs[prop.att]
            if "expanded_form" not in doc:
                raise AttributeError("expanded form not found in " + doc)
            _desc = doc["expanded_form"]
            if "inverse" in doc:
                _desc = "Inverse of " + doc["inverse"]

            _dockey = doc["dockey"]
            fout.write(prop.toRDF(_dockey,_desc))

    def write_semantic_types(self,sem_types,fout):
        fout.write(sem_types)
        fout.write("\n")


def get_manifest_path(output_dir):
    return os.path.join(output_dir, MANIFEST_FILENAME)

def remove_manifest(output_dir):
    path = get_manifest_path(output_dir)
    if os.path.exists(path):
        os.remove(path)

def write_manifest(output_dir, ontologies, skipped):
    manifest = {
        "umls_version": conf.UMLS_VERSION,
        "semantic_types_file": SEMANTIC_TYPES_FILE,
        "ontologies": ontologies,
        "skipped": skipped,
    }
    path = get_manifest_path(output_dir)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as fmanifest:
        json.dump(manifest, fmanifest, indent=2, sort_keys=True)
        fmanifest.write("\n")
    os.replace(tmp_path, path)

def ontologies_over_error_limit(results, max_errors):
    return [r for r in results if r["term_errors"] > max_errors]

def export_ontology(con, umls_code, file_out, load_on_field, include_obsolete,
                    output_dir, property_docs, sem_types, umls_docs=None, tool=None):
    """Write one umls.conf entry. Returns its manifest entry, or None if skipped."""
    alt_uri_code = None
    if ";" in umls_code:
        umls_code,alt_uri_code = umls_code.split(";")
    load_on_cuis = load_on_field == "load_on_cuis"
    output_file = os.path.join(output_dir,file_out)
    sys.stdout.write("Generating %s (using '%s')\n" %
            (umls_code,load_on_field))
    sys.stdout.flush()
    ns = get_umls_url(umls_code if not alt_uri_code else alt_uri_code)
    ont = UmlsOntology(umls_code,ns,con,load_on_cuis=load_on_cuis,
                       include_obsolete=include_obsolete)
    mrsab = UmlsTable("MRSAB", con)
    ont.mrsab_record = get_mrsab_record(mrsab, umls_code)
    ont.umls_docs = umls_docs
    ont.tool = tool
    if conf_setting("PROCESS_ONLY_CURRENT_UMLS_VERSION") and not ont.should_process_current_version():
        sys.stdout.write("Skipping %s: IMETA=%s, expected %s\n\n" % (
            umls_code,
            ont.mrsab_value(MRSAB_IMETA, "None"),
            conf.UMLS_VERSION))
        sys.stdout.flush()
        return None
    ont.load_tables()
    fout = ont.write_into(output_file,hierarchy=(ont.ont_code != "MSH"))
    try:
        ont.write_properties(fout,property_docs)
        if conf_setting("INCLUDE_SEMANTIC_TYPES"):
          ont.write_semantic_types(sem_types,fout)
    finally:
        fout.close()
    sys.stdout.write("done!\n\n")
    sys.stdout.flush()
    return {
        "sab": umls_code,
        "file": file_out,
        "terms_written": ont.terms_written,
        "obsolete_written": ont.obsolete_written,
        "term_errors": len(ont.term_errors),
    }

def export_ontologies(con, umls_conf, output_dir, property_docs, sem_types,
                      umls_docs=None, tool=None):
    """Export every umls.conf entry. One ontology failing (for example a LAT
    with no UMLS_LANGCODE_MAP entry) is logged and the rest still run.
    Returns (results, skipped SABs, failed SABs)."""
    results = []
    skipped = []
    failures = []
    for (umls_code, file_out, load_on_field, include_obsolete) in umls_conf:
        if umls_code.startswith("#"):
            continue
        sab = umls_code.split(";")[0]
        try:
            result = export_ontology(con, umls_code, file_out, load_on_field,
                                     include_obsolete, output_dir, property_docs,
                                     sem_types, umls_docs=umls_docs, tool=tool)
        except pymysql.err.OperationalError:
            # Lost connection and similar: every later ontology would fail too.
            raise
        except Exception as e:
            failures.append(sab)
            sys.stderr.write("ERROR %s: ontology not exported: %s: %s\n" % (
                sab, type(e).__name__, e))
            traceback.print_exc()
            sys.stderr.write("\n")
            sys.stderr.flush()
            continue
        if result is None:
            skipped.append(sab)
        else:
            results.append(result)
    return results, skipped, failures


if __name__ == "__main__":

    # Read the source list before connecting, so a missing file fails fast.
    umls_conf_path = get_umls_conf_path()
    umls_conf = read_umls_conf(umls_conf_path)
    sys.stdout.write("Using source list %s (%d entries)\n" % (
        os.path.abspath(umls_conf_path), len(umls_conf)))
    sys.stdout.flush()
    semantic_network = ""
    if conf_setting("ADD_STY_DEFINITIONS_AND_RELATIONS"):
        sn_dir = os.path.expanduser(conf_setting("SEMANTIC_NETWORK_DIR"))
        missing = [f for f in SEMANTIC_NETWORK_FILES
                   if not os.path.isfile(os.path.join(sn_dir, f))]
        if missing:
            sys.exit("ADD_STY_DEFINITIONS_AND_RELATIONS is on, but %s has no %s. "
                     "Run python download_semantic_network.py first."
                     % (sn_dir, " or ".join(missing)))
        semantic_network = read_semantic_network(sn_dir)

    con = __get_connection()

    output_dir = get_output_dir()
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    remove_manifest(output_dir)

    mrdoc_records = list(UmlsTable("MRDOC", con).scan())
    umls_docs = get_umls_release_docs(mrdoc_records)
    tool = get_umls2rdf_tool()

    write_semantic_types_file(output_dir,
                              generate_semantic_types(con,with_roots=True) + semantic_network,
                              umls_docs, tool)

    sem_types = generate_semantic_types(con,with_roots=False)
    property_docs = dict()
    for doc_record in mrdoc_records:
        _type = doc_record[MRDOC_TYPE]
        _expl = doc_record[MRDOC_DESC]
        _key = doc_record[MRDOC_VALUE]
        if _key not in property_docs:
            property_docs[_key] = dict()
            property_docs[_key]["dockey"] = doc_record[MRDOC_DOCKEY]
        if "inverse" in _type:
            _type = "inverse"
        property_docs[_key][_type] = _expl

    results, skipped, ontology_failures = export_ontologies(
        con, umls_conf, output_dir, property_docs, sem_types, umls_docs, tool)
    sys.stdout.flush()

    max_errors = conf_setting("MAX_TERM_ERRORS")
    failed = ontologies_over_error_limit(results, max_errors)
    if failed or ontology_failures:
        for r in failed:
            sys.stderr.write("FAILED %s: %d terms not written (MAX_TERM_ERRORS=%d)\n" % (
                r["sab"], r["term_errors"], max_errors))
        for sab in ontology_failures:
            sys.stderr.write("FAILED %s: ontology not exported\n" % sab)
        sys.stderr.write("No manifest written; see ERROR lines above.\n")
        sys.stderr.flush()
        sys.exit(1)
    write_manifest(output_dir, results, skipped)
    sys.stdout.write("Wrote %s\n" % get_manifest_path(output_dir))
    sys.stdout.flush()
