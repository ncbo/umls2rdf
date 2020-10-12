#! /usr/bin/env python

DEBUG = False

import codecs
import sys
import os
import urllib
from string import Template
import collections
import pymysql
import pdb
from rxnorm_tty_rank import rank, rank_by_tty
#from itertools import groupby

try:
    import conf
except:
    raise

PREFIXES = """
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix umls: <http://bioportal.bioontology.org/ontologies/umls/> .

"""

ONTOLOGY_HEADER = Template("""
<$uri>
    a owl:Ontology ;
    rdfs:comment "$comment" ;
    rdfs:label "$label" ;
    owl:imports <http://www.w3.org/2004/02/skos/core> ;
    owl:versionInfo "$versioninfo" .

""")

STY_URL = "http://bioportal.bioontology.org/ontologies/umls/sty/"
HAS_STY = "umls:hasSTY"
HAS_AUI = "umls:aui"
HAS_CUI = "umls:cui"
HAS_TUI = "umls:tui"

# mysql> select * from rxnorm.RXNCONSO limit 1;
# +-------+-----+------+------+------+------+--------+---------+------+----------+------+-------------+-----+----------+-----------------------------------+------+----------+------+
# | RXCUI | LAT | TS   | LUI  | STT  | SUI  | ISPREF | RXAUI   | SAUI | SCUI     | SDUI | SAB         | TTY | CODE     | STR                               | SRL  | SUPPRESS | CVF  |
#     0      1     2      3      4      5       6        7        8       9         10     11            12    13         14                                 15        16       17
# +-------+-----+------+------+------+------+--------+---------+------+----------+------+-------------+-----+----------+-----------------------------------+------+----------+------+
# | 3     | ENG |      |      |      |      |        | 8717795 |      | 58488005 |      | SNOMEDCT_US | PT  | 58488005 | 1,4-alpha-Glucan branching enzyme |      | N        |      |
# +-------+-----+------+------+------+------+--------+---------+------+----------+------+-------------+-----+----------+-----------------------------------+------+----------+------+
# 1 row in set (0.01 sec)
RXNCONSO_RXCUI = 0
RXNCONSO_RXAUI = 7
RXNCONSO_STR = 14
RXNCONSO_STT = 4
RXNCONSO_ISPREF = 6
RXNCONSO_TTY = 12

# mysql> select * from rxnorm.RXNREL limit 1;
# +--------+--------+--------+------+--------+--------+--------+------------------------+---------+------+--------+------+------+------+----------+------+
# | RXCUI1 | RXAUI1 | STYPE1 | REL  | RXCUI2 | RXAUI2 | STYPE2 | RELA                   | RUI     | SRUI | SAB    | SL   | DIR  | RG   | SUPPRESS | CVF  |
#     0        1        2       3       4        5        6        7                       8          9     10      11     12     13        14       15
# +--------+--------+--------+------+--------+--------+--------+------------------------+---------+------+--------+------+------+------+----------+------+
# | 317836 |        | CUI    | RO   | 317483 |        | CUI    | has_precise_ingredient | 3777458 |      | RXNORM |      |      |      |          |      |
# +--------+--------+--------+------+--------+--------+--------+------------------------+---------+------+--------+------+------+------+----------+------+
# 1 row in set (0.00 sec)
RXNREL_RXAUI1 = 1
RXNREL_RXAUI2 = 5
RXNREL_RXCUI1 = 0
RXNREL_RXCUI2 = 4
RXNREL_REL = 3
RXNREL_RELA = 7

# mysql> select * from rxnorm.RXNSAT limit 1;
# +--------+------+------+---------+-------+-----------+------+-------+-----------+--------+--------+----------+------+
# | RXCUI  | LUI  | SUI  | RXAUI   | STYPE | CODE      | ATUI | SATUI | ATN       | SAB    | ATV    | SUPPRESS | CVF  |
#     0       1      2       3         4      5           6       7      8           9       10          11      12
# +--------+------+------+---------+-------+-----------+------+-------+-----------+--------+--------+----------+------+
# | 858625 |      |      | 5028317 | AUI   | 65862-607 |      |       | DM_SPL_ID | MTHSPL | 470661 | N        | 4096 |
# +--------+------+------+---------+-------+-----------+------+-------+-----------+--------+--------+----------+------+
# 1 row in set (0.01 sec)
RXNSAT_RXCUI = 0
RXNSAT_ATV = 10
RXNSAT_ATN = 8

# mysql> select * from rxnorm.RXNDOC limit 1;
# +--------+----------+---------------+----------+
# | DOCKEY | VALUE    | TYPE          | EXPL     |
#     0        1         2               3
# +--------+----------+---------------+----------+
# | ATN    | AAL_TERM | expanded_form | AAL term |
# +--------+----------+---------------+----------+
# 1 row in set (0.01 sec)
RXNDOC_DOCKEY = 0
RXNDOC_VALUE = 1
RXNDOC_TYPE = 2
RXNDOC_DESC = 3

RXN_TTY_RANK = 0

# mysql> select * from rxnorm.RXNSTY limit 1;
# +-------+------+--------------+--------+------+------+
# | RXCUI | TUI  | STN          | STY    | ATUI | CVF  |
#     0      1      2              3        4      5
# +-------+------+--------------+--------+------+------+
# | 3     | T126 | A1.4.1.1.3.3 | Enzyme |      |      |
# +-------+------+--------------+--------+------+------+
# 1 row in set (0.00 sec)
RXNSTY_RXCUI = 0
RXNSTY_TUI = 1

# mysql> SELECT * FROM rxnorm.RXNSAB WHERE RSAB = 'RXNORM';
# +----------+----------+---------------------+--------+-------------------+--------+--------------+--------+------+--------+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+------+------+------+-------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+-------+--------+-------+------------------------------------------------------+--------------------------------------------------------------------------------------------------+
# | VCUI     | RCUI     | VSAB                | RSAB   | SON               | SF     | SVER         | VSTART | VEND | IMETA  | RMETA | SLC                                                                                                                                                                                                  | SCC                                                                                                                                                                                                  | SRL  | TFR  | CFR  | CXTY | TTYL                                                                                | ATNL                                                                                                                                                                                                                                                                                                                                                                                       | LAT  | CENC  | CURVER | SABIN | SSN                                                  | SCIT                                                                                             |
#    0          1           2                     3       4                   5        6                7       8      9        10     11                                                                                                                                                                                                     12                                                                                                                                                                                                     13     14     15     16     17                                                                                    18                                                                                                                                                                                                                                                                                                                                                                                           19     20       21      22      23                                                     24
# +----------+----------+---------------------+--------+-------------------+--------+--------------+--------+------+--------+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+------+------+------+-------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+-------+--------+-------+------------------------------------------------------+--------------------------------------------------------------------------------------------------+
# | C5233835 | C1140284 | RXNORM_20AA_200908F | RXNORM | RxNorm Vocabulary | RXNORM | 20AA_200908F |        |      | 2020AA |       | RxNorm Customer Service;;U.S. National Library of Medicine;8600 Rockville Pike;;Bethesda;MD;United States;20894;(888) FIND-NLM;;rxnorminfo@nlm.nih.gov;https://www.nlm.nih.gov/research/umls/rxnorm/ | RxNorm Customer Service;;U.S. National Library of Medicine;8600 Rockville Pike;;Bethesda;MD;United States;20894;(888) FIND-NLM;;rxnorminfo@nlm.nih.gov;https://www.nlm.nih.gov/research/umls/rxnorm/ |    0 | NULL | NULL |      | IN,SBDC,SBDF,SCD,BPCK,SBD,SCDG,SBDG,MIN,BN,GPCK,PIN,ET,SCDF,SY,DFG,SCDC,TMSY,DF,PSN | NDC,RXN_IN_EXPRESSED_FLAG,ORIG_CODE,RXN_BOSS_FROM,RXN_BOSS_STRENGTH_DENOM_UNIT,RXN_BOSS_STRENGTH_NUM_UNIT,RXN_STRENGTH,RXN_QUALITATIVE_DISTINCTION,RXTERM_FORM,RXN_VET_DRUG,RXN_BOSS_STRENGTH_DENOM_VALUE,RXN_AVAILABLE_STRENGTH,RXN_BOSS_AI,RXN_HUMAN_DRUG,RXN_QUANTITY,AMBIGUITY_FLAG,RXN_OBSOLETED,RXN_BOSS_STRENGTH_NUM_VALUE,RXN_ACTIVATED,RXN_BOSS_AM,RXN_BN_CARDINALITY,ORIG_SOURCE | ENG  | UTF-8 | Y      | Y     | RxNorm work done by the National Library of Medicine | ;;;;RxNorm;;;META2020AA Full Update 2020_09_08;Bethesda, MD;National Library of Medicine;;;;;;;; |
# +----------+----------+---------------------+--------+-------------------+--------+--------------+--------+------+--------+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+------+------+------+-------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+-------+--------+-------+------------------------------------------------------+--------------------------------------------------------------------------------------------------+
# 1 row in set (0.00 sec)
RXNSAB_LAT = 19

UMLS_LANGCODE_MAP = {"eng" : "en", "fre" : "fr", "cze" : "cz", "fin" : "fi", "ger" : "de", "ita" : "it", "jpn" : "jp", "pol" : "pl", "por" : "pt", "rus" : "ru", "spa" : "es", "swe" : "sw", "scr" : "hr", "dut" : "nl", "lav" : "lv", "hun" : "hu", "kor" : "kr", "dan" : "da", "nor" : "no", "heb" : "he", "baq" : "eu"}

def get_umls_url(code):
    return "%s%s/"%(conf.UMLS_BASE_URI,code)

def flatten(matrix):
    return reduce(lambda x,y: x+y,matrix)

def escape(string):
    return string.replace("\\","\\\\").replace('"','\\"')

def get_url_term(ns,code):
    if ns[-1] == '/':
        ret = ns + urllib.quote(code)
    else:
        ret = "%s/%s"%(ns,urllib.quote(code))
    return ret

def get_rel_fragment(rel):
    return rel[RXNREL_RELA] if rel[RXNREL_RELA] else rel[RXNREL_REL]

def get_rel_code_source(rel):
    return rel[RXNREL_RXCUI2]
def get_rel_code_target(rel):
    return rel[RXNREL_RXCUI1]

def get_code(reg):
    if not reg[RXNCONSO_RXCUI]:
        raise AttributeError("Could not retrieve code because RXCUI does not exist [%s]" % ("|".join(reg)))
    return reg[RXNCONSO_RXCUI]

def __get_connection():
    return pymysql.connect(conf.DB_HOST, conf.DB_USER, conf.DB_PASS, conf.DB_NAME_RXNORM)

def generate_semantic_types(con,with_roots=False):
    url = get_umls_url("STY")
    hierarchy = collections.defaultdict(lambda : list())
    all_nodes = list()
    mrsty = RxnTable("RXNSTY", con,
                     load_select="SELECT DISTINCT TUI, STN, STY FROM RXNSTY")
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


class RxnTable(object):
    def __init__(self,table_name,conn,load_select=None):
        self.table_name = table_name
        self.conn = conn
        self.page_size = 500000
        self.load_select = load_select

    # mysql> select * from rxnorm.RXNREL r where r.sab = 'MSH' and r.rel = 'CHD';
    #+----------+
    # | count(*) |
    # +----------+
    # |      392 |
    # +----------+
    # 1 row in set (1 min 16.94 sec)
    def mesh_tree(self):
        q = """select DISTINCT c1.rxcui as parent, c2.rxcui as child
from RXNREL r, RXNCONSO c1, RXNCONSO c2 where r.sab = 'MSH' and r.rel = 'CHD'
and c1.rxcui = r.rxcui1
and c2.rxcui = r.rxcui2
and c2.rxcui like 'D%'
and c1.rxcui like 'D%'
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
        #c = self.count()
        i = 0
        page = 0
        cont = True
        cursor = self.conn.cursor()
        while cont:
            if self.load_select:
                q = self.load_select
            else:
                q = "SELECT * FROM %s WHERE %s LIMIT %s OFFSET %s"%(self.table_name,filt,self.page_size,page * self.page_size)
                if filt == None or len(filt) == 0:
                    q = "SELECT * FROM %s LIMIT %s OFFSET %s"%(self.table_name,self.page_size,page * self.page_size)
            sys.stdout.write("[UMLS-Query] %s\n" % q)
            sys.stdout.flush()
            cursor.execute(q)
            result = cursor.fetchall()
            cont = False
            for record in result:
                cont = True
                i += 1
                yield record
                if limit and i >= limit:
                    cont = False
                    break
            # Do we already have all the rows available for the query?
            if self.load_select:
                cont = False
            elif not limit and i < self.page_size:
                cont = False
            page += 1
        cursor.close()



class RxnClass(object):
    """
    RxnClass encapsulates all data from RXCUI, RXNREL, RXNSAT, etc for one code - RXCUI.
    """
    def __init__(self,ns,atoms=None,rels=None,
                 defs=None,atts=None,rank=None,
                 rank_by_tty=None,sty=None,
                 sty_by_cui=None,load_on_cuis=False,
                 is_root=None):
        self.ns = ns
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
        codes = set([get_code(x) for x in self.atoms])
        if len(codes) != 1:
            raise AttributeError("Only one code per term.")
        #if DEBUG:
            #sys.stderr.write(self.atoms)
            #sys.stderr.write(codes)
        return codes.pop()

    def getAltLabels(self,prefLabel):
        #is_pref_atoms =  filter(lambda x: x[MRCONSO_ISPREF] == 'Y', self.atoms)
        return set([atom[RXNCONSO_STR] for atom in self.atoms if atom[RXNCONSO_STR] != prefLabel])

    def getPrefLabel(self):
        # RXNCONSO does NOT provide ISPREF therefore we look into TTY rank.
        if len(self.rank) > 0:
            sort_key = \
            lambda x: int(self.rank[self.rank_by_tty[x[RXNCONSO_TTY]][0]][RXN_TTY_RANK])  # this will get 358 for 'BN'
            rank_sorted_atoms = sorted(self.atoms,key=sort_key,reverse=True)
            return rank_sorted_atoms[0][RXNCONSO_STR]

    def getURLTerm(self,code):
        return get_url_term(self.ns,code)

    def properties(self):
        return self.class_properties

    def toRDF(self,fmt="Turtle",hierarchy=True,lang="en",tree=None):
        if not fmt == "Turtle":
            raise AttributeError("Only fmt='Turtle' is currently supported")
        term_code = self.code()
        url_term = self.getURLTerm(term_code)
        prefLabel = self.getPrefLabel()
        altLabels = self.getAltLabels(prefLabel)
        rdf_term = """<%s> a owl:Class ;
\tskos:prefLabel \"\"\"%s\"\"\"@%s ;
\tskos:notation \"\"\"%s\"\"\"^^xsd:string ;
"""%(url_term,escape(prefLabel),lang,escape(term_code))

        if len(altLabels) > 0:
            rdf_term += """\tskos:altLabel %s ;
"""%(" , ".join(map(lambda x: '\"\"\"%s\"\"\"@%s'%(escape(x),lang),
                                                            set(altLabels))))

        if self.is_root:
            rdf_term += '\trdfs:subClassOf owl:Thing ;\n'

        count_parents = 0
        if tree:
            if term_code in tree:
                for parent in tree[term_code]:
                    o = self.getURLTerm(parent)
                    rdf_term += "\trdfs:subClassOf <%s> ;\n" % (o,)
        for rel in self.rels:
            source_code = get_rel_code_source(rel)
            target_code = get_rel_code_target(rel)
            if source_code != term_code:
                raise AttributeError("Inconsistent code in rel")
            # Map child relations to rdf:subClassOf (skip parent relations).
            if rel[RXNREL_REL] == 'PAR':
                continue
            if rel[RXNREL_REL] == 'CHD' and hierarchy:
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
                    rdf_term += "\trdfs:subClassOf <%s> ;\n" % (o,)
            else:
                p = self.getURLTerm(get_rel_fragment(rel))
                o = self.getURLTerm(target_code)
                rdf_term += "\t<%s> <%s> ;\n" % (p,o)
                if p not in self.class_properties:
                    self.class_properties[p] = \
                        RxnAttribute(p, get_rel_fragment(rel))

        for att in self.atts:
            atn = att[RXNSAT_ATN]
            atv = att[RXNSAT_ATV]
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
                    rdf_term += "\trdfs:subClassOf owl:Thing;\n"
            p = self.getURLTerm(atn)
            rdf_term += "\t<%s> \"\"\"%s\"\"\"^^xsd:string ;\n"%(p, escape(atv))
            if p not in self.class_properties:
                self.class_properties[p] = RxnAttribute(p, atn)

        #auis = set([x[MRCONSO_AUI] for x in self.atoms])
        cuis = set([x[RXNCONSO_RXCUI] for x in self.atoms])
        sty_recs = flatten([indexes for indexes in [self.sty_by_cui[cui] for cui in cuis]])
        types = [self.sty[index][RXNSTY_TUI] for index in sty_recs]

        #for t in auis:
        #    rdf_term += """\t%s \"\"\"%s\"\"\"^^xsd:string ;\n"""%(HAS_AUI,t)
        for t in cuis:
            rdf_term += """\t%s \"\"\"%s\"\"\"^^xsd:string ;\n"""%(HAS_CUI,t)
        for t in set(types):
            rdf_term += """\t%s \"\"\"%s\"\"\"^^xsd:string ;\n"""%(HAS_TUI,t)
        for t in set(types):
            rdf_term += """\t%s <%s> ;\n"""%(HAS_STY,get_umls_url("STY")+t)

        return rdf_term + " .\n\n"



class RxnAttribute(object):
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



class RxnOntology(object):
    def __init__(self,ont_code,ns,con,load_on_cuis=False):
        self.loaded = False
        self.ont_code = ont_code
        self.ns = ns
        self.con = con
        self.load_on_cuis = load_on_cuis
        #self.alt_uri_code = alt_uri_code
        self.atoms = list()
        self.atoms_by_rxcui = collections.defaultdict(lambda : list())
        self.atoms_by_rxaui = collections.defaultdict(lambda : list())
        self.rels = list()
        self.rels_by_rxcui_src = collections.defaultdict(lambda : list())
        self.atts = list()
        self.atts_by_rxcui = collections.defaultdict(lambda : list())
        self.rank = rank
        self.rank_by_tty = rank_by_tty
        self.sty = list()
        self.sty_by_rxcui = collections.defaultdict(lambda : list())
        self.cui_roots = set()
        self.lang = None
        self.ont_properties = dict()

    def load_tables(self,limit=None):
        rxnconso = RxnTable("RXNCONSO", self.con)

        rxnsab  = RxnTable("RXNSAB", self.con)
        for sab_rec in rxnsab.scan(filt="RSAB = '" + self.ont_code + "'", limit=1):
            self.lang = sab_rec[RXNSAB_LAT].lower()
        # rxnconso_filt = "SAB = '%s' AND lat = '%s' AND SUPPRESS = 'N'" % (self.ont_code, self.lang)
        rxnconso_filt = "SAB = '%s' AND lat = '%s' AND SUPPRESS = 'N'"%(self.ont_code,self.lang)

        for atom in rxnconso.scan(filt=rxnconso_filt,limit=limit):
            index = len(self.atoms)
            self.atoms_by_rxcui[atom[RXNCONSO_RXCUI]].append(index)
            self.atoms_by_rxaui[atom[RXNCONSO_RXAUI]].append(index)
            self.atoms.append(atom)
        if DEBUG:
            sys.stderr.write("length atoms: %d\n" % len(self.atoms))
            sys.stderr.write("length atoms_by_rxaui: %d\n" % len(self.atoms_by_rxaui))
            sys.stderr.write("atom example: %s\n\n" % str(self.atoms))
            sys.stderr.flush()

        rxnrel = RxnTable("RXNREL", self.con)
        # rxnrel_filt = "SAB = '%s' AND SUPPRESS = 'N')"%self.ont_code
        # NOTE RXNREL does NOT populate SUPPRESS field (yet) according to Technical Doc therefore remove that condition
        # mysql> select count(*) from RXNREL where not SUPPRESS='' and not SUPPRESS is null;
        # +----------+
        # | count(*) |
        # +----------+
        # | 0 |
        # +----------+
        # 1 row in set (1 min 13.90 sec)
        rxnrel_filt = "SAB = '%s'"%self.ont_code
        for rel in rxnrel.scan(filt=rxnrel_filt,limit=limit):
            index = len(self.rels)
            self.rels_by_rxcui_src[rel[RXNREL_RXCUI2]].append(index)
            self.rels.append(rel)
        if DEBUG:
            sys.stderr.write("length rels: %d\n\n" % len(self.rels))
            sys.stderr.flush()

        rxnsat = RxnTable("RXNSAT", self.con)
        rxnsat_filt = "SAB = '%s'"%self.ont_code
        for att in rxnsat.scan(filt=rxnsat_filt):
            index = len(self.atts)
            self.atts_by_rxcui[att[RXNSAT_RXCUI]].append(index)
            self.atts.append(att)
        if DEBUG:
            sys.stderr.write("length atts: %d\n\n" % len(self.atts))
            sys.stderr.flush()

        load_rxnsty = "SELECT sty.* FROM RXNSTY sty, RXNCONSO conso \
        WHERE conso.SAB = '%s' AND conso.rxcui = sty.rxcui AND conso.suppress = 'N'"
        load_rxnsty %= self.ont_code
        rxnsty = RxnTable("RXNSTY", self.con, load_select=load_rxnsty)
        for sty in rxnsty.scan(filt=None):
            index = len(self.sty)
            self.sty_by_rxcui[sty[RXNSTY_RXCUI]].append(index)
            self.sty.append(sty)
        if DEBUG:
            sys.stderr.write("length sty: %d\n\n" % len(self.sty))
            sys.stderr.flush()

        # Track the loaded status
        self.loaded = True
        sys.stdout.write("%s tables loaded ...\n" % self.ont_code)
        sys.stdout.flush()

    def terms(self):
        if not self.loaded:
            self.load_tables()

        for rxcui in self.atoms_by_rxcui:
            rxcui_atoms = [self.atoms[row] for row in self.atoms_by_rxcui[rxcui]]

            rels_to_class = [self.rels[index] for index in self.rels_by_rxcui_src[rxcui]]

            # Ps: is_root is always False for sab='RXNORM'
            # mysql> select count(*) from rxnorm.RXNREL where REL = 'CHD' and sab='RXNORM';
            # +----------+
            # | count(*) |
            # +----------+
            # | 0 |
            # +----------+
            # 1 row in set(9.01 sec)
            is_root = False

            atts = [self.atts[x] for x in self.atts_by_rxcui[rxcui]]

            umls_class = RxnClass(self.ns, atoms=rxcui_atoms, rels=rels_to_class,
                                  defs=None, atts=atts, rank=self.rank, rank_by_tty=self.rank_by_tty,
                                  sty=self.sty, sty_by_cui=self.sty_by_rxcui,
                                  load_on_cuis=self.load_on_cuis, is_root=is_root)

            yield umls_class

    def write_into(self,file_path,hierarchy=True):
        sys.stdout.write("%s writing terms ... %s\n" % (self.ont_code, file_path))
        sys.stdout.flush()
        fout = codecs.open(file_path,"w","utf-8")
        #nterms = len(self.atoms_by_rxcui)
        fout.write(PREFIXES)
        comment = "RDF Version of the RXNORM ontology %s; " +\
                  "converted with the RXNORM2RDF tool forked from UMLS2RDF " +\
                  "(https://github.com/katyberg/umls2rdf), "+\
                  "developed by the NCBO project."
        header_values = dict(
           label = self.ont_code,
           comment = comment % self.ont_code,
           versioninfo = conf.RXNORM_VERSION,
           uri = self.ns
        )
        fout.write(ONTOLOGY_HEADER.substitute(header_values))
        for term in self.terms():
            try:
                rdf_text = term.toRDF(lang=UMLS_LANGCODE_MAP[self.lang])
                fout.write(rdf_text)
            except Exception as e:
                print("ERROR dumping term!")
                print(e)

            for att in term.properties():
                if att not in self.ont_properties:
                    self.ont_properties[att] = term.properties()[att]

        return fout

    def properties(self):
        return self.ont_properties

    def write_properties(self,fout,property_docs):
        self.ont_properties["hasSTY"] =\
                RxnAttribute(HAS_STY, "hasSTY")
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



if __name__ == "__main__":

    con = __get_connection()

    umls_conf = None
    with open("umls.conf","r") as fconf:
        umls_conf = [line.split(",") \
                        for line in fconf.read().splitlines() \
                            if len(line) > 0]
        umls_conf = filter(lambda x: not x[0].startswith("#"), umls_conf)
        fconf.close()

    if not os.path.isdir(conf.OUTPUT_FOLDER):
        raise Exception("Output folder '%s' not found."%conf.OUTPUT_FOLDER)

    sem_types = generate_semantic_types(con,with_roots=True)
    output_file = os.path.join(conf.OUTPUT_FOLDER,"umls_semantictypes.ttl")
    with codecs.open(output_file,"w","utf-8") as semfile:
        semfile.write(PREFIXES)
        semfile.write(sem_types)
        semfile.flush()
        semfile.close()

    sem_types = generate_semantic_types(con,with_roots=False)
    rxndoc = RxnTable("RXNDOC", con)
    property_docs = dict()
    for doc_record in rxndoc.scan():
        _type = doc_record[RXNDOC_TYPE]
        _expl = doc_record[RXNDOC_DESC]
        _key = doc_record[RXNDOC_VALUE]
        if _key not in property_docs:
            property_docs[_key] = dict()
            property_docs[_key]["dockey"] = doc_record[RXNDOC_DOCKEY]
        if "inverse" in _type:
            _type = "inverse"
        property_docs[_key][_type] = _expl

    for (umls_code, file_out, load_on_field) in umls_conf:
        alt_uri_code = None
        if ";" in umls_code:
            umls_code,alt_uri_code = umls_code.split(";")
        if umls_code.startswith("#"):
            continue
        load_on_cuis = load_on_field == "load_on_cuis"
        output_file = os.path.join(conf.OUTPUT_FOLDER,file_out)
        sys.stdout.write("Generating %s (using '%s')\n" %
                (umls_code,load_on_field))
        sys.stdout.flush()
        ns = get_umls_url(umls_code if not alt_uri_code else alt_uri_code)
        ont = RxnOntology(umls_code, ns, con, load_on_cuis=load_on_cuis)
        ont.load_tables()
        fout = ont.write_into(output_file,hierarchy=(ont.ont_code != "MSH"))
        ont.write_properties(fout,property_docs)
        if conf.INCLUDE_SEMANTIC_TYPES:
          ont.write_semantic_types(sem_types,fout)
        fout.close()
        sys.stdout.write("done!\n\n")
        sys.stdout.flush()
    sys.stdout.flush()
