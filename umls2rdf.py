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
    raise AttributeError, "No code on reg [%s]"%("|".join(reg))

def __get_connection():
    return MySQLdb.connect(host=conf.DB_HOST,user=conf.DB_USER,
              passwd=conf.DB_PASS,db=conf.DB_NAME_UMLS,charset='utf8',use_unicode=True)

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
        self.page_size = 500000
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



class UmlsClass(object):
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
        codes = set([get_code(x,self.load_on_cuis) for x in self.atoms])
        if len(codes) <> 1:
            raise AttributeError, "Only one code per term."
        #if DEBUG:
            #sys.stderr.write(self.atoms)
            #sys.stderr.write(codes)
        return codes.pop()

    def getAltLabels(self,prefLabel):
        #is_pref_atoms =  filter(lambda x: x[MRCONSO_ISPREF] == 'Y', self.atoms)
        return set([atom[MRCONSO_STR] for atom in self.atoms if atom[MRCONSO_STR] <> prefLabel])

    def getPrefLabel(self):
        if self.load_on_cuis:
            if len(self.atoms) == 1:
                return self.atoms[0][MRCONSO_STR]

            labels = set([x[MRCONSO_STR] for x in self.atoms])
            if len(labels) == 1:
                return labels.pop()

            is_pref_atoms =  filter(lambda x: x[MRCONSO_ISPREF] == 'Y', self.atoms)
            if len(is_pref_atoms) == 0:
                return self.atoms[0][MRCONSO_STR]
            elif len(is_pref_atoms) == 1:
                return is_pref_atoms[0][MRCONSO_STR]

            is_pref_atoms =  filter(lambda x: x[MRCONSO_STT] == 'PF', is_pref_atoms)
            if len(is_pref_atoms) == 0:
                return self.atoms[0][MRCONSO_STR]
            elif len(is_pref_atoms) == 1:
                return is_pref_atoms[0][MRCONSO_STR]

            is_pref_atoms =  filter(lambda x: x[MRCONSO_TTY][0] == 'P', self.atoms)
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
                pref_atom = filter(lambda x: 'P' in x[MRCONSO_TTY], self.atoms)
                if len(pref_atom) == 1:
                    return pref_atom[0][MRCONSO_STR]
            raise AttributeError, "Unable to select pref label"

    def getURLTerm(self,code):
        return get_url_term(self.ns,code)

    def properties(self):
        return self.class_properties

    def toRDF(self,fmt="Turtle",hierarchy=True,lang="en",tree=None):
        if not fmt == "Turtle":
            raise AttributeError, "Only fmt='Turtle' is currently supported"
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

        if len(self.defs) > 0:
            rdf_term += """\tskos:definition %s ;
"""%(" , ".join(map(lambda x: '\"\"\"%s\"\"\"@%s'%(escape(x[MRDEF_DEF]),lang),
                                                                set(self.defs))))

        count_parents = 0
        if tree:
            if term_code in tree:
                for parent in tree[term_code]:
                    o = self.getURLTerm(parent)
                    rdf_term += "\trdfs:subClassOf <%s> ;\n" % (o,)
        for rel in self.rels:
            source_code = get_rel_code_source(rel,self.load_on_cuis)
            target_code = get_rel_code_target(rel,self.load_on_cuis)
            if source_code <> term_code:
                raise AttributeError, "Inconsistent code in rel"
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
                    rdf_term += "\trdfs:subClassOf <%s> ;\n" % (o,)
            else:
                p = self.getURLTerm(get_rel_fragment(rel))
                o = self.getURLTerm(target_code)
                rdf_term += "\t<%s> <%s> ;\n" % (p,o)
                if p not in self.class_properties:
                    self.class_properties[p] = \
                        UmlsAttribute(p,get_rel_fragment(rel))

        for att in self.atts:
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
                    rdf_term += "\trdfs:subClassOf owl:Thing;\n"
            p = self.getURLTerm(atn)
            rdf_term += "\t<%s> \"\"\"%s\"\"\"^^xsd:string ;\n"%(p, escape(atv))
            if p not in self.class_properties:
                self.class_properties[p] = UmlsAttribute(p,atn)

        #auis = set([x[MRCONSO_AUI] for x in self.atoms])
        cuis = set([x[MRCONSO_CUI] for x in self.atoms])
        sty_recs = flatten([indexes for indexes in [self.sty_by_cui[cui] for cui in cuis]])
        types = [self.sty[index][MRSTY_TUI] for index in sty_recs]

        #for t in auis:
        #    rdf_term += """\t%s \"\"\"%s\"\"\"^^xsd:string ;\n"""%(HAS_AUI,t)
        for t in cuis:
            rdf_term += """\t%s \"\"\"%s\"\"\"^^xsd:string ;\n"""%(HAS_CUI,t)
        for t in set(types):
            rdf_term += """\t%s \"\"\"%s\"\"\"^^xsd:string ;\n"""%(HAS_TUI,t)
        for t in set(types):
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
            raise AttributeError, "Only fmt='Turtle' is currently supported"
        _type = ""
        if "REL" in dockey:
            _type = "ObjectProperty"
        elif dockey == "ATN":
            _type = "DatatypeProperty"
        else:
            raise AttributeError, ("Unknown DOCKEY" + dockey)

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
    def __init__(self,ont_code,ns,con,load_on_cuis=False):
        self.loaded = False
        self.ont_code = ont_code
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

    def load_tables(self,limit=None):
        mrconso = UmlsTable("MRCONSO",self.con)
        if self.ont_code == 'MSH':
            self.tree = mrconso.mesh_tree()
        else:
            self.tree = None
        mrsab  = UmlsTable("MRSAB", self.con)
        for sab_rec in mrsab.scan(filt="RSAB = '" + self.ont_code + "'", limit=1):
            self.lang = sab_rec[MRSAB_LAT].lower()
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
        mrsat_filt = "SAB = '%s' AND CODE IS NOT NULL"%self.ont_code
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
        WHERE conso.SAB = '%s' AND conso.cui = sty.cui AND conso.suppress = 'N'"
        load_mrsty %= self.ont_code
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

    def terms(self):
        if not self.loaded:
            self.load_tables()
        # Note: most UMLS ontologies are 'load_on_codes' (only HL7 is load_on_cuis)
        for code in self.atoms_by_code:
            code_atoms = [self.atoms[row] for row in self.atoms_by_code[code]]
            field = MRCONSO_CUI if self.load_on_cuis else MRCONSO_AUI
            ids = map(lambda x: x[field], code_atoms)
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

                    if len(code_source) <> 1 or len(code_target) > 1:
                        raise AttributeError, "more than one or none codes"
                    if len(code_source) == 1 and len(code_target) == 1 and \
                        code_source[0] <> code_target[0]:
                        code_source = code_source[0]
                        code_target = code_target[0]
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
                load_on_cuis=self.load_on_cuis,is_root=is_root)

            # TODO: patch to fix roots in SNOMED-CT
            # suppress should fix this one
            #if self.ont_code == "SNOMEDCT_US":
            #    umls_class.is_root = (umls_class.code() == "138875005")

            yield umls_class

    def write_into(self,file_path,hierarchy=True):
        sys.stdout.write("%s writing terms ... %s\n" % (self.ont_code, file_path))
        sys.stdout.flush()
        fout = codecs.open(file_path,"w","utf-8")
        #nterms = len(self.atoms_by_code)
        fout.write(PREFIXES)
        comment = "RDF Version of the UMLS ontology %s; " +\
                  "converted with the UMLS2RDF tool " +\
                  "(https://github.com/ncbo/umls2rdf), "+\
                  "developed by the NCBO project."
        header_values = dict(
           label = self.ont_code,
           comment = comment % self.ont_code,
           versioninfo = conf.UMLS_VERSION,
           uri = self.ns
        )
        fout.write(ONTOLOGY_HEADER.substitute(header_values))
        for term in self.terms():
            try:
                rdf_text = term.toRDF(lang=UMLS_LANGCODE_MAP[self.lang],tree=self.tree)
                fout.write(rdf_text)
            except Exception, e:
                print "ERROR dumping term ", e

            for att in term.properties():
                if att not in self.ont_properties:
                    self.ont_properties[att] = term.properties()[att]

        return fout

    def properties(self):
        return self.ont_properties

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
                raise AttributeError, "expanded form not found in " + doc
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
    mrdoc = UmlsTable("MRDOC", con)
    property_docs = dict()
    for doc_record in mrdoc.scan():
        _type = doc_record[MRDOC_TYPE]
        _expl = doc_record[MRDOC_DESC]
        _key = doc_record[MRDOC_VALUE]
        if _key not in property_docs:
            property_docs[_key] = dict()
            property_docs[_key]["dockey"] = doc_record[MRDOC_DOCKEY]
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
        ont = UmlsOntology(umls_code,ns,con,load_on_cuis=load_on_cuis)
        ont.load_tables()
        fout = ont.write_into(output_file,hierarchy=(ont.ont_code <> "MSH"))
        ont.write_properties(fout,property_docs)
        if conf.INCLUDE_SEMANTIC_TYPES:
          ont.write_semantic_types(sem_types,fout)
        fout.close()
        sys.stdout.write("done!\n\n")
        sys.stdout.flush()
    sys.stdout.flush()

