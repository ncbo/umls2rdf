#! /usr/bin/python

import sys
import os
import urllib
from string import Template
import collections
import MySQLdb
#import pdb
#from itertools import groupby

try:
    import conf
except:
    print "Copy and modify conf_sample.py into conf.py"
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

MRDOC_VALUE = 1
MRDOC_TYPE = 2
MRDOC_DESC = 3

MRRANK_TTY = 2
MRRANK_RANK = 0

MRSTY_CUI = 0
MRSTY_TUI = 1

def get_umls_url(code):
    return "http://purl.bioontology.org/ontology/%s/"%code

def flatten(matrix):
    return reduce(lambda x,y: x+y,matrix)

def escape(string):
    return string.replace("\\","\\\\").replace('"','\\"')

def get_url_term(ns,code):
    if ns[-1] == '/':
        ret = ns + urllib.quote(code)
    else:
        ret = "%s/%s"%(ns,urllib.quote(code))
    return ret.replace("%20","+") 

def get_rel_fragment(rel):
    return rel[MRREL_REL] if not rel[MRREL_RELA] else rel[MRREL_RELA]

def get_rel_code_source(rel,on_cuis):
    return rel[-1] if not on_cuis else rel[MRREL_CUI2]

def get_rel_code_target(rel,on_cuis):
    return rel[-2] if not on_cuis else rel[MRREL_CUI1]

def get_code(reg,load_on_cuis):
    if not load_on_cuis:
        if reg[MRCONSO_CODE]:
            return reg[MRCONSO_CODE]
        raise AttributeError, "No code on reg [%s]"%("|".join(reg))
    else:
        return reg[MRCONSO_CUI]

def __get_connection():
    return MySQLdb.connect(host=conf.DB_HOST,user=conf.DB_USER,
              passwd=conf.DB_PASS,db=conf.DB_NAME)

def generate_semantic_types(con,url,fileout):
    hierarchy = collections.defaultdict(lambda : list()) 
    all_nodes = list()
    mrsty = UmlsTable("MRSTY",con,load_select="SELECT DISTINCT TUI, STN, STY FROM MRSTY")
    ont = list()
    ont.append(PREFIXES)

    for stt in mrsty.scan():
        hierarchy[stt[1]].append(stt[0])
        sty_term = """<%s> a owl:Class;
skos:notation "%s"^^xsd:string;
skos:prefLabel "%s"@en .
"""%(url+stt[0],stt[0],stt[2])
        ont.append(sty_term)
        all_nodes.append(stt)
    
    for node in all_nodes:
        parent = ".".join(node[1].split(".")[0:-1])
        rdfs_subclasses = ["<%s> rdfs:subClassOf <%s> ."%(url+node[0],url+x) for x in hierarchy[parent]]
        for sc in rdfs_subclasses:
            ont.append(sc)
    data_ont_ttl = "\n".join(ont)
    with open(fileout,"w") as fout:
        fout.write(data_ont_ttl)
        fout.write("\n")
        fout.close()


class UmlsTable(object):
    def __init__(self,table_name,conn,load_select=None):
        self.table_name = table_name
        self.conn = conn
        self.page_size = 50000
        self.load_select = load_select

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
            print("[UMLS-Query] %s"%q)
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
            if self.load_select:
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

    def code(self):
        codes = set([get_code(x,self.load_on_cuis) for x in self.atoms])
        if len(codes) <> 1:
            raise AttributeError, "Only one code per term."
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

            #if there's only one ISPREF=Y then that one.
            is_pref_atoms =  filter(lambda x: x[MRCONSO_ISPREF] == 'Y', self.atoms)
            if len(is_pref_atoms) == 1:
                return is_pref_atoms[0][MRCONSO_STR]
            elif len(is_pref_atoms) > 1:
                is_pref_atoms =  filter(lambda x: x[MRCONSO_STT] == 'PF', is_pref_atoms)
                if len(is_pref_atoms) > 0:
                    return is_pref_atoms[0][MRCONSO_STR]
            is_pref_atoms =  filter(lambda x: x[MRCONSO_STT] == 'PF', self.atoms)
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
    
    def toRDF(self,fmt="Turtle",hierarchy=True):
        term_code = self.code()
        url_term = self.getURLTerm(term_code)

        if not fmt == "Turtle":
            raise AttributeError, "Only fmt='Turtle' is currently supported"

        prefLabel = self.getPrefLabel()
        altLabels = self.getAltLabels(prefLabel)

        rdf_term = """<%s> a owl:Class;
\tskos:prefLabel \"\"\"%s\"\"\"@en;
\tskos:notation \"\"\"%s\"\"\"^^xsd:string;
"""%(url_term,escape(prefLabel),escape(term_code))
        if len(altLabels) > 0:
            rdf_term += """\tskos:altLabel %s;
"""%(", ".join(map(lambda x: '\"\"\"%s\"\"\"@en'%escape(x),set(altLabels))))
        if self.is_root: 
            rdf_term += '\tumls:isRoot "true"^^xsd:boolean;\n'

        if len(self.defs) > 0:
            rdf_term += """\tskos:definition %s;
"""%(", ".join(map(lambda x: '\"\"\"%s\"\"\"@en'%escape(x[MRDEF_DEF]),set(self.defs))))

        for rel in self.rels:
            source_code = get_rel_code_source(rel,self.load_on_cuis)
            target_code = get_rel_code_target(rel,self.load_on_cuis)
            if source_code <> term_code:
                raise AttributeError, "Inconsistent code in rel"
            if rel[MRREL_REL] == 'CHD' and hierarchy:
                rdf_term += """ rdfs:subClassOf <%s>;
"""%(self.getURLTerm(target_code))
            elif  rel[MRREL_REL] == 'PAR':
                continue
            else:
                rdf_term += """ <%s> <%s>;
"""%(self.getURLTerm(get_rel_fragment(rel)),self.getURLTerm(target_code))
        for att in self.atts:
            rdf_term += """ <%s> \"\"\"%s\"\"\"^^xsd:string;
"""%(self.getURLTerm(att[MRSAT_ATN]),escape(att[MRSAT_ATV]))
           

        cuis = set([x[MRCONSO_CUI] for x in self.atoms])
        sty_recs = flatten([indexes for indexes in [self.sty_by_cui[cui] for cui in cuis]])
        types = [self.sty[index][MRSTY_TUI] for index in sty_recs]

        for t in set(cuis):
            rdf_term += """ %s \"\"\"%s\"\"\"^^xsd:string;
"""%(HAS_CUI,t)
        for t in set(types):
            rdf_term += """ %s \"\"\"%s\"\"\"^^xsd:string;
"""%(HAS_TUI,t)
        for t in set(types):
            rdf_term += """ %s <%s>;
"""%(HAS_STY,STY_URL+t)

        return rdf_term + " .\n\n"
        
class UmlsAttribute(object):
    def __init__(self,ns,att):
        self.ns = ns
        self.att = att

    def getURLTerm(self,code):
        return get_url_term(self.ns,code)

    def toRDF(self,fmt="Turtle"):
        if not fmt == "Turtle":
            raise AttributeError, "Only fmt='Turtle' is currently supported"
        return """<%s> a owl:DatatypeProperty;
rdfs:label \"\"\"%s\"\"\";
rdfs:comment \"\"\"%s\"\"\" .
\n"""%(self.getURLTerm(self.att[MRDOC_VALUE]),escape(self.att[MRDOC_VALUE]),escape(self.att[MRDOC_DESC]))
        

class UmlsOntology(object):
    def __init__(self,ont_code,ns,con,load_on_cuis=False):
        self.ont_code = ont_code
        self.ns = ns
        self.load_on_cuis = load_on_cuis
        self.alt_uri_code = alt_uri_code
        
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

        self.con = con

    def load_tables(self,limit=None):
        mrconso = UmlsTable("MRCONSO",self.con)
        mrconso_filt = "SAB = '%s' AND lat = 'ENG'"%self.ont_code
        for atom in mrconso.scan(filt=mrconso_filt,limit=limit):
            index = len(self.atoms)
            self.atoms_by_code[get_code(atom,self.load_on_cuis)].append(index)
            if not self.load_on_cuis:
                self.atoms_by_aui[atom[MRCONSO_AUI]].append(index)
            self.atoms.append(atom)
        
        mrconso_filt = "SAB = 'SRC' AND CODE = 'V-%s'"%self.ont_code
        for atom in mrconso.scan(filt=mrconso_filt,limit=limit):
            self.cui_roots.add(atom[MRCONSO_CUI])

        mrrel = UmlsTable("MRREL",self.con)
        mrrel_filt = "SAB = '%s'"%self.ont_code
        field = MRREL_AUI2 if not self.load_on_cuis else MRREL_CUI2
        for rel in mrrel.scan(filt=mrrel_filt,limit=limit):
            index = len(self.rels)
            self.rels_by_aui_src[rel[field]].append(index)
            self.rels.append(rel)
        
        mrdef = UmlsTable("MRDEF",self.con)
        mrdef_filt = "SAB = '%s'"%self.ont_code
        field = MRDEF_AUI if not self.load_on_cuis else MRDEF_CUI
        for defi in mrdef.scan(filt=mrdef_filt):
            index = len(self.defs)
            self.defs_by_aui[defi[field]].append(index)
            self.defs.append(defi)
        
        mrsat = UmlsTable("MRSAT",self.con)
        mrsat_filt = "SAB = '%s' AND CODE IS NOT NULL"%self.ont_code 
        field = MRSAT_CODE if not self.load_on_cuis else MRSAT_CUI
        for att in mrsat.scan(filt=mrsat_filt):
            index = len(self.atts)
            self.atts_by_code[att[field]].append(index)
            self.atts.append(att)

        mrrank = UmlsTable("MRRANK",self.con)
        mrrank_filt = "SAB = '%s'"%self.ont_code 
        for rank in mrrank.scan(filt=mrrank_filt):
            index = len(self.rank)
            self.rank_by_tty[rank[MRRANK_TTY]].append(index)
            self.rank.append(rank)

        load_mrsty = "SELECT sty.* FROM MRSTY sty, MRCONSO conso \
        WHERE conso.SAB = '%s' AND conso.cui = sty.cui"
        load_mrsty %= self.ont_code
        mrsty = UmlsTable("MRSTY",self.con,load_select=load_mrsty)
        for sty in mrsty.scan(filt=None):
            index = len(self.sty)
            self.sty_by_cui[sty[MRSTY_CUI]].append(index)
            self.sty.append(sty)

    def terms(self):
        for code in self.atoms_by_code:
            code_atoms = [self.atoms[row] for row in self.atoms_by_code[code]] 
            field = MRCONSO_AUI if not self.load_on_cuis else MRCONSO_CUI
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
                    if rel[MRREL_CUI1] in self.cui_roots:
                        is_root = True
                    if len(code_source) <> 1 or len(code_target) > 1:
                        raise AttributeError, "more than one or none codes"
                    if len(code_source) == 1 and len(code_target) == 1 and \
                        code_source[0] <> code_target[0]:
                        code_source = code_source[0]
                        code_target = code_target[0]
                        rel_with_codes.append(code_target)
                        rel_with_codes.append(code_source)
                        rels_to_class.append(rel_with_codes)
            defs = [self.defs[x] for x in self.defs_by_aui[_id] for _id in ids]
            atts = [self.atts[x] for x in self.atts_by_code[code]]

            yield UmlsClass(self.ns,atoms=code_atoms,rels=rels_to_class,
                defs=defs,atts=atts,rank=self.rank,rank_by_tty=self.rank_by_tty,
                sty=self.sty, sty_by_cui=self.sty_by_cui,
                load_on_cuis=self.load_on_cuis,is_root=is_root)

    def write_into(self,file_path,hierarchy=True):
        fout = file(file_path,"w")
        #nterms = len(self.atoms_by_code)
        fout.write(PREFIXES)
        comment = "RDF Version of the UMLS ontology %s; " +\
                  "converted with the UMLS2RDF tool " +\
                  "(https://github.com/ncbo/umls2rdf). "+\
                  "Developed by the NCBO project."

        header_values=dict(
           label=self.ont_code,
           comment=comment%self.ont_code,
           versioninfo=conf.UMLS_VERSION,
           uri=self.ns
        )
        header = ONTOLOGY_HEADER.substitute(header_values)
        fout.write(header)
        for term in self.terms():
            fout.write(term.toRDF(fmt="Turtle",hierarchy=True))
        fout.close()

if __name__ == "__main__":

    con = __get_connection()
    
    umls_conf = None
    with open("umls.conf","r") as fconf:
        umls_conf = [line.split(",") \
                        for line in fconf.read().splitlines() \
                            if len(line) > 0]
        fconf.close()

    if not os.path.isdir(conf.OUTPUT_FOLDER):
        raise Exception("Output folder '%s' not found."%conf.OUTPUT_FOLDER)
    
    for (umls_code, vrt_id, file_out, load_on_field) in umls_conf:
        alt_uri_code = None
        if ";" in umls_code:
            umls_code,alt_uri_code = umls_code.split(";")
        if umls_code.startswith("#"):
            continue
        load_on_cuis = load_on_field == "load_on_cuis"
        print("Generating %s vrt_id %s"%(umls_code,vrt_id))
        ns = get_umls_url(umls_code if not alt_uri_code else alt_uri_code)
        ont = UmlsOntology(umls_code,ns,con,load_on_cuis=load_on_cuis)
        output_file = os.path.join(conf.OUTPUT_FOLDER,file_out)
        ont.load_tables(limit=None)
        print("tables loaded, writing terms ...")
        ont.write_into(output_file,hierarchy=(ont.ont_code <> "MSH"))
        print("done!") 
   
    generate_semantic_types(con,STY_URL,
                os.path.join(conf.OUTPUT_FOLDER,
                "umls_semantictypes.ttl"))
    print("generate MRDOC at global/UMLS level")
