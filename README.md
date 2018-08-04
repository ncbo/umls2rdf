This project takes a MySQL Unified Medical Language System (UMLS) database and converts the ontologies to RDF using OWL and SKOS as the main schemas.

To use it:

* Specify your database connection conf.py
* Specify the SAB ontologies to export in umls.conf

The umls.conf configuration file must contain one ontology per line. The lines are comma separated tuples where the elements are:

<pre>
(0) SAB
(1) BioPortal Virtual ID. This is optional, any value works.
(2) Output file name
(3) Conversion strategy. Accepted values (load_on_codes, load_on_cuis).
</pre>

Note that 'CCS COSTAR DSM3R DSM4 DXP ICPC2ICD10ENG MCM MMSL MMX MTHCMSFRF MTHMST MTHSPL MTH NDFRT SNM' have no code and should not be loaded on loads_on_codes.

umls2rdf.py is designed to be an offline, run-once process. It's memory intensive and exports all of the default ontologies in umls.conf in 3h 30min. The ontologies listed in umls.conf are the UMLS ontologies accessible in [BioPortal](https://bioportal.bioontology.org/).

If running a Windows 10 OS with MySQL, the following tips may be of help.

- Install [MySQL 5.5] (https://dev.mysql.com/downloads/mysql/5.5.html#downloads) to avoid the InnoDB space [disclaimer] (https://www.nlm.nih.gov/research/umls/implementation_resources/scripts/README_RRF_MySQL_Output_Stream.html) by NLM. 
- [Python 2.7.x] (https://www.python.org/downloads/) should be used to avoid syntax errors on 'raise Attribute'
- For installtion of the MySQLdb module, <pre>python -m pip install MySQLdb</pre> doesn't seem to work. Install with executable [MySQL-python-1.2.3.win-amd64-py2.7.exe] (http://www.codegood.com/archives/129) (last known location).
- Create your RRF subset(s) with the MySQL load option, load your database, edit conf.py and umls.py to specifications, run umsl2rdf.py
