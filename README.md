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
