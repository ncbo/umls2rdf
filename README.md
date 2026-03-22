This project takes a MySQL Unified Medical Language System (UMLS) database and converts the ontologies to RDF using OWL and SKOS as the main schemas.

Virtual Appliance users can review the [documentation in the OntoPortal Administration Guide}(https://ontoportal.github.io/documentation/administration/ontologies/handling_umls).

To use it:

* Install Python dependencies with <code>pip install -r requirements.txt</code>
* Specify your database connection conf.py
* Create the MySQL database with <code>python create_mysql_db.py</code>
* Optionally download the full pre-built UMLS release with download_umls.py
* Or run the full resumable import/export pipeline with <code>python run_umls_pipeline.py</code>
* Specify the SAB ontologies to export in umls.conf

Generated TTL files are written under a versioned output directory based on
<code>OUTPUT_FOLDER</code> from <code>conf.py</code>. A common pattern is
<code>OUTPUT_FOLDER = "output/%s" % UMLS_VERSION.upper()</code>, which writes to
<code>output/2025AB</code>.

The umls.conf configuration file must contain one ontology per line. The lines are comma separated tuples where the elements are:

<em>The following list needs updating.</em>
<pre>
(0) SAB
(1) BioPortal Virtual ID. This is optional, any value works.
(2) Output file name
(3) Conversion strategy. Accepted values (load_on_codes, load_on_cuis).
</pre>

Note that 'CCS COSTAR DSM3R DSM4 DXP ICPC2ICD10ENG MCM MMSL MMX MTHCMSFRF MTHMST MTHSPL MTH NDFRT SNM' have no code and should not be loaded on loads_on_codes.

umls2rdf.py is designed to be an offline, run-once process. 
It's memory intensive and exports all of the default ontologies in umls.conf in 3h 30min. 
The ontologies listed in umls.conf are the UMLS ontologies accessible in [BioPortal](https://bioportal.bioontology.org/).

To download the full UMLS release archive before loading it into MySQL, install
the [cthoyt/umls_downloader](https://github.com/cthoyt/umls_downloader) package
and run:

<pre>
python download_umls.py
</pre>

The downloader returns the local path to the downloaded archive. This step only
fetches and extracts the pre-built UMLS release; you still need to load the
UMLS tables into MySQL before running <code>umls2rdf.py</code>. The script uses
<code>UMLS_VERSION</code> and <code>UMLS_API_KEY</code> from <code>conf.py</code>.
If <code>UMLS_DOWNLOAD_DIR</code> is set, the zip archive is stored under that
directory. If it is not set, the library default <code>~/.data/bio/umls</code>
is used. By default, the archive is extracted into an
<code>extracted</code> subdirectory next to the downloaded zip. You can override
that location with <code>UMLS_EXTRACT_DIR</code>.

To create the target MySQL database with explicit UTF-8 settings, run:

<pre>
python create_mysql_db.py
</pre>

The script creates or updates <code>DB_NAME</code> from <code>conf.py</code>
with <code>utf8mb4</code> character set and
<code>utf8mb4_unicode_ci</code> collation.

To run the full UMLS pipeline end-to-end, use:

<pre>
python run_umls_pipeline.py
</pre>

The pipeline performs these stages:

* Download the configured UMLS full release archive
* Extract the release only when the extracted <code>META</code> directory is not already present
* Recreate the configured <code>DB_NAME</code> and load it with the extracted <code>META/populate_mysql_db.sh</code> script
* Run <code>umls2rdf.py</code>

The loader script is patched into a pipeline work directory before execution, so
the extracted UMLS release contents remain unchanged. Pipeline state is stored
under <code>PIPELINE_WORK_DIR</code> (default:
<code>data/pipeline/&lt;UMLS_VERSION&gt;</code>) and reruns skip completed steps
after validating the extracted files, MySQL tables, and RDF output. Add
<code>MYSQL_HOME</code> to <code>conf.py</code>; if your MySQL client is at
<code>/usr/bin/mysql</code>, set <code>MYSQL_HOME = "/usr"</code>. Pipeline
stdout and stderr are appended to <code>PIPELINE_LOG_FILE</code> when set, or
to <code>data/pipeline/&lt;UMLS_VERSION&gt;/pipeline.log</code> by default.

If <code>PROCESS_ONLY_CURRENT_UMLS_VERSION</code> is set to <code>True</code>,
the exporter only processes ontologies whose <code>MRSAB.IMETA</code> exactly
matches <code>UMLS_VERSION</code>. Ontologies with a different value are skipped
and logged.

If you get an error when installing the MySQL-python python library, https://stackoverflow.com/questions/12218229/my-config-h-file-not-found-when-intall-mysql-python-on-osx-10-8 may be of help.

If running a Windows 10 OS with MySQL, the following tips may be of help.

- Install [MySQL 5.5](https://dev.mysql.com/downloads/mysql/5.5.html#downloads) to avoid the InnoDB space [disclaimer](https://www.nlm.nih.gov/research/umls/implementation_resources/scripts/README_RRF_MySQL_Output_Stream.html) by NLM. 
- [Python 2.7.x](https://www.python.org/downloads/) should be used to avoid syntax errors on 'raise Attribute'
- For installtion of the MySQLdb module <pre>python -m pip install MySQLdb</pre> is error prone. Install with executable [MySQL-python-1.2.3.win-amd64-py2.7](http://www.codegood.com/archives/129) (last known location).
- Create your RRF subset(s) using mmsys with the MySQL load option, load your database, edit conf.py and umls.py to specifications, run umsl2rdf.py
