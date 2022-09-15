<h3>PURPOSE</h3>
<br>
<p>
The pupose of this application is to deliver data from an S3 bucket to fact and dimensional tables
in Redshift data warehouse through a data pipeline using Apache Airflow to schedule batch Extractions,
Transformation, and Loading.
</p>
<br>

<h3>SETUP</h3>
<br>
<p>
# Setup Json Path
<ul>
    <li>
    Create an object key within the S3 bucket <strong>udacity-dend2-mogo</strong>. Name it 
    <strong>jsonpaths</strong>.
    </li>
    <li>
    Upload the two JSON files "jpath.json" and "jsonpath_songs.json" into this new key,
    <strong>jsonpaths</strong>
    </li>
</ul>
<br>
# Create Tables
<ul>
    <li>
    Copy "create_tables.sql" file into <strong>"/tmp/"</strong> directory
    </li>
    <li>
    Then, from Airflow UI, run the "create_table" DAG located in "to_redshift.py" file.
    </li>
    <li>
    This will create ALL required tables in AWS Redshift
    </li>
</ul>
<br>
# Load Staging Table
</p>
