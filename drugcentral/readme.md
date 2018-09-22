install postgres 10.5 (by default had 9)
download dump 
http://drugcentral.org/download
http://iridium.noip.me/drugcentral.dump.08262018.sql.gz

$ createdb drugcentral
$ psql -d drugcentral -f <(zcat drugcentral.dump.08262018.sql.gz)

echo "COPY (SELECT * FROM omop_relationship
LEFT JOIN identifier ON omop_relationship.struct_id=identifier.struct_id
WHERE omop_relationship.relationship_name = 'indication' AND (omop_relationship.umls_cui IS NOT NULL OR omop_relationship.snomed_conceptid IS NOT NULL)
) TO STDOUT WITH CSV HEADER" > query.sql

psql -d drugcentral -f query.sql > indications_drugcentral.csv
