COPY (SELECT * FROM omop_relationship
LEFT JOIN identifier ON omop_relationship.struct_id=identifier.struct_id
WHERE omop_relationship.relationship_name = 'indication' AND
(omop_relationship.umls_cui IS NOT NULL OR omop_relationship.snomed_conceptid IS NOT NULL)
) TO STDOUT WITH CSV HEADER
