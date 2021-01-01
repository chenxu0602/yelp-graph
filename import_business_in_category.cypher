
CALL apoc.periodic.iterate("
CALL apoc.load.csv('file:///dataset/csv/business_in_category.csv', {
  mapping: {
    business_id: {type: 'str'},
    category: {type: 'str'}
  }
}) YIELD map as row return row
","
MATCH (b:Business {id: row.business_id})
MATCH (c:Category {name: row.category})
MERGE (b)-[:IN_CATEGORY]->(c)
", {batchSize:100, iterateList:true, parallel:true});
