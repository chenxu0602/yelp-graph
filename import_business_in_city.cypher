
CALL apoc.periodic.iterate("
CALL apoc.load.csv('file:///dataset/csv/business_in_city.csv', {
  mapping: {
    business_id: {type: 'str'},
    city: {type: 'str'}
  }
}) YIELD map as row return row
","
MATCH (b:Business {id: row.business_id})
MATCH (c:City {name: row.city})
MERGE (b)-[:IN_CITY]->(c)
", {batchSize:100, iterateList:true, parallel:true});
