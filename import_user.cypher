
CALL apoc.periodic.iterate("
CALL apoc.load.csv('file:///dataset/csv/user.csv', {
  mapping: {
    user_id: {type: 'str'},
    name: {type: 'str'}
  }
}) YIELD map as row return row
","
MERGE (u:User {id: row.user_id})
SET u.name = row.name
", {batchSize:10000, iterateList:true, parallel:true});
