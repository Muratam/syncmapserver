const execSync = require('child_process').execSync

const dbname = process.env.DB_NAME || "isucari",
  username = process.env.DB_USER || "aokabi",
  password = process.env.DB_PASSWORD || "aokabi"

var reader = require('readline').createInterface({
  input: process.stdin,
  output: process.stdout
});
var json = ""
reader.on('line', function (line) {
  json += line
});
reader.on('close', function () {
  //do something
  var queries = eval("(" + json + ")")
  for (var table in queries) {
    for (var func in queries[table]) {
      queries[table][func].forEach(query => {
        try {
          query = query.replace(/\?/g, Date.now().toString())
          execSync(`mysql -u${username} -p${password} ${dbname} -e'explain ${query}' 2>&1`)
        }
        catch (error) {
          console.error(error.stdout.toString())
          console.error(`query> ${query}`)
          console.error("-----------------------------")
        }
      })
    }
  }
});
