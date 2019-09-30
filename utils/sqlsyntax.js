const execSync = require('child_process').execSync
const dbname = process.env.DB_NAME || "isucari",
  username = process.env.DB_USER || "aokabi",
  password = process.env.DB_PASSWORD || "aokabi"

let parsed = JSON.parse("" + require("fs").readFileSync(0));
for (let queryStruct of parsed.queries.main) {
  let query = queryStruct.query;
  try {
    query = query.replace(/\?/g, Date.now().toString())
    // execSync(`echo '${query}' >> hoge`)
    execSync(`mysql -u${username} -p${password} ${dbname} -e'explain ${query}' 2>&1`)
  }
  catch (error) {
    console.error(error.stdout.toString())
    console.error(`query> ${query}`)
    console.error("-----------------------------")
  }
}
