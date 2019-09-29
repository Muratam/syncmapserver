// NOTE: 都合上省きたいものもあるだろう！
const ignoreFuncNames = [
  "main",
]
// NOTE: 都合上無視したいテーブルもあるだろう！
const ignoreTableNames = ["SHA2", "floor"]
// NOTE: 都合上関数同士の関係を省きたいときもあるだろう！
const ignoreFunctionRelativity = false;
// NOTE: 見にくいレイアウトはいやだろう！ dot, fdp, twopi
const layoutType = "dot";

// TODO: join や 複合クエリ
function parseGoSQLRelation(content) {
  // 見つかった関数
  let queries = {}
  for (let matched of content.matchAll(/(?:"\s*(?:SELECT|DELETE|INSERT|UPDATE)\s.+?"|`\s*(?:SELECT|DELETE|INSERT|UPDATE)\s.+?`)/ig)) {
    let index = matched.index
    let functionName = textIndexToFuncName[index]
    let query = matched[0]
    query = query.substring(1, query.length - 1)
    if (query.substring(1, query.length).match(/SELECT/ig)) {
      // NOTE: カッコは一つだけとして雑に複数のSELECTをチェック
      let innerQuery = query.substring(1, query.length).match(/SELECT\s.+/ig)[0]
      let [table, queryType] = getTableName(innerQuery, functionName)
      let stored = { query: innerQuery, table: table, type: queryType }
      if (table) {
        queries[functionName] = (queries[functionName] || []).concat(stored)
        if (verpose) console.log("STORED AT" + functionName)
      }
      if (verpose) console.log(`COMPLEX INNER QUERY at ${functionName}`)
      if (verpose) console.log(stored)
    }
    if (query.match(/JOIN/ig)) { // JOIN チェック
      let commands = query.split(" ")
      let joinedTables = []
      for (let i = 0; i < commands.length - 1; i++) {
        let command = commands[i]
        if (!command.match(/^JOIN$/ig)) continue;
        let table = commands[i + 1];
        let queryType = "SELECT";
        queries[functionName] = (queries[functionName] || []).concat(
          { query: query, table: table, type: queryType })
        joinedTables.push(table)
      }
      if (verpose) console.log("JOIN : " + query)
      if (verpose) console.log(joinedTables)
    }
    let [table, queryType] = getTableName(query, functionName)
    queries[functionName] = (queries[functionName] || []).concat(
      { query: query, table: table, type: queryType })
  }
}

function writeDot(queries, calls) {
  function getUnusedFunctionSet() {
    // 雑にやってもいけるやろ
    let unusedSet = {}
    for (let unused of ignoreFuncNames) {
      unusedSet[unused] = true;
    }
    let dirty = true;
    while (dirty) {
      dirty = false;
      for (let src in calls) {
        if (unusedSet[src]) continue
        let funcs = calls[src] || []
        let sqls = queries[src] || []
        // sql は絶対に必要
        if (sqls.length > 0) continue;
        if (funcs.length === 0) {
          dirty = unusedSet[src] = true;
          continue;
        }
        // 全ての funcs が不要なら不要
        if (funcs.every(x => unusedSet[x])) {
          dirty = unusedSet[src] = true;
          continue;
        }
        // 全てが一つの終点しか参照していないものは不要
        if (funcs.every(dst => unusedSet[dst] || (calls[dst] || []).every(x => unusedSet[x]))) {
          dirty = unusedSet[src] = true;
          continue;
        }
      }
    }
    for (let src in calls) {
      if (unusedSet[src]) continue;
      // たどり着ける先全てにSQLが無ければunused
      let sqlExists = false;
      let visited = {}
      function visit(src) {
        if (sqlExists) return;
        if (unusedSet[src]) return;
        if (visited[src]) return;
        visited[src] = true;
        let sqls = queries[src] || []
        if (sqls.length > 0) sqlExists = true;
        let funcs = calls[src] || []
        for (let dst of funcs) visit(dst);
      }
      visit(src)
      if (sqlExists) continue
      for (let v in visited) unusedSet[v] = true
    }
    return unusedSet
  }
  let unusedSet = getUnusedFunctionSet()
  let tableRel = ""
  let tables = {}
  // sanitize
  let sanitizeLength = 0;
  let sanitizeCache = {}
  function sanitize(name) {
    if (sanitizeCache[name]) return sanitizeCache[name];
    sanitizeLength++;
    sanitizeCache[name] = "a" + sanitizeLength;
    return sanitizeCache[name];
  }

  for (let src in queries) {
    if (ignoreFuncNames.includes(src)) continue;
    let already = {}
    let srcS = sanitize(src);
    for (let dst of queries[src]) {
      // dst.query : all of query
      // dst.type  : "SELECT" / "INSERT" / ...
      let label = `[style="bold"]`
      let tableName = dst.table;
      if (ignoreTableNames.includes(tableName)) continue;
      if ((dst.query.match(/\(/g) || []).length === (dst.query.match(/\)/g) || []).length) {
        // 複合クエリの内側のものは除外
        tables[dst.table] = tables[dst.table] || {};
        tables[dst.table][src] = tables[dst.table][src] || [];
        tables[dst.table][src].push(dst.query)
      }
      if ((already[dst.type] || {})[tableName]) continue;
      already[dst.type] = already[dst.type] || {}
      already[dst.type][tableName] = true;
      if (dst.type === "SELECT") {
        tableRel += `${tableName} -> ${srcS}${label};\n`
      } else { // INSERT / DELETE / UPDATE
        label = `[style="bold",label="${dst.type}",color="#f08060",fontcolor="#f08060"]`
        tableRel += `${srcS} -> ${tableName}${label};\n`
      }
      if (tableName === "parse_error") tableName = `???`;
      tableName = tableName.replace(/([a-z])_/g, "$1\n_")
      tableRel += `${dst.table}[label="${tableName}",shape=box, style="filled, bold, rounded", fillcolor="#ffffcc"];\n`
    }
    tableRel += `${srcS}[label="${src.replace(/([a-z])([A-Z])/g, "$1\n$2")}"];\n`
  }
  // sort and write
  console.log(tables)

  let funcRel = ""
  if (!ignoreFunctionRelativity) {
    for (let src in calls) {
      for (let dst of calls[src]) {
        if (unusedSet[src] || unusedSet[dst]) continue;
        let dstS = sanitize(dst);
        let srcS = sanitize(src);
        funcRel += `${dstS} -> ${srcS}; \n`
        funcRel += `${srcS}[label="${src.replace(/([a-z])([A-Z])/g, "$1\n$2")}"];\n`
        funcRel += `${dstS}[label="${dst.replace(/([a-z])([A-Z])/g, "$1\n$2")}"];\n`
      }
    }
  }
  return `
    digraph  {
      layout = "${layoutType}";
      // overlap = false;
      // splines = true;
      node[
        // landscape = true,
        width = 0.2,
        height = 0.2,
        fontname = "Helvetica",
        style = "filled",
        fillcolor = "#fafafa",
        shape = box,
        style = "filled, bold, rounded"
      ];
      edge[
        len = 0.1,
        fontsize = "8",
        fontname = "Helvetica",
        style = "dashed",
    ];
      ${ funcRel}
      ${ tableRel}
    }`
}
// [tableName,queryType] を返す
function getTableName(query, functionName) {
  query = query.trim().replace(/[\t\n]/g, " ")
  let commands = query.split(" ")
  let queryType = commands[0].toUpperCase()
  let target = {
    "SELECT": "FROM",
    "UPDATE": "UPDATE",
    "INSERT": "INTO",
    "DELETE": "FROM",
  }[queryType]
  for (let i = 0; i < commands.length; i++) {
    let command = commands[i].toUpperCase();
    if (command !== target) {
      continue;
    }
    let table = commands[i + 1].replace(/[^_0-9a-zA-Z]/g, "");
    if (table === "") continue;
    return [table, queryType];
  }
  if (queryType === "INSERT") {
    if (commands.length > 1) return [commands[1], queryType]
  }
  if (queryType === "SELECT") {
    if (verpose) console.log(`WARNING (${functionName}) ${queryType} :: ${query}`)
    if (commands.length > 1 && commands[1].match(/[_a-zA-Z0-9]+\(/))
      return [commands[1].replace(/\(.+/, ""), queryType]
  }
  console.error(`ERROR (${functionName}) ${queryType} :: ${query}`)
  return ["parse_error", queryType]
}
let parsed = JSON.parse("" + require("fs").readFileSync(0))
// 整形
let queries = {};
let calls = {};
for (let query of parsed.queries.main) { // [{query/caller}]
  let [tableName, queryType] = getTableName(query.query, query.caller);
  queries[query.caller] = (queries[query.caller] || []).concat({
    table: tableName,
    type: queryType,
    query: query.query,
  })
}
for (let call of parsed.calls.main) { // [caller/callee]
  if (call.caller.match(/\./)) continue;
  if (call.callee.match(/\./)) continue;
  calls[call.caller] = (calls[call.caller] || []).concat(call.callee)
}
for (let i = 0; i < calls.length; i++)calls[i] = Array.from(new Set(calls[i]));

let dotted = writeDot(queries, calls);
require("fs").writeFileSync("sqlmap.dot", dotted)
