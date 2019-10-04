// NOTE: 都合上省きたいものもあるだろう！
const ignoreFuncPrefixes = [
  "main", "(*SyncMapServerConn)."
]
// NOTE: 都合上無視したいテーブルもあるだろう！
const ignoreTableNames = ["SHA2", "floor"]
// NOTE: 都合上関数同士の関係を省きたいときもあるだろう！
const ignoreFunctionRelativity = false;
// NOTE: 見にくいレイアウトはいやだろう！ dot, fdp, twopi
const layoutType = "dot";

function parseComplexSQL(query, functionName) {
  let result = []
  // NOTE: カッコは一つだけとして雑に複数のSELECTをチェック
  if (query.substring(1, query.length).match(/SELECT/ig)) {
    let innerQuery = query.substring(1, query.length).match(/SELECT\s.+/ig)[0]
    let [table, queryType] = getTableName(innerQuery, functionName)
    result.push({ query: innerQuery, table: table, type: queryType })
    console.warn(`COMPLEX INNER QUERY at ${functionName}`)
  }
  // JOIN チェック
  if (query.match(/JOIN/ig)) {
    let commands = query.split(" ")
    let joinedTables = []
    for (let i = 0; i < commands.length - 1; i++) {
      let command = commands[i]
      if (!command.match(/^JOIN$/ig)) continue;
      let table = commands[i + 1];
      result.push({ query: query, table: table, type: "SELECT" })
    }
    console.warn("JOIN : " + query)
    console.warn(joinedTables)
  }
  { // 普通に
    let [table, queryType] = getTableName(query, functionName)
    result.push({ query: query, table: table, type: queryType })
  }
  return result;
}

function writeDot(queries, calls) {
  function getUnusedFunctionSet() {
    // 雑にやってもいけるやろ
    let unusedSet = {}
    for (let unused of ignoreFuncPrefixes) unusedSet[unused] = true;
    let dirty = true;
    let funcList = [];
    for (let src in calls) {
      for (let dst of calls[src]) funcList.push(dst);
      funcList.push(src);
    }
    funcList = Array.from(new Set(funcList))
    while (dirty) {
      dirty = false;
      for (let src of funcList) {
        if (unusedSet[src]) continue;
        let funcs = calls[src] || []
        let sqls = queries[src] || []
        // sql は絶対に必要
        if (sqls.length > 0) continue;
        // ignore のものから始まってほしくない
        for (let prefix of ignoreFuncPrefixes) {
          if (!src.startsWith(prefix)) continue;
          dirty = unusedSet[src] = true;
        }
        if (dirty) continue;
        // 自身が誰も参照していなければ不要
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
        // 自身が誰からも参照されていなければ不要
        //   if (
        //     (() => {
        //       for (let src2 in calls) {
        //         if (unusedSet[src2]) continue;
        //         for (let dst2 of calls[src2]) {
        //           if (dst2 === src) return false;
        //         }
        //       }
        //       return true;
        //     })()
        //   ) {
        //     dirty = unusedSet[src] = true;
        //     continue;
        //   }
      }
    }
    for (let src of funcList) {
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
    // if (ignoreFuncPrefixes.includes(src)) continue;
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
        tableRel += `${tableName} -> ${srcS}${label}[dir="none"];\n`
      } else { // INSERT / DELETE / UPDATE
        label = `[style="bold",dir="none",label="${dst.type}",color="#f08060",fontcolor="#f08060"]`
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
        funcRel += `${srcS} -> ${dstS}; \n`
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
      ${funcRel}
      ${tableRel}
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
    console.warn(`WARNING (${functionName}) ${queryType} :: ${query}`)
    if (commands.length > 1 && commands[1].match(/[_a-zA-Z0-9]+\(/))
      return [commands[1].replace(/\(.+/, ""), queryType]
  }
  console.error(`ERROR (${functionName}) ${queryType} :: ${query}`)
  return ["parse_error", queryType]
}
function parseJsonInput() {
  let parsed = JSON.parse("" + require("fs").readFileSync(0))
  // 整形
  let queries = {};
  let calls = {};
  for (let queryStruct of parsed.queries.main) { // [{query/caller}]
    let { query, caller } = queryStruct;
    queries[caller] = (queries[caller] || []).concat(parseComplexSQL(query, caller))
  }
  for (let call of parsed.calls.main) { // [caller/callee]
    let { caller, callee } = call;
    if (caller === callee) continue; // 自己ループ
    calls[caller] = (calls[caller] || []).concat(callee)
  }
  // deduplicate
  for (let k in calls) calls[k] = Array.from(new Set(calls[k]));
  // sort key and values
  let callsKeys = [];
  for (let k in calls) callsKeys.push(k);
  let resCalls = {};
  for (let k of callsKeys.sort()) resCalls[k] = calls[k].sort();
  let queriesKeys = [];
  for (let k in queries) queriesKeys.push(k);
  let resQueries = {};
  for (let k of queriesKeys.sort()) resQueries[k] = queries[k].sort();
  return [resCalls, resQueries]
}

let [calls, queries] = parseJsonInput();
let dotted = writeDot(queries, calls);
require("fs").writeFileSync("./sqlgraph.dot", dotted)
require('child_process').execSync("dot sqlgraph.dot -Tpng > sqlgraph.png")
