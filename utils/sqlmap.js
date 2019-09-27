// NOTE: 都合上省きたいものもあるだろう！
const ignoreFuncNames = [
  "main",
  // "Render", "GetInitialize", "wsGameHandler", "NewHandler", "InitBenchmark",
]
// NOTE: 都合上無視したいテーブルもあるだろう！
const ignoreTableNames = ["SHA2", "floor"]
// NOTE: 都合上関数同士の関係を省きたいときもあるだろう！
const ignoreFunctionRelativity = false;
// NOTE: 見にくいレイアウトはいやだろう！ dot, fdp, twopi
const layoutType = "dot";
// NOTE: 怪しいログを出しておきたいこともあるだろう！
const verpose = false;

// JOIN / func()A(){} | func A(){}
// func A(){}  / func (){} /
// func (this *SyncMapServerConn) insertImpl(value []byte) int {}
// func                           insertImpl(value []byte) int {}
// func (this *SyncMapServerConn)                          int {}

const fs = require("fs")
function parseGoSQLRelation(content) {
  content = content.replace(/\/\/.*\n/g, "") // コメント除去
  content = content.replace(/\/\*.*\*\//g, "") // コメント除去
  content = content.replace(/\t\n/g, " ") // 空白
  content = content.replace(/\s+/g, " ")
  content = content.replace(/"\s*\+\s*"/g, "") // 文字列結合
  content = content.replace(/`\s*\+\s*`/g, "") // 文字列結合
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
  // 見つかった関数
  let textIndexToFuncName = new Array(content.length);
  let functionNames = []
  let preI = 0
  let preName = "unknown_function"
  let havingFunctionsMap = {}
  // NOTE:interface{} を引数に取るものが無理かも
  for (let found of content.matchAll(/func(?:\s*?\(.+?\)\s*?|\s+?)(.+?)\(.*?\)/g)) {
    let funcName = found[1].trim()
    if (!funcName.match(/^[_0-9a-zA-Z]+$/)) {
      if (verpose) {
        console.log(funcName)
        console.log(found[0])
      }
      continue;
    }
    let index = found.index
    for (let i = preI; i < index; i++) {
      textIndexToFuncName[i] = preName
    }
    functionNames.push(funcName)
    havingFunctionsMap[funcName] = []
    preName = funcName
    preI = index
  }
  for (let i = preI; i < content.length; i++) {
    textIndexToFuncName[i] = preName
  }
  // 見つかったSQL
  // SET ALTER RENAME DROP REPLACE あたりは知らない...
  let functionNameToSQL = {}
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
        functionNameToSQL[functionName] = (functionNameToSQL[functionName] || []).concat(stored)
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
        functionNameToSQL[functionName] = (functionNameToSQL[functionName] || []).concat(
          { query: query, table: table, type: queryType })
        joinedTables.push(table)
      }
      if (verpose) console.log("JOIN : " + query)
      if (verpose) console.log(joinedTables)
    }
    let [table, queryType] = getTableName(query, functionName)
    functionNameToSQL[functionName] = (functionNameToSQL[functionName] || []).concat(
      { query: query, table: table, type: queryType })
  }
  // 見つかったトークン
  for (let found of content.matchAll(/([_0-9a-zA-Z]+)/g)) {
    let foundFuncName = found[0]
    let rootFuncName = textIndexToFuncName[found.index]
    if (havingFunctionsMap[rootFuncName] === undefined) continue;
    if (havingFunctionsMap[foundFuncName] === undefined) continue;
    if (rootFuncName === foundFuncName) continue;
    havingFunctionsMap[rootFuncName].push(foundFuncName)
  }
  for (let key in havingFunctionsMap) {
    let set = new Set(havingFunctionsMap[key])
    set.delete(key) // 自身は不要
    havingFunctionsMap[key] = Array.from(set)
  }
  return {
    functionNames,
    havingFunctionsMap,
    functionNameToSQL
  }
}
function joinFiles(filenames) {
  let content = ""
  for (let filename of filenames) {
    if (!filename.endsWith(".go")) continue;
    content += fs.readFileSync(filename, "utf8")
  }
  return content
}
function writeDot(parsed) {
  let { functionNames, havingFunctionsMap, functionNameToSQL } = parsed
  function getUnusedFunctionSet() {
    // 雑にやってもいけるやろ
    let unusedSet = {}
    for (let unused of ignoreFuncNames) {
      unusedSet[unused] = true;
    }
    let dirty = true;
    while (dirty) {
      dirty = false;
      for (let src in havingFunctionsMap) {
        if (unusedSet[src]) continue
        let funcs = havingFunctionsMap[src] || []
        let sqls = functionNameToSQL[src] || []
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
        if (funcs.every(dst => unusedSet[dst] || (havingFunctionsMap[dst] || []).every(x => unusedSet[x]))) {
          dirty = unusedSet[src] = true;
          continue;
        }
      }
    }
    for (let src in havingFunctionsMap) {
      if (unusedSet[src]) continue;
      // たどり着ける先全てにSQLが無ければunused
      let sqlExists = false;
      let visited = {}
      function visit(src) {
        if (sqlExists) return;
        if (unusedSet[src]) return;
        if (visited[src]) return;
        visited[src] = true;
        let sqls = functionNameToSQL[src] || []
        if (sqls.length > 0) sqlExists = true;
        let funcs = havingFunctionsMap[src] || []
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
  for (let src in functionNameToSQL) {
    if (ignoreFuncNames.includes(src)) continue;
    let already = {}
    for (let dst of functionNameToSQL[src]) {
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
        tableRel += `${tableName} -> ${src}${label};\n`
      } else { // INSERT / DELETE / UPDATE
        label = `[style="bold",label="${dst.type}",color="#f08060",fontcolor="#f08060"]`
        tableRel += `${src} -> ${tableName}${label};\n`
      }
      if (tableName === "parse_error") tableName = `???`;
      tableName = tableName.replace(/([a-z])_/g, "$1\n_")
      tableRel += `${dst.table}[label="${tableName}",shape=box, style="filled, bold, rounded", fillcolor="#ffffcc"];\n`
    }
    tableRel += `${src}[label="${src.replace(/([a-z])([A-Z])/g, "$1\n$2")}"];\n`
  }
  // sort and write
  console.log(tables)

  let funcRel = ""
  if (!ignoreFunctionRelativity) {
    for (let src in havingFunctionsMap) {
      for (let dst of havingFunctionsMap[src]) {
        if (unusedSet[src] || unusedSet[dst]) continue;
        funcRel += `${dst} -> ${src}; \n`
        funcRel += `${src}[label="${src.replace(/([a-z])([A-Z])/g, "$1\n$2")}"];\n`
        funcRel += `${dst}[label="${dst.replace(/([a-z])([A-Z])/g, "$1\n$2")}"];\n`
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
    }
    `
}

// 引数のパス一覧の中のGoのコード中からSQLの関係図を生成する
// router.Get("/hoge",func(){...}) みたいなコードは全てflattenした場合を想定。
// 1. \s*?func\s+?(\S+?)\(.*?\) を関数の始まりのラインとして解釈して、
// 2. [`"]\s*(SELECT|DELETE|INSERT|UPDATE) .+?[`"]があるものをSQL文として解釈する。
//   - SELECT は依存しているテーブル名を全て抜き出し、 他は...?
// 3. SQL文を含む関数一覧が取れるので関数同士の依存関係のグラフも生成できる。
// 4. SQLをパースして、テーブルとの関係を取得してgraphvizする。

// 除外したい場合もあるだろうので、 `node sqlmap.js ./*.go` みたいにするのがいいかな
let joinedContents = joinFiles(process.argv)
let parsed = parseGoSQLRelation(joinedContents)
let dotted = writeDot(parsed)
fs.writeFileSync("sqlmap.dot", dotted)
