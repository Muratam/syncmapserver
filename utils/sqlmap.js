// NOTE: 都合上省きたいものもあるだろう！
const ignoreFuncNames = ["main", "unsafeParseDate", "NewHandler", "InitBenchmark"]
// NOTE: 都合上関数同士の関係を省きたいときもあるだろう！
const ignoreFunctionRelativity = false;
// NOTE: 見にくいレイアウトはいやだろう！ dot, fdp, neato, sfdp, twopi
const layoutType = "dot";

const fs = require("fs")
function parseGoSQLRelation(content) {
  content = content.replace(/\/\/.*\n/g, "")
  function escape(s) {
    return s.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')
  }
  function getTableName(query) {
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
      return [commands[i + 1].replace(/[^_0-9a-zA-Z]/g, ""), queryType];
    }
    console.error("ERROR QUERY (splited ?)!!")
    console.error(query)
    return ["parse_error", queryType]
  }
  // 見つかった関数
  let textIndexToFuncName = new Array(content.length);
  let functionNames = []
  let preI = 0
  let preName = "unknown_function"
  let havingFunctionsMap = {}
  for (let found of content.matchAll(/func\s+?(\S+?)\(.*?\)/g)) {
    let funcName = found[1]
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
  let sqlFounds = content.match(/"\s*(SELECT|DELETE|INSERT|UPDATE)\s.+?"/ig) || []
  for (let other of content.match(/`\s*(SELECT|DELETE|INSERT|UPDATE)\s.+?`/ig) || []) sqlFounds.push(other)
  preI = 0
  let functionNameToSQL = {}
  for (let sql of sqlFounds) {
    let index = content.substring(preI).search(escape(sql)) + preI
    preI = index
    let functionName = textIndexToFuncName[index]
    let normalized = sql.substring(1, sql.length - 1)
    let [table, queryType] = getTableName(normalized)
    functionNameToSQL[functionName] = (functionNameToSQL[functionName] || []).concat(
      { query: normalized, table: table, type: queryType })
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
  let usedTableNames = {}
  for (let src in functionNameToSQL) {
    if (ignoreFuncNames.includes(src)) continue;
    for (let dst of functionNameToSQL[src]) {
      // dst.query : all of query
      // dst.type  : "SELECT" / "INSERT" / ...
      let label = `[style="bold"]`
      usedTableNames[dst.table] = true;
      if (dst.type === "SELECT") {
        tableRel += `${dst.table} -> ${src}${label};\n`
      } else { // INSERT / DELETE / UPDATE
        label = `[style="bold",label="${dst.type}",color="blue",fontcolor="blue"]`
        tableRel += `${src} -> ${dst.table}${label};\n`
      }
    }
  }
  let tableStr = ""
  for (let table in usedTableNames) {
    let tableName = table;
    if (table === "parse_error") tableName = `???`;
    tableStr += `${table}[label="${tableName}",shape=box, style="filled, bold, rounded", fillcolor="#ffffcc"];\n`
  }

  let funcRel = ""
  let funcStr = ""
  if (!ignoreFunctionRelativity) {
    for (let src in havingFunctionsMap) {
      for (let dst of havingFunctionsMap[src]) {
        if (unusedSet[src] || unusedSet[dst]) continue;
        funcRel += `${dst} -> ${src};\n`
      }
    }
    for (let func of functionNames) {
      if (unusedSet[func]) continue;
      funcStr += `${func}[label="${func}"];\n`
    }
  }
  return `
  digraph  {
    layout = "${layoutType}";
    overlap = false;
    splines = false;
    node [
      landscape = true,
      width = 1,
      height = 1,
      fontname = "Helvetica",
      style="filled",
      fillcolor="#fafafa",
    ];
    edge [
      len=0.1,
      fontsize="8",
      fontname = "Helvetica",
      style="dashed",
    ];
    ${tableStr}
    ${funcStr}
    ${funcRel}
    ${tableRel}
  }
  `
}

// 引数のパス一覧の中のGoのコード中からSQLの関係図を生成する
// router.Get("/hoge",func(){...}) みたいなコードは全てflattenした場合を想定。
// 1. \s*?func\s+?(\S+?)\(.*?\) を関数の始まりのラインとして解釈して、
// 2. [`"]\s*(SELECT|DELETE|INSERT|UPDATE) .+?[`"] があるものをSQL文として解釈する。
//   - SELECT は依存しているテーブル名を全て抜き出し、 他は...?
// 3. SQL文を含む関数一覧が取れるので関数同士の依存関係のグラフも生成できる。
// 4. SQLをパースして、テーブルとの関係を取得してgraphvizする。

// 除外したい場合もあるだろうので、 `node sqlmap.js ./*.go` みたいにするのがいいかな
let joinedContents = joinFiles(process.argv)
let parsed = parseGoSQLRelation(joinedContents)
let dotted = writeDot(parsed)
fs.writeFileSync("sqlmap.dot", dotted)
