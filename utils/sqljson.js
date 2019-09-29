// 引数のパス一覧の中のGoのコード中からSQLの関係図を生成する
// router.Get("/hoge",func(){...}) みたいなコードは全てflattenした場合を想定。
// 1. \s*?func\s+?(\S+?)\(.*?\) を関数の始まりのラインとして解釈して、
// 2. [`"]\s*(SELECT|DELETE|INSERT|UPDATE) .+?[`"]があるものをSQL文として解釈する。
//   - SELECT は依存しているテーブル名を全て抜き出し、 他は...?
// 3. SQL文を含む関数一覧が取れるので関数同士の依存関係のグラフも生成できる。
// 4. SQLをパースして、テーブルとの関係を取得してgraphvizする。
// 除外したい場合もあるだろうので、 `node sqlmap.js ./*.go` みたいにするのがいいかな

const fs = require("fs");
let content = ""
for (let filename of process.argv) {
  if (!filename.endsWith(".go")) continue;
  content += fs.readFileSync(filename, "utf8")
}

content = content.replace(/\/\/.*\n/g, "") // コメント除去
content = content.replace(/\/\*.*\*\//g, "") // コメント除去
content = content.replace(/\t\n/g, " ") // 空白
content = content.replace(/\s+/g, " ")
content = content.replace(/"\s*\+\s*"/g, "") // 文字列結合
content = content.replace(/`\s*\+\s*`/g, "") // 文字列結合
// 見つかった関数
let textIndexToFuncName = new Array(content.length);
let functionNames = []
let preI = 0
let preName = "unknown_function"
let roots = {}
// NOTE:interface{} を引数に取るものが無理かも
for (let found of content.matchAll(/func(?:\s*?\(.+?\)\s*?|\s+?)(.+?)\(.*?\)/g)) {
  let funcName = found[1].trim()
  if (!funcName.match(/^[_0-9a-zA-Z]+$/)) {
    console.warn(funcName)
    console.warn(found[0])
    continue;
  }
  let index = found.index
  for (let i = preI; i < index; i++) {
    textIndexToFuncName[i] = preName
  }
  functionNames.push(funcName)
  roots[funcName] = []
  preName = funcName
  preI = index
}
for (let i = preI; i < content.length; i++) textIndexToFuncName[i] = preName
// 見つかったSQL
// SET ALTER RENAME DROP REPLACE あたりは知らない...
let queries = []
for (let matched of content.matchAll(/(?:"\s*(?:SELECT|DELETE|INSERT|UPDATE)\s.+?"|`\s*(?:SELECT|DELETE|INSERT|UPDATE)\s.+?`)/ig)) {
  let index = matched.index
  let functionName = textIndexToFuncName[index]
  let query = matched[0]
  query = query.substring(1, query.length - 1)
  queries.push({
    query: query,
    caller: functionName,
  })
}
// 見つかったトークン
let calls = []
for (let found of content.matchAll(/([_0-9a-zA-Z]+)/g)) {
  let foundFuncName = found[0]
  let rootFuncName = textIndexToFuncName[found.index]
  if (roots[rootFuncName] === undefined) continue;
  if (roots[foundFuncName] === undefined) continue;
  if (rootFuncName === foundFuncName) continue;
  calls.push({
    caller: rootFuncName,
    callee: foundFuncName,
  })
}
let result = {
  queries: { main: queries },
  calls: { main: calls }
}
console.log(JSON.stringify(result));
