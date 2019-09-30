# [findqueries / findqueries.js] -> sqlgraph.js
- go のファイルをパースしてグラフにするやつ.
- go(AST)実装と js(regex)実装があって、二つあることで本番に使えない確率を減らす。
- sqlgraph.dot と sqlgraph.png を生成する。
- `findqueries /path/to/webapp/ | node sqlgraph.js`
- `node findqueries.js /path/to/webapp/*.go | node sqlgraph.js`

# [findqueries / findqueries.js] -> sqlsyntax.js
- SQLがvalidかをチェックしてくれる.
- `findqueries /path/to/webapp/ | node sqlsyntax.js`
- `node findqueries.js /path/to/webapp/*.go | node sqlsyntax.js`

# reflect.go
- SQL の更新がない場合にGoに直接コードを埋め込むときのテンプレート

# template.nim
- go の "html/template" のリフレクションが遅い場合、直で埋め込むやつ

# findqueries.go
- `go get -u github.com/nakario/findqueries`
