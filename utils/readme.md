# [findqueries / sqljson.js] -> sqlgraph.js
- go のファイルをパースしてグラフにするやつ
- go(AST)実装と js(regex)実装があって、二つあることで本番に使えない確率を減らす。
- `findqueries ../webapp/go/ | node sqlgraph.js | dot sqlmap.dot -Tpng`
- `findqueries ../webapp/go/*.go | node sqlgraph.js | dot sqlmap.dot -Tpng`

# [findqueries / sqljson.js] -> sqlsyntax.js
- SQLがvalidかをチェックしてくれる.

# reflect.go
- SQL の更新がない場合にGoに直接コードを埋め込むときのテンプレート
# template.nim
- go の "html/template" のリフレクションが遅い場合、直で埋め込むやつ
