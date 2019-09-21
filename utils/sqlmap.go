package main

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"log"
	"os"

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/loader"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("%+v", err)
	}
}

func run() error {
	source := `
package main
import "fmt"
type s struct{}
func (s s)Println(x string) {}
func main(){
  fmt.Println("xxx")
  {
      fmt := s{}
      fmt.Println("yyy")
  }
  fmt.Println("xxx")
}
`
	loader := loader.Config{ParserMode: parser.ParseComments}
	astf, err := loader.ParseFile("main.go", source)
	if err != nil {
		return err
	}
	loader.CreateFromFiles("main", astf)
	prog, err := loader.Load()
	if err != nil {
		return err
	}
	main := prog.Package("main")
	fmtpkg := prog.Package("fmt").Pkg
	for _, f := range main.Files {
		ast.Inspect(f, func(node ast.Node) bool {
			t, _ := node.(*ast.SelectorExpr)
			if t == nil {
				return true
			}
			if main.Info.ObjectOf(t.Sel).Pkg() != fmtpkg {
				return false
			}
			ast.Inspect(t.X, func(node ast.Node) bool {
				t, _ := node.(*ast.Ident)
				if t == nil {
					return true
				}
				if t.Name == "fmt" && t.Obj == nil {
					t.Name = "fmt2"
				}
				return false
			})
			return false
		})
		astutil.RewriteImport(prog.Fset, f, "fmt", "fmt2")
		pp := &printer.Config{Tabwidth: 8, Mode: printer.UseSpaces | printer.TabIndent}
		pp.Fprint(os.Stdout, prog.Fset, f)
	}
	return nil
}
