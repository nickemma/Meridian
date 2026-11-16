// meridian-pki creates ephemeral local research credentials without overwriting.
package main

import (
	"flag"
	"fmt"
	security "github.com/nickemma/meridian/internal/security/adapters"
	"os"
)

func main() {
	dir := flag.String("out", "", "new output directory (required)")
	flag.Parse()
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "-out is required")
		os.Exit(2)
	}
	if err := security.GenerateDevelopmentPKI(*dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Development credentials created in", *dir)
}
