// Command world-android-image-digest calculates the exact immutable identity
// consumed by the managed Android emulator driver.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/cuttlefish"
)

func main() {
	path := flag.String("path", "", "installed Android SDK system-image directory")
	flag.Parse()
	if flag.NArg() != 0 || strings.TrimSpace(*path) == "" {
		fmt.Fprintln(os.Stderr, "usage: world-android-image-digest -path <system-image-directory>")
		os.Exit(2)
	}
	digest, err := cuttlefish.DigestManagedSystemImage(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "digest Android system image: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(digest.String())
}
