package version

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"

	"juno/pkg/debug"
)

var (
	Version   string = "1.1"
	Revision  string = ""
	BuildId   string = ""
	BuildTime string = ""
)

func OnelineVersionString() string {
	return Version + "." + Revision + "." + BuildId
}

func WriteVersionInfo(w io.Writer) {
	binName := filepath.Base(os.Args[0])
	var buildType = "release"
	if debug.DEBUG {
		buildType = "debug"
	}
	fmt.Fprintf(w, "\nJuno %s %s (%s build)\n\n", filepath.Base(binName), Version, buildType)

	if BuildId != "" {
		fmt.Fprintf(w, "  Build No. : %s\n", BuildId)
	}
	if Revision != "" {
		fmt.Fprintf(w, "  Git Commit: %s\n", Revision)
	}
	fmt.Fprintf(w, "  Go Version: %s\n  OS/Arch   : %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	if BuildTime != "" {
		fmt.Fprintf(w, "  Built     : %s\n", BuildTime)
	}
	fmt.Fprintf(w, "\n")
}

func PrintVersionInfo() {
	WriteVersionInfo(os.Stdout)
}

func HttpHandler(w http.ResponseWriter, r *http.Request) {
	WriteVersionInfo(w)
}
