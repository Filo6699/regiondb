//go:build windows

package fs_split

func syncParentDirectory(string) error {
	// replacePath uses MOVEFILE_WRITE_THROUGH because Windows does not expose
	// directory handles through os.Open that can be synchronized.
	return nil
}
