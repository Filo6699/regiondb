package fs_split

import "time"

var replaceRetryDelays = [...]time.Duration{
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	40 * time.Millisecond,
	80 * time.Millisecond,
	160 * time.Millisecond,
}

func replaceFile(oldPath, newPath string, writeThrough bool) error {
	return replaceFileWithRetry(
		oldPath,
		newPath,
		func(oldPath, newPath string) error {
			return replacePath(oldPath, newPath, writeThrough)
		},
		isTransientReplaceError,
		time.Sleep,
	)
}

func replaceFileWithRetry(
	oldPath string,
	newPath string,
	replace func(string, string) error,
	transient func(error) bool,
	wait func(time.Duration),
) error {
	for _, delay := range replaceRetryDelays {
		err := replace(oldPath, newPath)
		if err == nil || !transient(err) {
			return err
		}
		wait(delay)
	}
	return replace(oldPath, newPath)
}
