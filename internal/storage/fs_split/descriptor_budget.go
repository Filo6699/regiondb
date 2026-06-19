package fs_split

import "errors"

func clampWALStreamLimit(requested, descriptorBudget int) int {
	if descriptorBudget < 1 {
		descriptorBudget = 1
	}
	if requested > descriptorBudget {
		return descriptorBudget
	}
	return requested
}

// EffectiveWALHandleLimit returns the WAL handle limit after reserving
// descriptors owned by the rest of the process.
func EffectiveWALHandleLimit(requested, descriptorReserve int) (int, error) {
	if requested <= 0 {
		return 0, errors.New("maximum open WAL handles must be positive")
	}
	if descriptorReserve < 0 {
		return 0, errors.New("descriptor reserve must not be negative")
	}
	return clampWALStreamLimit(requested, walDescriptorBudget(descriptorReserve)), nil
}

// AvailableWALDescriptors returns the process descriptor capacity remaining
// after the caller's reserve, without forcing the result to a usable minimum.
func AvailableWALDescriptors(descriptorReserve int) (int, error) {
	if descriptorReserve < 0 {
		return 0, errors.New("descriptor reserve must not be negative")
	}
	return walDescriptorBudget(descriptorReserve), nil
}
