package fs_split

func clampWALStreamLimit(requested, descriptorBudget int) int {
	if descriptorBudget < 1 {
		descriptorBudget = 1
	}
	if requested > descriptorBudget {
		return descriptorBudget
	}
	return requested
}
