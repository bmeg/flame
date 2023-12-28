package flame

/*
func allTrue(x []bool) bool {
	for _, y := range x {
		if !y {
			return false
		}
	}
	return true
}
*/

func anyTrue(x []bool) bool {
	for _, y := range x {
		if y {
			return true
		}
	}
	return false
}
