// Conversation ids: single "si_<a>:<b>" with a < b, group "sg_<group_id>".
// ":" is the separator because user ids contain "_".
package conv

func Single(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return "si_" + a + ":" + b
}

func Group(groupId string) string { return "sg_" + groupId }
