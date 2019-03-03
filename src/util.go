package main

func ToString(val interface{}) string {
	if val == nil {
		return ""
	}
	t, ok := val.(string)
	if ok {
		return t
	}
	return ""
}

func ToInt(val interface{}) int {
	if val == nil {
		return 0
	}
	t, ok := val.(int)
	if ok {
		return t
	}
	return 0
}
