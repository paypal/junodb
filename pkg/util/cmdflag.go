package util

type StringListFlags []string

func (l *StringListFlags) String() string {
	return "string list flags"
}

func (l *StringListFlags) Set(value string) error {
	*l = append(*l, value)
	return nil
}
