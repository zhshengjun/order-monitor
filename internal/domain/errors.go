package domain

type InputError string

func (e InputError) Error() string { return string(e) }
