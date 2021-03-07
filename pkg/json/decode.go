package json

import (
	"strings"

	"encoding/json"
)

func (u *Uuid) UnmarshalJSON(b []byte) error {

	var s string
	//todo:find better way to convert [,] to byte
	//remove outer square brackets if there are any.
	if b[0] == []byte("[")[0] && b[len(b)-1] == []byte("]")[0] {
		b = b[1 : len(b)-1]
	}
	err := UnmarshalJSON(b, &s)
	if err != nil {
		return err
	}
	*u = Uuid(s)
	return nil
}
func (u *Set) UnmarshalJSON(b []byte) error {
	k := make([]string, 0)
	//todo:to find better way to convert [,] to byte
	//add outer square brackets if there is no brackets.
	if b[0] != []byte("[")[0] && b[len(b)-1] != []byte("]")[0] {
		b = append([]byte("["), b...)
		b = append(b, []byte("]")...)
	}
	err := UnmarshalJSON(b, &k)
	if err != nil {
		return err
	}
	*u = Set(k)
	return nil
}

func processStr(str string) string {
	processMaps := func(str string) string {
		mapSubstrings := strings.Split(str, `["map",[`)
		res := mapSubstrings[0]
		for _, mapSubstring := range mapSubstrings[1:] {
			splittedSubstring := strings.SplitN(mapSubstring, `]]]`, 2)
			mapToEdit := splittedSubstring[0]
			restOfSubstring := splittedSubstring[1]
			mapToEdit = strings.ReplaceAll(mapToEdit, `","`, `":"`)
			mapToEdit = strings.ReplaceAll(mapToEdit, `],[`, `,`)
			mapToEdit = strings.ReplaceAll(mapToEdit, `]`, ``)
			mapToEdit = strings.ReplaceAll(mapToEdit, `[`, ``)
			res += `{` + mapToEdit + `}` + restOfSubstring
		}
		return res
	}
	processSets := func(str string) string {
		ProccessSetsWithMultipleElements := func(str string) string {
			setSubstrings := strings.Split(str, `["set",[`)
			res := setSubstrings[0]
			for _, setSubstring := range setSubstrings[1:] {
				splitted := strings.SplitN(setSubstring, `]]`, 2)
				setToEdit := splitted[0]
				rest := splitted[1]
				setToEdit = strings.ReplaceAll(setToEdit, `"uuid",`, ``)
				setToEdit = strings.ReplaceAll(setToEdit, `],[`, `,`)
				setToEdit = strings.ReplaceAll(setToEdit, `]`, ``)
				setToEdit = strings.ReplaceAll(setToEdit, `[`, ``)
				if rest[0] != ']' {
					rest = `]` + rest
				}
				res += `[` + setToEdit + rest
			}
			return res
		}
		//ProccessSetWithSingleElement assums that we called earlier to ProccessSetsWithMultipleElements on the input str
		ProccessSetWithSingleElement := func(str string) string {
			setSubstrings := strings.Split(str, `:["uuid",`)
			res := setSubstrings[0]
			for _, setSubstring := range setSubstrings[1:] {
				splittedSubstring := strings.SplitN(setSubstring, `]`, 2)
				setToEdit := splittedSubstring[0]
				restOfSubstring := splittedSubstring[1]
				res += ":[" + setToEdit + "]" + restOfSubstring
			}
			return res
		}
		return ProccessSetWithSingleElement(ProccessSetsWithMultipleElements(str))
	}
	s := processSets(processMaps(str))
	s = strings.ReplaceAll(s, `\"`, `\\\"`) //escaping the backslash char
	return s
}

func UnmarshalJSON(b []byte, v interface{}) error {
	s := processStr(string(b))
	return json.Unmarshal([]byte(s), v)
}
