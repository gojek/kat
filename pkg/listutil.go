package pkg

import "regexp"

type ListUtil struct {
	List []string
}

func (l ListUtil) Filter(regex string, include bool) ([]string, error) {
	var filteredList []string
	for _, value := range l.List {
		matched, err := regexp.Match(regex, []byte(value))
		if err != nil {
			return nil, err
		}

		if (include && matched) || (!include && !matched) {
			filteredList = append(filteredList, value)
		}
	}
	return filteredList, nil
}

func (l ListUtil) Contains(element string) bool {
	for _, i := range l.List {
		if i == element {
			return true
		}
	}
	return false
}
