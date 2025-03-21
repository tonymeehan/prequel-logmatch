package match

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"

	"github.com/goccy/go-yaml"
	"github.com/itchyny/gojq"
	"github.com/rs/zerolog/log"
)

var ErrEmptyTerm = errors.New("empty term")

type Matcher interface {
	Scan(e entry.LogEntry) Hits
}

type LogEntry = entry.LogEntry

type Hits struct {
	Cnt  int
	Logs []LogEntry
}

func (h *Hits) PopFront() []LogEntry {
	if h.Cnt <= 0 {
		return nil
	}

	var (
		sz   = len(h.Logs) / h.Cnt
		logs = h.Logs[:sz]
	)

	h.Cnt -= 1
	h.Logs = h.Logs[sz:]
	return logs
}

func (h Hits) Last() []LogEntry {
	return h.Index(h.Cnt - 1)
}

func (h Hits) Index(i int) []LogEntry {
	if i >= h.Cnt {
		return nil
	}
	var (
		nLogs = len(h.Logs)
		sz    = nLogs / h.Cnt
		off   = i * sz
	)
	return h.Logs[off : off+sz]
}

type MatchFunc func(string) bool

func makeMatchFunc(s string) (m MatchFunc, err error) {

	switch {
	case s == "":
		err = ErrEmptyTerm
	case strings.HasPrefix(s, "jq_"):
		if m, err = makeJqMatch(s); err != nil {
			err = fmt.Errorf("fail jq compile '%s': %w", s, err)
		}
	case isRegex(s):
		if m, err = makeRegexMatch(s); err != nil {
			err = fmt.Errorf("fail regex compile '%s': %w", s, err)
		}
	default:
		m = makeRawMatch(s)
	}

	return
}

func isRegex(v string) bool {
	return regexp.QuoteMeta(v) != v
}

func makeRawMatch(s string) MatchFunc {
	return func(line string) bool {
		return strings.Contains(line, s)
	}
}

func makeRegexMatch(term string) (MatchFunc, error) {
	exp, err := regexp.Compile(term)
	if err != nil {
		return nil, err
	}

	return func(line string) bool {
		return exp.MatchString(line)
	}, nil
}

func makeJsonUnmarshal() func(string) (any, error) {
	// memorize unmarshaller; this avoids unmarshalling
	// multiple times if there is more than one Jq matcher installed

	var (
		lastLine  string
		lastError error
		lastValue any
	)

	return func(line string) (any, error) {
		if line == lastLine {
			return lastValue, lastError
		}
		lastLine = line
		lastError = json.Unmarshal([]byte(line), &lastValue)
		return lastValue, lastError
	}
}

func makeYamlUnmarshal() func(string) (any, error) {
	// memorize unmarshaller; this avoids unmarshalling
	// multiple times if there is more than one Jq matcher installed

	var (
		lastLine  string
		lastError error
		lastValue any
	)

	return func(line string) (any, error) {
		if line == lastLine {
			return lastValue, lastError
		}
		lastLine = line
		lastError = yaml.Unmarshal([]byte(line), &lastValue)
		return lastValue, lastError
	}
}

func NewJqJson(term string) (MatchFunc, error) {
	unmarshal := makeJsonUnmarshal()

	query, err := gojq.Parse(term)
	if err != nil {
		return nil, err
	}

	code, err := gojq.Compile(query)
	if err != nil {
		return nil, err
	}

	return _makeJqMatch(term, code, unmarshal), nil
}

func makeJqMatch(term string) (MatchFunc, error) {
	var unmarshal unmarshalFuncT

	switch {
	case strings.HasPrefix(term, "jq_json:"):
		unmarshal = makeJsonUnmarshal()
	case strings.HasPrefix(term, "jq_yaml:"):
		unmarshal = makeYamlUnmarshal()
	default:
		return nil, errors.New("unknown jq format")
	}

	term = term[8:]

	query, err := gojq.Parse(term)
	if err != nil {
		return nil, err
	}

	code, err := gojq.Compile(query)
	if err != nil {
		return nil, err
	}

	return _makeJqMatch(term, code, unmarshal), nil
}

type unmarshalFuncT func(string) (any, error)

func _makeJqMatch(term string, code *gojq.Code, unmarshal unmarshalFuncT) MatchFunc {
	return func(line string) (match bool) {
		// Avoid unnecessary allocation on the cast
		var (
			err error
			v   any
		)

		// This is obviously not ideal;  unmarshal the entire payload
		// just to do a matching check is extremely wasteful.
		// Ideally we'd have an inline matcher for both JSON and YAML.
		if v, err = unmarshal(line); err != nil {
			log.Debug().Err(err).Str("line", line).Msg("Fail parse JSON log line")
			return false
		}
		iter := code.Run(v)
		for {
			res, ok := iter.Next()
			if !ok {
				break
			}
			if err, ok := res.(error); ok {
				if err, ok := err.(*gojq.HaltError); ok && err.Value() == nil {
					break
				}
				log.Debug().Err(err).
					Str("line", line).
					Str("term", term).
					Msg("Fail jq query on JSON log line")
				match = false
				break
			}

			if res != nil {
				if v, ok := res.(bool); ok {
					if v {
						match = true
					}
				} else {
					match = true
				}
			}
		}

		return
	}
}
