package match

import (
	"testing"
)

func TestMatchJson(t *testing.T) {

	tt := TermT{
		Type:  TermJqJson,
		Value: `select(.shrubbery == "apple")`,
	}

	m, err := tt.NewMatcher()
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`{"shrubbery":"apple"}`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`{"nope":"apple"}`) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestMatchJsonString(t *testing.T) {
	tt := TermT{
		Type:  TermJqJson,
		Value: `select(.shrubbery == "apple")`,
	}

	m, err := tt.NewMatcher()
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`{"shrubbery":"apple"}`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`{"shrubbery":"xapple"}`) {
		t.Errorf("Expected no match, got match.")
	}

}

func TestMatchJsonRegex(t *testing.T) {
	tt := TermT{
		Type:  TermJqJson,
		Value: `.shrubbery | test("^a.")`,
	}

	m, err := tt.NewMatcher()
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`{"shrubbery":"apple"}`) {
		t.Errorf("Expected match, got fail.")
	}

	if !m(`{"shrubbery":"applex"}`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`{"shrubbery":"banana"}`) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestMatchYaml(t *testing.T) {
	tt := TermT{
		Type:  TermJqYaml,
		Value: `.shrubbery`,
	}

	m, err := tt.NewMatcher()
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`shrubbery: apple`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`nope: apple`) {
		t.Errorf("Expected no match, got match.")
	}
}

var jsonData = `
{
  "widget": {
    "debug": "on",
    "window": {
      "title": "Sample Konfabulator Widget",
      "name": "main_window",
      "width": 500,
      "height": 500
    },
    "image": { 
      "src": "Images/Sun.png",
      "hOffset": 250,
      "vOffset": 250,
      "alignment": "center"
    },
    "text": {
      "data": "Click Here",
      "size": 36,
      "style": "bold",
      "vOffset": 100,
      "alignment": "center",
      "onMouseUp": "sun1.opacity = (sun1.opacity / 100) * 90;"
    }
  }
} `

func BenchmarkMatchJson(b *testing.B) {
	var (
		tt1 = TermT{
			Type:  TermJqJson,
			Value: `.widget.window.name`,
		}
		tt2 = TermT{
			Type:  TermJqJson,
			Value: `.widget.image.hOffset`,
		}
		tt3 = TermT{
			Type:  TermJqJson,
			Value: `.widget.text.onMouseUp`,
		}

		m1, _ = tt1.NewMatcher()
		m2, _ = tt2.NewMatcher()
		m3, _ = tt3.NewMatcher()
	)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m1(jsonData)
		m2(jsonData)
		m3(jsonData)
	}

}
